//! `jj-mesh status`: show the daemon and mesh state.
//!
//! The overview covers this machine, its peers and every mesh repo, cloned
//! here or not. The repo view covers one repo and its workspaces across the
//! mesh. Both end with the issues that need action.

use std::collections::{BTreeMap, BTreeSet};

use clap::Args;
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{Result, ensure};

use super::{complete, ui};
use crate::{
    config::{ConfigDir, MAX_MACHINE_WORKSPACES, sanitize},
    daemon::control::{
        self, ConnectionStatus, PathInfo, PeerReport, RepoHealthState, Route, Status, WatchStatus,
        WorkspacePlace, WorkspaceState, WorkspaceStatus,
    },
};

/// Show the daemon state and the live mesh status
#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Show the details of this repo
    #[arg(add = ArgValueCandidates::new(complete::mesh_repos))]
    repo: Option<String>,
}

/// Runs the `status` command.
pub fn run(args: &StatusArgs, dir: &ConfigDir) -> Result<()> {
    let status = control::query_status_blocking(dir)?;
    let issues = if let Some(name) = &args.repo {
        print_repo(&status, name)?;
        repo_issues(&status, |repo| repo == name)
    } else {
        print_mesh(&status);
        let mut issues = mesh_issues(&status);
        issues.extend(repo_issues(&status, |_| true));
        issues
    };

    if !issues.is_empty() {
        println!();
        for issue in &issues {
            println!("{}", ui::warn(issue));
        }
    }
    Ok(())
}

/// Prints the overview of this machine, its peers and the mesh repos.
fn print_mesh(status: &Status) {
    println!(
        "{}  {}",
        machine(&sanitize(&status.name), status.server),
        ui::dim(format_args!(
            "jj-mesh {}, up {}",
            env!("CARGO_PKG_VERSION"),
            ui::format_duration(status.uptime_secs)
        ))
    );
    println!();
    print_peers(status);
    println!();
    print_repos(status);
}

/// Prints one line per peer.
fn print_peers(status: &Status) {
    if status.peers.is_empty() {
        println!("no paired peers");
        return;
    }
    let rows: Vec<_> = status
        .peers
        .iter()
        .map(|peer| {
            let (state, detail) = connection(&peer.connection);
            [machine(&peer.name, peer.server), state, detail]
        })
        .collect();
    print_table(&rows);
}

/// Prints one line per mesh repo, with the other machines holding it.
fn print_repos(status: &Status) {
    if status.repos.is_empty() && status.available.is_empty() {
        println!("no repos added");
        return;
    }
    let holders = holders(status);
    let machines = |name: &str| {
        let machines = holders.get(name).into_iter().flatten().copied();
        machines.collect::<Vec<_>>().join(", ")
    };
    let mut rows = Vec::new();
    for repo in &status.repos {
        rows.push([
            repo.name.clone(),
            ui::dim(ui::display_path(&repo.path)).to_string(),
            watch_summary(&repo.watch),
            machines(&repo.name),
        ]);
    }
    for name in &status.available {
        rows.push([
            name.clone(),
            ui::dim("-").to_string(),
            String::new(),
            machines(name),
        ]);
    }
    print_table(&rows);
}

/// Prints one repo and its workspaces.
fn print_repo(status: &Status, name: &str) -> Result<()> {
    let Some(repo) = status.repos.iter().find(|repo| repo.name == name) else {
        ensure!(
            status.available.iter().any(|available| available == name),
            "no repo named `{name}` in the mesh"
        );
        println!("{name}  {}", ui::dim("not cloned"));
        return Ok(());
    };

    // Local workspaces are only known while the repo is watched.
    let listed = repo
        .workspaces
        .iter()
        .any(|w| matches!(&w.place, WorkspacePlace::Local { path, .. } if *path == repo.path));
    if listed {
        println!("{name}  {}", watch_summary(&repo.watch));
    } else {
        println!(
            "{name}  {}  {}",
            ui::dim(ui::display_path(&repo.path)),
            watch_summary(&repo.watch)
        );
    }
    if !repo.workspaces.is_empty() {
        let rows: Vec<_> = repo.workspaces.iter().map(workspace_row).collect();
        println!();
        print_table(&rows);
    }
    Ok(())
}

/// Columns of a workspace line: name, path or machine, state when not
/// synced.
fn workspace_row(workspace: &WorkspaceStatus) -> [String; 3] {
    let (place, state) = match &workspace.place {
        WorkspacePlace::Peer { machine } => (sanitize(machine), String::new()),
        WorkspacePlace::Missing => (
            ui::dim("-").to_string(),
            ui::warn("directory missing").to_string(),
        ),
        WorkspacePlace::Local { path, state } => {
            let state = match state {
                WorkspaceState::Claimed => String::new(),
                WorkspaceState::Contested { .. } => ui::bad("contested").to_string(),
                WorkspaceState::Foreign { .. }
                | WorkspaceState::Stale
                | WorkspaceState::Unclaimable => ui::dim("not synced").to_string(),
            };
            (ui::dim(ui::display_path(path)).to_string(), state)
        }
    };
    // `@` marks workspace names, as in `jj log`.
    [format!("{}@", sanitize(&workspace.name)), place, state]
}

/// Prints rows as aligned columns.
fn print_table<const N: usize>(rows: &[[String; N]]) {
    for line in ui::table(rows) {
        println!("{line}");
    }
}

/// State and details of a peer connection.
fn connection(connection: &ConnectionStatus) -> (String, String) {
    match connection {
        ConnectionStatus::Connecting { failures: 0 } => {
            (ui::warn("connecting").to_string(), String::new())
        }
        ConnectionStatus::Connecting { .. } => {
            (ui::bad("offline").to_string(), "retrying".to_owned())
        }
        ConnectionStatus::Backoff { retry_in_secs, .. } => (
            ui::bad("offline").to_string(),
            format!("retry in {}", ui::format_duration(*retry_in_secs)),
        ),
        ConnectionStatus::Incompatible { retry_in_secs, .. } => (
            ui::bad("incompatible").to_string(),
            format!("retry in {}", ui::format_duration(*retry_in_secs)),
        ),
        ConnectionStatus::Connected { path, .. } => {
            let route = match path {
                Some(PathInfo {
                    route: Route::Direct { addr },
                    rtt_ms,
                }) => format!("direct {addr}, {rtt_ms} ms"),
                Some(PathInfo {
                    route: Route::Relay { url },
                    rtt_ms,
                }) => format!("relay {}, {rtt_ms} ms", relay_host(url)),
                None => "path pending".to_owned(),
            };
            (ui::good("connected").to_string(), route)
        }
    }
}

/// A machine name, marked when it runs in server mode.
fn machine(name: &str, server: bool) -> String {
    if server {
        format!("{name} {}", ui::dim("(server)"))
    } else {
        name.to_owned()
    }
}

/// The host of a relay URL, enough to tell relays apart.
fn relay_host(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let host = rest.split_once('/').map_or(rest, |(host, _)| host);
    // Relay URLs are fully qualified (`relay.example.com./`).
    host.strip_suffix('.').unwrap_or(host)
}

/// One-line state of a repo watch.
fn watch_summary(watch: &WatchStatus) -> String {
    match watch {
        WatchStatus::Opening => "opening".to_owned(),
        WatchStatus::Watching {
            last_change_secs: Some(secs),
            ..
        } => ui::good(format!("synced {} ago", ui::format_age(*secs))).to_string(),
        WatchStatus::Watching { .. } => ui::good("watching").to_string(),
        WatchStatus::Failed { retry_in_secs, .. } => format!(
            "{}, retry in {}",
            ui::bad("error"),
            ui::format_duration(*retry_in_secs)
        ),
        WatchStatus::Missing { .. } => ui::warn("directory missing").to_string(),
        WatchStatus::Indexing => "indexing commits".to_owned(),
    }
}

/// The peers holding each repo: those claiming a workspace in it and the
/// connected ones reporting it, except where they report it missing or
/// announce a different repo under its name.
fn holders(status: &Status) -> BTreeMap<&str, BTreeSet<&str>> {
    let mut holders: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();
    for repo in &status.repos {
        for workspace in &repo.workspaces {
            if let WorkspacePlace::Peer { machine } = &workspace.place {
                holders
                    .entry(repo.name.as_str())
                    .or_default()
                    .insert(machine.as_str());
            }
        }
    }
    for PeerReport { peer, report } in &status.peer_reports {
        for repo in &report.repos {
            let peers = holders.entry(repo.name.as_str()).or_default();
            if repo.state == RepoHealthState::Missing {
                peers.remove(peer.as_str());
            } else {
                peers.insert(peer.as_str());
            }
        }
    }
    for conflict in &status.conflicts {
        if let Some(peers) = holders.get_mut(conflict.repo.as_str()) {
            peers.remove(conflict.peer.as_str());
        }
    }
    holders
}

/// Issues about this machine and its peers rather than a repo.
fn mesh_issues(status: &Status) -> Vec<String> {
    let mut issues = Vec::new();
    if let Some(warning) = crate::repo::jj_version_warning(status.jj_version.as_deref()) {
        issues.push(format!("{}: {warning}", sanitize(&status.name)));
    }
    for peer in &status.peers {
        if let ConnectionStatus::Incompatible { peer: hello, .. } = &peer.connection {
            issues.push(format!("{}: {hello}", peer.name));
        }
    }
    for PeerReport { peer, report } in &status.peer_reports {
        let warning = crate::repo::jj_peer_warning(
            status.jj_version.as_deref(),
            report.jj_version.as_deref(),
        );
        issues.extend(warning.map(|warning| format!("{peer}: {warning}")));
    }
    issues
}

/// Issues about the `wanted` repos, here and on connected peers.
fn repo_issues(status: &Status, wanted: impl Fn(&str) -> bool) -> Vec<String> {
    let mut issues = Vec::new();
    for repo in status.repos.iter().filter(|repo| wanted(&repo.name)) {
        match &repo.watch {
            WatchStatus::Failed { error, .. } => {
                issues.push(format!("`{}`: {}", repo.name, sanitize(error)));
            }
            WatchStatus::Missing { .. } => issues.push(format!(
                "`{0}`: directory missing. Run `jj-mesh repo forget {0}` if it was deleted",
                repo.name
            )),
            _ => {}
        }
        for workspace in &repo.workspaces {
            if let Some(issue) = workspace_issue(&workspace.name, &workspace.place) {
                issues.push(format!("`{}`: {issue}", repo.name));
            }
        }
    }
    for conflict in status.conflicts.iter().filter(|c| wanted(&c.repo)) {
        issues.push(format!(
            "`{}`: peer {} announced a different repo under the same name",
            conflict.repo, conflict.peer,
        ));
    }
    for PeerReport { peer, report } in &status.peer_reports {
        for repo in report.repos.iter().filter(|repo| wanted(&repo.name)) {
            let problem = match repo.state {
                RepoHealthState::Ok => continue,
                RepoHealthState::Failed => "sync error (see that machine's status)",
                RepoHealthState::Missing => "directory missing on that machine",
            };
            issues.push(format!("`{}` on {peer}: {problem}", repo.name));
        }
    }
    issues
}

/// What the user can do about a workspace this machine does not keep
/// fresh.
fn workspace_issue(name: &str, place: &WorkspacePlace) -> Option<String> {
    let name = sanitize(name);
    // jj workspace names are mesh-wide: renaming one copy renames both.
    let recreate = format!(
        "re-create it under another name (`jj workspace forget {name}`, then \
         `jj workspace add --name <new>`)"
    );
    let issue = match place {
        WorkspacePlace::Local { state, .. } => match state {
            WorkspaceState::Claimed => return None,
            WorkspaceState::Contested { machines } => format!(
                "workspace `{name}` also exists on {}, so neither copy is synced. On one machine, {recreate}",
                sanitize(&machines.join(", ")),
            ),
            WorkspaceState::Foreign { machines } => format!(
                "workspace `{name}` here is not synced, it belongs to {}. To sync it, {recreate}",
                sanitize(&machines.join(", ")),
            ),
            WorkspaceState::Stale => format!(
                "workspace `{name}` was stale when found. Run `jj workspace update-stale` in it to sync it"
            ),
            WorkspaceState::Unclaimable => format!(
                "workspace `{name}` cannot be synced because its name is not valid in the mesh, or this \
                 machine syncs {MAX_MACHINE_WORKSPACES} workspaces already"
            ),
        },
        WorkspacePlace::Missing => format!(
            "workspace `{name}` is missing. Run `jj workspace forget {name}` if it was deleted"
        ),
        WorkspacePlace::Peer { .. } => return None,
    };
    Some(issue)
}

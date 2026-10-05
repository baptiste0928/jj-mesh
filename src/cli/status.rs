//! `jj-mesh status`: show the daemon and mesh state.

use clap::Args;
use color_eyre::eyre::Result;

use super::ui;
use crate::{
    config::{ConfigDir, MAX_MACHINE_WORKSPACES, sanitize},
    daemon::control::{
        self, ConnectionStatus, PeerReport, RepoHealthState, Route, WorkspacePlace, WorkspaceState,
    },
};

/// Show the daemon state and the live mesh status
#[derive(Debug, Args)]
pub struct StatusArgs {}

/// Runs the `status` command.
pub fn run(_args: StatusArgs, dir: &ConfigDir) -> Result<()> {
    println!(
        "{}",
        ui::dim(format_args!(
            "jj-mesh {} ({})",
            env!("CARGO_PKG_VERSION"),
            env!("JJ_MESH_COMMIT")
        ))
    );

    let status = control::query_status_blocking(dir)?;

    println!(
        "daemon: {} (uptime {})",
        ui::good("running"),
        ui::format_duration(status.uptime_secs)
    );
    println!("machine: {}", sanitize(&status.name));
    if let Some(warning) = crate::repo::jj_version_warning(status.jj_version.as_deref()) {
        println!("{} {warning}", ui::warn("warning:"));
    }

    println!();
    if status.peers.is_empty() {
        println!("no paired peers");
    } else {
        println!("{}", ui::heading("peers:"));
        let width = ui::name_width(status.peers.iter().map(|p| p.name.as_str()));
        for peer in &status.peers {
            println!(
                "  {:width$}  {}",
                peer.name,
                connection_summary(&peer.connection)
            );
        }
    }

    println!();
    if status.repos.is_empty() {
        println!("no repos added");
    } else {
        println!("{}", ui::heading("repos:"));
        let width = ui::name_width(status.repos.iter().map(|r| r.name.as_str()));
        for repo in &status.repos {
            // The root workspace line shows the repo path; local workspaces
            // are unknown until the repo is watched.
            let listed = repo.workspaces.iter().any(
                |w| matches!(&w.place, WorkspacePlace::Local { path, .. } if *path == repo.path),
            );
            let path = if listed {
                String::new()
            } else {
                format!("{}  ", ui::dim(ui::display_path(&repo.path)))
            };
            println!(
                "  {:width$}  {path}{}",
                repo.name,
                watch_summary(&repo.watch)
            );
            // `@` marks workspace names, as in `jj log`.
            let names: Vec<String> = repo
                .workspaces
                .iter()
                .map(|w| format!("{}@", sanitize(&w.name)))
                .collect();
            let width = ui::name_width(names.iter().map(String::as_str));
            for (workspace, name) in repo.workspaces.iter().zip(&names) {
                println!("    {name:width$}  {}", place_summary(&workspace.place));
            }
        }
    }
    if !status.available.is_empty() {
        println!();
        println!(
            "  {}",
            ui::dim(format_args!("(available: {})", status.available.join(", ")))
        );
    }

    let issues = collect_issues(&status);
    if !issues.is_empty() {
        println!();
        println!("{}", ui::warn("issues:").bold());
        for issue in &issues {
            println!("  {issue}");
        }
    }

    Ok(())
}

/// Gathers everything that needs the user's attention into one list:
/// local name conflicts, plus the problems connected peers report about
/// their own instances. Healthy peer reports stay silent.
fn collect_issues(status: &control::Status) -> Vec<String> {
    let mut issues = Vec::new();

    for repo in &status.repos {
        for workspace in &repo.workspaces {
            if let Some(issue) = workspace_issue(&workspace.name, &workspace.place) {
                issues.push(format!("`{}`: {issue}", repo.name));
            }
        }
    }
    for conflict in &status.conflicts {
        issues.push(format!(
            "`{}`: peer {} announced a different repo under the same name",
            conflict.repo, conflict.peer,
        ));
    }
    for PeerReport { peer, report } in &status.peer_reports {
        if let Some(warning) =
            crate::repo::jj_peer_warning(status.jj_version.as_deref(), report.jj_version.as_deref())
        {
            issues.push(format!("{peer}: {warning}"));
        }
        for repo in &report.repos {
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

/// One-line description of where a workspace lives.
fn place_summary(place: &WorkspacePlace) -> String {
    let (path, state) = match place {
        WorkspacePlace::Local { path, state } => (path, state),
        WorkspacePlace::Missing => return ui::warn("directory missing").to_string(),
        WorkspacePlace::Peer { machine } => {
            return ui::dim(format_args!("on {}", sanitize(machine))).to_string();
        }
    };
    let path = ui::dim(ui::display_path(path));
    match state {
        WorkspaceState::Claimed => path.to_string(),
        WorkspaceState::Contested { .. } => format!("{path}  {}", ui::bad("(contested)")),
        WorkspaceState::Foreign { .. } | WorkspaceState::Stale | WorkspaceState::Unclaimable => {
            format!("{path}  {}", ui::dim("(not synced)"))
        }
    }
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
                "workspace `{name}` also exists on {}, so neither copy is synced; on one machine, {recreate}",
                sanitize(&machines.join(", ")),
            ),
            WorkspaceState::Foreign { machines } => format!(
                "workspace `{name}` here is not synced, it belongs to {}; to sync it, {recreate}",
                sanitize(&machines.join(", ")),
            ),
            WorkspaceState::Stale => format!(
                "workspace `{name}` was stale when found; run `jj workspace update-stale` in it to sync it"
            ),
            WorkspaceState::Unclaimable => format!(
                "workspace `{name}` cannot be synced: its name is not valid in the mesh, or this \
                 machine syncs {MAX_MACHINE_WORKSPACES} workspaces already"
            ),
        },
        WorkspacePlace::Missing => format!(
            "workspace `{name}` is missing; run `jj workspace forget {name}` if it was deleted"
        ),
        WorkspacePlace::Peer { .. } => return None,
    };
    Some(issue)
}

/// One-line description of a repo watch.
fn watch_summary(watch: &control::WatchStatus) -> String {
    match watch {
        control::WatchStatus::Opening => "opening".to_owned(),
        control::WatchStatus::Watching {
            last_change_secs, ..
        } => {
            let synced = match last_change_secs {
                Some(secs) => format!(" (synced {} ago)", ui::format_duration(*secs)),
                None => String::new(),
            };
            format!("{}{synced}", ui::good("watching"))
        }
        control::WatchStatus::Failed {
            error,
            retry_in_secs,
        } => format!(
            "{} {} (retry in {})",
            ui::bad("error:"),
            sanitize(error),
            ui::format_duration(*retry_in_secs)
        ),
        control::WatchStatus::Missing { retry_in_secs } => format!(
            "{} (retry in {})",
            ui::warn("directory missing"),
            ui::format_duration(*retry_in_secs)
        ),
        control::WatchStatus::Indexing => "indexing commits".to_owned(),
    }
}

/// One-line description of a peer connection.
fn connection_summary(connection: &ConnectionStatus) -> String {
    match connection {
        ConnectionStatus::Connecting { failures: 0 } => ui::warn("connecting").to_string(),
        ConnectionStatus::Connecting { .. } => {
            format!("{} (retrying)", ui::bad("offline"))
        }
        ConnectionStatus::Backoff { retry_in_secs, .. } => format!(
            "{} (retry in {})",
            ui::bad("offline"),
            ui::format_duration(*retry_in_secs)
        ),
        ConnectionStatus::Connected { path, since_secs } => {
            let route = match path {
                Some(control::PathInfo {
                    route: Route::Direct { addr },
                    rtt_ms,
                }) => format!("direct {addr} (rtt {rtt_ms} ms)"),
                Some(control::PathInfo {
                    route: Route::Relay { url },
                    rtt_ms,
                }) => format!("relay {url} (rtt {rtt_ms} ms)"),
                None => "path pending".to_owned(),
            };
            format!(
                "{} {route}, up {}",
                ui::good("connected"),
                ui::format_duration(*since_secs)
            )
        }
    }
}

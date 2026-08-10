//! `jj-mesh status`: show the daemon and mesh state.

use clap::Args;
use color_eyre::eyre::Result;

use super::ui;
use crate::{
    config::{ConfigDir, sanitize},
    daemon::control::{self, ConnectionStatus, PeerReport, RepoHealthState, Route},
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
        let paths: Vec<String> = status
            .repos
            .iter()
            .map(|r| ui::display_path(&r.path))
            .collect();
        let path_width = ui::name_width(paths.iter().map(String::as_str));
        for (repo, path) in status.repos.iter().zip(&paths) {
            println!(
                "  {:width$}  {}  {}",
                repo.name,
                ui::dim(format_args!("{path:path_width$}")),
                watch_summary(&repo.watch),
            );
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
/// local name conflicts and pauses, plus the problems connected peers
/// report about their own instances. Healthy peer reports stay silent.
fn collect_issues(status: &control::Status) -> Vec<String> {
    let mut issues = Vec::new();

    for conflict in &status.conflicts {
        issues.push(format!(
            "`{}`: peer {} announced a different repo under the same name",
            conflict.repo, conflict.peer,
        ));
    }
    for paused in &status.paused {
        issues.push(format!(
            "`{}`: this machine and {} both have a colocated instance",
            paused.repo,
            paused.peers.join(", "),
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
                RepoHealthState::Paused => "paused there (colocation conflict)",
            };
            issues.push(format!("`{}` on {peer}: {problem}", repo.name));
        }
    }

    issues
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
        ConnectionStatus::Connecting => ui::warn("connecting").to_string(),
        ConnectionStatus::Backoff { retry_in_secs } => format!(
            "{} (retry in {})",
            ui::bad("unreachable"),
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

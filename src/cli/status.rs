//! `jj-mesh status`: show the daemon and mesh state.

use clap::Args;
use color_eyre::eyre::Result;

use crate::{
    config::{Config, ConfigDir, MachineKey},
    daemon::control::{self, ConnectionStatus, Route},
};

/// Show the daemon state and the live mesh status
#[derive(Debug, Args)]
pub struct StatusArgs {}

/// Runs the `status` command.
pub fn run(_args: StatusArgs, dir: &ConfigDir) -> Result<()> {
    match control::query_status_blocking(dir)? {
        Some(status) => print_live(&status),
        None => print_static(dir)?,
    }

    Ok(())
}

/// Prints the live status reported by the daemon.
fn print_live(status: &control::Status) {
    println!(
        "daemon: running (uptime {})",
        format_duration(status.uptime_secs)
    );
    println!("local endpoint id: {}", status.endpoint);

    println!();
    if status.peers.is_empty() {
        println!("no paired peers");
    } else {
        println!("peers:");
        let width = name_width(status.peers.iter().map(|p| p.name.as_str()));
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
        println!("repos:");
        let width = name_width(status.repos.iter().map(|r| r.name.as_str()));
        for repo in &status.repos {
            println!(
                "  {:width$}  {}  ({})",
                repo.name,
                watch_summary(&repo.watch),
                repo.path.display(),
            );
        }
    }
}

/// One-line description of a repo watch.
fn watch_summary(watch: &control::WatchStatus) -> String {
    match watch {
        control::WatchStatus::Opening => "opening".to_owned(),
        control::WatchStatus::Watching {
            op_heads,
            last_change_secs,
            last_sync_secs,
        } => {
            let changed = match last_change_secs {
                Some(secs) => format!(", changed {} ago", format_duration(*secs)),
                None => String::new(),
            };
            let synced = match last_sync_secs {
                Some(secs) => format!(", synced {} ago", format_duration(*secs)),
                None => String::new(),
            };
            let divergent = match op_heads {
                2.. => format!(", {op_heads} divergent heads"),
                _ => String::new(),
            };
            format!("watching{changed}{synced}{divergent}")
        }
        control::WatchStatus::Failed {
            error,
            retry_in_secs,
        } => format!(
            "error: {} (retry in {})",
            sanitize(error),
            format_duration(*retry_in_secs)
        ),
    }
}

/// Strips control characters from daemon-provided text before it reaches
/// the terminal: error messages embed bytes read from repo files, which
/// could otherwise smuggle escape sequences.
fn sanitize(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

/// Prints the static configuration when no daemon is running.
fn print_static(dir: &ConfigDir) -> Result<()> {
    let key = MachineKey::from_config(dir)?;
    let config = Config::from_config(dir)?;

    println!("daemon: not running");
    println!("local endpoint id: {}", key.endpoint_id());
    println!(
        "{} paired peer(s), {} repo(s) configured",
        config.peers.len(),
        config.repos.len(),
    );

    Ok(())
}

/// One-line description of a peer connection, shared with `jj-mesh peers`.
pub(super) fn connection_summary(connection: &ConnectionStatus) -> String {
    match connection {
        ConnectionStatus::Connecting => "connecting".to_owned(),
        ConnectionStatus::Backoff { retry_in_secs } => {
            format!("unreachable (retry in {})", format_duration(*retry_in_secs))
        }
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
            format!("connected {route}, up {}", format_duration(*since_secs))
        }
    }
}

/// Formats a duration in seconds compactly (`43s`, `12m 3s`, `2h 4m`...).
pub(super) fn format_duration(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m {}s", s / 60, s % 60),
        s if s < 86400 => format!("{}h {}m", s / 3600, (s % 3600) / 60),
        s => format!("{}d {}h", s / 86400, (s % 86400) / 3600),
    }
}

/// Width of the longest name, for column alignment.
pub(super) fn name_width<'a>(names: impl Iterator<Item = &'a str>) -> usize {
    names.map(str::len).max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_escape_sequences() {
        assert_eq!(sanitize("plain text"), "plain text");
        assert_eq!(sanitize("a\x1b[2Kb\r\nc"), "a?[2Kb??c");
    }
}

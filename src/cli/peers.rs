//! `jj-mesh peers`: list paired peers.
//!
//! Shows live connection state (reachability, direct or relay path, rtt)
//! when the daemon is running, and falls back to the static configuration
//! otherwise.

use clap::Args;
use color_eyre::eyre::Result;

use super::status::{connection_summary, name_width};
use crate::{
    config::{Config, ConfigDir, MachineKey},
    daemon::control,
};

/// List the paired machines
#[derive(Debug, Args)]
pub struct PeersArgs {}

/// Runs the `peers` command.
pub fn run(_args: PeersArgs, dir: &ConfigDir) -> Result<()> {
    let key = MachineKey::from_config(dir)?;
    let status = control::query_status_blocking(dir)?;

    println!("local endpoint id: {}\n", key.endpoint_id());

    if let Some(status) = status {
        if status.peers.is_empty() {
            println!("this machine has no paired peers (run `jj-mesh pair` to add one)");
            return Ok(());
        }

        let width = name_width(status.peers.iter().map(|p| p.name.as_str()));
        for peer in &status.peers {
            println!(
                "{:width$}  {}  {}",
                peer.name,
                peer.endpoint,
                connection_summary(&peer.connection)
            );
        }
    } else {
        let config = Config::from_config(dir)?;
        if config.peers.is_empty() {
            println!("this machine has no paired peers (run `jj-mesh pair` to add one)");
            return Ok(());
        }

        println!("(daemon not running; showing the static configuration)");
        let width = name_width(config.peers.keys().map(String::as_str));
        for (name, peer) in &config.peers {
            println!("{name:width$}  {}", peer.endpoint);
        }
    }

    Ok(())
}

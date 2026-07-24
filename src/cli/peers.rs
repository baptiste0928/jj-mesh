//! `jj-mesh peers`: list paired peers.
//!
//! Static view from the configuration for now. Once the daemon exists, this
//! will query its control socket for live state (reachability, direct or
//! relay path, rtt, last sync) and keep the static listing as a fallback.

use clap::Args;
use color_eyre::eyre::Result;

use crate::config::{Config, ConfigDir, MachineKey};

/// List the paired machines
#[derive(Debug, Args)]
pub struct PeersArgs {}

/// Runs the `peers` command.
pub fn run(_args: PeersArgs, dir: &ConfigDir) -> Result<()> {
    let key = MachineKey::from_config(dir)?;
    let config = Config::from_config(dir)?;

    println!("local endpoint id: {}\n", key.endpoint_id());

    if config.peers.is_empty() {
        println!("this machine has no paired peers (run `jj-mesh pair` to add one)");
        return Ok(());
    }

    let width = config.peers.keys().map(String::len).max().unwrap_or(0);
    for (name, peer) in &config.peers {
        println!("{name:width$}  {}", peer.endpoint);
    }

    Ok(())
}

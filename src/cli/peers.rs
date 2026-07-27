//! `jj-mesh peers`: list and manage paired peers.
//!
//! The listing shows live connection state (reachability, direct or relay
//! path, rtt) when the daemon is running, and falls back to the stored mesh
//! state otherwise.

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, bail};

use super::status::{connection_summary, name_width};
use crate::{
    config::{ConfigDir, MachineKey, MeshState},
    daemon::control::{self, Request, Response},
};

/// List the paired machines
#[derive(Debug, Args)]
pub struct PeersArgs {
    #[command(subcommand)]
    command: Option<PeersCommand>,
}

#[derive(Debug, Subcommand)]
enum PeersCommand {
    /// Remove a paired machine
    ///
    /// The daemon disconnects the peer immediately. It must be running.
    Remove {
        /// Name of the peer to remove
        name: String,
    },
}

/// Runs the `peers` command.
pub fn run(args: PeersArgs, dir: &ConfigDir) -> Result<()> {
    match args.command {
        None => list(dir),
        Some(PeersCommand::Remove { name }) => remove(dir, &name),
    }
}

/// Asks the daemon to remove a peer.
fn remove(dir: &ConfigDir, name: &str) -> Result<()> {
    let request = Request::RemovePeer {
        name: name.to_owned(),
    };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::PeerRemoved(endpoint) = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!("Removed peer `{name}` ({endpoint})");
    Ok(())
}

/// Lists the peers, live when the daemon answers.
fn list(dir: &ConfigDir) -> Result<()> {
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
        let state = MeshState::load(dir)?;
        if state.peers.is_empty() {
            println!("this machine has no paired peers (run `jj-mesh pair` to add one)");
            return Ok(());
        }

        println!("(daemon not running; showing the stored mesh state)");
        let width = name_width(state.peers.keys().map(String::as_str));
        for (name, peer) in &state.peers {
            println!("{name:width$}  {}", peer.endpoint);
        }
    }

    Ok(())
}

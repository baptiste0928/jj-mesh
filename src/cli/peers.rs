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
    /// Remove a machine from the mesh
    ///
    /// The daemon disconnects the peer immediately and propagates the
    /// removal to the other machines. It must be running.
    Remove {
        /// Name of the peer to remove (or its endpoint id, when several
        /// peers share a name)
        peer: String,
    },
}

/// Runs the `peers` command.
pub fn run(args: PeersArgs, dir: &ConfigDir) -> Result<()> {
    match args.command {
        None => list(dir),
        Some(PeersCommand::Remove { peer }) => remove(dir, &peer),
    }
}

/// Asks the daemon to remove a peer.
fn remove(dir: &ConfigDir, peer: &str) -> Result<()> {
    let request = Request::RemovePeer {
        peer: peer.to_owned(),
    };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::PeerRemoved(endpoint) = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!("Removed peer `{peer}` ({endpoint})");
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
        if state.alive_peers().count() == 0 {
            println!("this machine has no paired peers (run `jj-mesh pair` to add one)");
            return Ok(());
        }

        println!("(daemon not running; showing the stored mesh state)");
        let width = name_width(state.alive_peers().map(|(_, name)| name));
        for (endpoint, name) in state.alive_peers() {
            println!("{name:width$}  {endpoint}");
        }
    }

    Ok(())
}

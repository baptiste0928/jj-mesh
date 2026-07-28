//! `jj-mesh peer remove`: unpair a machine from the mesh.

use clap::Args;
use color_eyre::eyre::{Result, bail};

use crate::{
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Remove a machine from the mesh
///
/// The removal is immediately propagated to other machines in the mesh.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Name of the peer to remove
    peer: String,
}

/// Runs the `peer remove` command.
pub fn run(args: RemoveArgs, dir: &ConfigDir) -> Result<()> {
    let RemoveArgs { peer } = args;
    let request = Request::RemovePeer { peer: peer.clone() };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::PeerRemoved(endpoint) = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!("Removed peer `{peer}` ({endpoint})");
    Ok(())
}

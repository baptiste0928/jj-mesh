//! `jj-mesh peer remove`: unpair a machine from the mesh.

use clap::Args;
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{Result, bail};

use crate::{
    cli::{complete, ui},
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Remove a machine from the mesh
///
/// The removal is immediately propagated to other machines in the mesh.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Name of the peer to remove
    #[arg(add = ArgValueCandidates::new(complete::peers))]
    peer: String,
}

/// Runs the `peer remove` command.
pub fn run(args: RemoveArgs, dir: &ConfigDir) -> Result<()> {
    let RemoveArgs { peer } = args;
    let request = Request::RemovePeer { peer: peer.clone() };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::PeerRemoved(_) = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!("{}", ui::good(format_args!("Removed peer `{peer}`")));
    Ok(())
}

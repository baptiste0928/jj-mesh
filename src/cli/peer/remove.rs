//! `jj-mesh peer remove`: unpair a machine from the mesh.

use anyhow::{Result, bail};
use clap::Args;
use clap_complete::ArgValueCandidates;

use crate::{
    cli::{complete, ui},
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Remove a machine from the mesh
///
/// Every machine disconnects it as soon as the removal reaches it. Pair it
/// again to bring it back.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Name of the peer to remove
    #[arg(add = ArgValueCandidates::new(complete::peers))]
    peer: String,

    /// Skip the confirmation prompt
    #[arg(long, short = 'y')]
    yes: bool,
}

/// Runs the `peer remove` command.
pub fn run(args: RemoveArgs, dir: &ConfigDir) -> Result<()> {
    let RemoveArgs { peer, yes } = args;
    if !yes && !ui::approve(&format!("Remove peer {peer} from the whole mesh?"))? {
        ui::info("Aborted");
        return Ok(());
    }
    let request = Request::RemovePeer { peer: peer.clone() };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::PeerRemoved(_) = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    ui::success(format_args!("Removed peer {peer}"));
    Ok(())
}

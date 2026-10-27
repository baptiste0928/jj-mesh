//! `jj-mesh setup`: get this machine onto the mesh.
//!
//! ```text
//! check jj ─▶ start the daemon ─▶ name the machine ─▶ server mode ─▶ pair
//!             (install or start    (only before the   (with           (print a ticket,
//!              the service)         first peer/repo)   `--server`)     or redeem one)
//! ```

use std::path::Path;

use anyhow::Result;
use clap::Args;

use super::{peer, server, service, ui};
use crate::{
    config::{ConfigDir, validate_name},
    hint::hinted,
    net::pair::PairTicket,
    repo::{jj_bin, jj_version, jj_version_warning},
};

/// Set up this machine and pair it with the mesh
///
/// Installs the background service, names this machine, then prints a
/// pairing ticket to run on another machine.
///
/// Pass a ticket printed on another machine to join its mesh instead.
///
/// Every step but pairing is skipped when already done.
#[derive(Debug, Args)]
pub struct SetupArgs {
    /// Pairing ticket printed by `jj-mesh setup` or `jj-mesh peer add`
    ticket: Option<PairTicket>,

    /// Run this machine in server mode (see `jj-mesh server`)
    #[arg(long)]
    server: bool,
}

/// Runs the `setup` command.
pub fn run(args: SetupArgs, dir: &ConfigDir) -> Result<()> {
    // Resolved as the service will run it.
    let path = std::env::var_os("PATH").unwrap_or_default();
    let jj = crate::service::which(Path::new(&jj_bin()), &path).ok_or_else(|| {
        hinted(
            "jj is not found on `PATH` or through `JJ_BIN`",
            "Install jj, or set `JJ_BIN` to its path",
        )
    })?;
    if let Some(warning) = jj_version_warning(jj_version(jj.as_os_str()).as_deref()) {
        ui::warning(warning);
    }

    let status = service::ensure_running(dir)?;

    // The machine name becomes the workspace name in synced repos, and
    // renaming the machine later does not rename them.
    if status.peers.is_empty() && status.repos.is_empty() {
        let name = ui::input("Name of this machine", &status.name, |name| {
            validate_name("machine", name)
        })?;
        if let Some(name) = name.filter(|name| *name != status.name) {
            peer::rename(dir, &name)?;
        }
    }

    if args.server && !status.server {
        server::set(dir, true)?;
    }

    peer::pair(dir, args.ticket)?;

    ui::hint(
        "Add a repo to the mesh with `jj-mesh add`, or clone one from another machine with \
         `jj-mesh clone`",
    );
    Ok(())
}

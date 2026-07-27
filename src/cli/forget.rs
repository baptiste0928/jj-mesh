//! `jj-mesh forget`: retire a repo from the mesh.

use clap::Args;
use color_eyre::eyre::{Result, bail};

use crate::{
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Remove a repo from the mesh
///
/// Every machine stops synchronizing the repo and drops it from its mesh
/// state; the repository files are left untouched everywhere. The daemon
/// must be running.
#[derive(Debug, Args)]
pub struct ForgetArgs {
    /// Name of the repo in the mesh
    name: String,
}

/// Runs the `forget` command.
pub fn run(args: &ForgetArgs, dir: &ConfigDir) -> Result<()> {
    let request = Request::ForgetRepo {
        name: args.name.clone(),
    };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::RepoForgotten { was_local } = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!("Forgot repo `{}` on the mesh", args.name);
    if was_local {
        println!("It is no longer synchronized here; its files are untouched.");
    }
    Ok(())
}

//! `jj-mesh repo forget`: unregister a repo on this machine only.

use clap::Args;
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{Result, bail};

use crate::{
    cli::{complete, ui},
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Forget the local instance of a repo
///
/// This machine stops synchronizing the repo and unregisters it, without
/// touching any of the local files. Other machines keep syncing it.
///
/// If you want to completely remove a repo from the mesh, use `jj-mesh repo
/// remove`.
#[derive(Debug, Args)]
pub struct ForgetArgs {
    /// Name of the repo in the mesh
    #[arg(add = ArgValueCandidates::new(complete::registered_repos))]
    name: String,
}

/// Runs the `repo forget` command.
pub fn run(args: ForgetArgs, dir: &ConfigDir) -> Result<()> {
    let ForgetArgs { name } = args;
    let request = Request::ForgetRepo { name: name.clone() };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::RepoForgotten { .. } = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!(
        "{}",
        ui::good(format_args!(
            "Forgot repo `{name}`. It is no longer synchronized on this machine."
        )),
    );
    Ok(())
}

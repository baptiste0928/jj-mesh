//! `jj-mesh repo rename`: rename a repo across the whole mesh.

use anyhow::{Result, bail};
use clap::Args;
use clap_complete::ArgValueCandidates;

use crate::{
    cli::{complete, ui},
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Rename a repo across the whole mesh
///
/// Every machine holding the repo keeps syncing it under the new name. Its
/// files and workspaces are left untouched.
#[derive(Debug, Args)]
pub struct RenameArgs {
    /// Name or id of the repo in the mesh
    #[arg(add = ArgValueCandidates::new(complete::mesh_repos))]
    name: String,

    /// New name of the repo
    new: String,
}

/// Runs the `repo rename` command.
pub fn run(args: RenameArgs, dir: &ConfigDir) -> Result<()> {
    let RenameArgs { name, new } = args;
    let request = Request::RenameRepo {
        name: name.clone(),
        new: new.clone(),
    };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::RepoRenamed = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    ui::success(format_args!("Renamed repo {name} to {new}"));
    Ok(())
}

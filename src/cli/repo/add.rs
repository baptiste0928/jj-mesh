//! `jj-mesh repo add`: register a repo on the mesh.

use std::path::PathBuf;

use clap::{Args, ValueHint};
use color_eyre::eyre::{Result, bail, eyre};

use crate::{
    cli::ui,
    config::ConfigDir,
    daemon::control::{self, Request, Response},
    repo::JjRepo,
};

/// Add a repo to the mesh
///
/// The repo will be made available for other machines to clone with
/// `jj-mesh repo clone`, and any changes will be synced across the mesh.
#[derive(Debug, Args)]
pub struct AddArgs {
    /// Path inside the jj repo to add (defaults to the current directory)
    #[arg(value_hint = ValueHint::DirPath)]
    path: Option<PathBuf>,

    /// Name of the repo in the mesh (defaults to the repo directory name)
    #[arg(long)]
    name: Option<String>,
}

/// Runs the `repo add` command.
pub fn run(args: AddArgs, dir: &ConfigDir) -> Result<()> {
    let path = args.path.unwrap_or_else(|| PathBuf::from("."));
    let repo = JjRepo::discover(&path)?;

    let name = match args.name {
        Some(name) => name,
        None => repo
            .root()
            .file_name()
            .ok_or_else(|| eyre!("cannot derive a name from {}, use --name", path.display()))?
            .to_string_lossy()
            .into_owned(),
    };

    let request = Request::AddRepo {
        name: name.clone(),
        path: repo.root().to_owned(),
    };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::RepoAdded = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!(
        "{}",
        ui::good(format_args!(
            "Added repo `{name}` at {}",
            repo.root().display()
        ))
    );
    Ok(())
}

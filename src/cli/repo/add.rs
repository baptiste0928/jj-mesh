//! `jj-mesh repo add`: register a repo on the mesh.

use std::path::PathBuf;

use clap::{Args, ValueHint};
use color_eyre::eyre::{Result, bail, eyre};

use crate::{
    cli::{machine_name, ui},
    config::ConfigDir,
    daemon::control::{self, Request, Response},
    repo::Workspace,
};

/// Add a repo to the mesh
///
/// The repo will be made available for other machines to clone with
/// `jj-mesh repo clone`, and any changes will be synced across the mesh.
/// Every workspace of the repo on this machine is kept up to date; run from
/// a secondary workspace, this adds the repo it belongs to.
#[derive(Debug, Args)]
pub struct AddArgs {
    /// Path inside the jj repo to add (defaults to the current directory)
    #[arg(value_hint = ValueHint::DirPath)]
    path: Option<PathBuf>,

    /// Name of the repo in the mesh (defaults to the repo directory name)
    #[arg(long)]
    name: Option<String>,

    /// Override the main workspace name (defaults to this machine name)
    ///
    /// We assign a workspace for each copy of the repo across the mesh, so
    /// the current head of each machine is displayed in `jj log`.
    #[arg(long)]
    workspace: Option<String>,
}

/// Runs the `repo add` command.
pub fn run(args: AddArgs, dir: &ConfigDir) -> Result<()> {
    let path = args.path.unwrap_or_else(|| PathBuf::from("."));
    let workspace = Workspace::discover(&path)?;
    let repo = workspace.repo()?;
    if !workspace.is_main() {
        eprintln!(
            "{} {} is a secondary workspace, adding its repo at {} (all its workspaces are synced)",
            ui::warn("warning:").for_stderr(),
            workspace.root().display(),
            repo.root().display(),
        );
    }

    let name = match args.name {
        Some(name) => name,
        None => repo
            .root()
            .file_name()
            .ok_or_else(|| eyre!("cannot derive a name from {}, use --name", path.display()))?
            .to_string_lossy()
            .into_owned(),
    };

    // A main workspace still on jj's `default` name gets the machine name,
    // so workspace names stay unique across the mesh; a deliberately named
    // workspace is kept as-is.
    let rename = match args.workspace {
        Some(name) => Some(name),
        None if repo.workspace()?.name()? == "default" => Some(machine_name(dir)?),
        None => None,
    };
    if let Some(rename) = &rename {
        super::jj(Some(repo.root()), &["workspace", "rename", rename])?;
    }

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

//! `jj-mesh repo add`: register a repo on the mesh.

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Args, ValueHint};

use super::settings::{Scope, SettingsArgs};
use crate::{
    cli::ui,
    config::{ConfigDir, MeshState},
    daemon::control::{self, Request, Response},
    hint::hinted,
    repo::Workspace,
};

/// Add a repo to the mesh
///
/// The repo will be made available for other machines to clone with
/// `jj-mesh clone`, and any changes will be synced across the mesh.
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

    #[command(flatten)]
    settings: SettingsArgs,
}

/// Runs the `repo add` command.
pub fn run(args: AddArgs, dir: &ConfigDir) -> Result<()> {
    let path = args.path.unwrap_or_else(|| PathBuf::from("."));
    let workspace = Workspace::discover(&path)?;
    let repo = workspace.repo()?;
    if !workspace.is_main() {
        ui::warning(format_args!(
            "{} is a secondary workspace, adding its repo at {} (all its workspaces are synced)",
            ui::path(workspace.root()),
            ui::path(repo.root()),
        ));
    }

    let name = match args.name {
        Some(name) => name,
        None => repo
            .root()
            .file_name()
            .ok_or_else(|| {
                hinted(
                    format!("cannot derive a name from {}", ui::path(&path)),
                    "Pass one with `--name`",
                )
            })?
            .to_string_lossy()
            .into_owned(),
    };

    // Best-effort pre-checks, so a doomed add fails before the rename and
    // settings below change the repo (the daemon re-validates).
    control::ensure_daemon_blocking(dir)?;
    let state = MeshState::load(dir)?;
    state.validate_new_repo(&name, repo.root())?;

    // A main workspace still on jj's `default` name gets the machine name,
    // so workspace names stay unique across the mesh; a deliberately named
    // workspace is kept as-is.
    let rename = match args.workspace {
        Some(name) => Some(name),
        None if repo.workspace()?.name()? == "default" => Some(state.machine.name),
        None => None,
    };
    if let Some(rename) = &rename {
        super::jj(Some(repo.root()), &["workspace", "rename", rename])?;
    }

    // Written before registration: the daemon reads them once.
    args.settings.write(repo.root(), Scope::Repo)?;

    let request = Request::AddRepo {
        name: name.clone(),
        path: repo.root().to_owned(),
    };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::RepoAdded = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    ui::success(format_args!(
        "Added repo {name} at {}",
        ui::path(repo.root())
    ));
    ui::hint(format_args!(
        "Clone it on another machine with `jj-mesh clone {}`",
        shell_words::quote(&name),
    ));
    Ok(())
}

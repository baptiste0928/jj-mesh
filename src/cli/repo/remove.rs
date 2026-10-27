//! `jj-mesh repo remove`: retire repos from the whole mesh.

use anyhow::{Context as _, Result, bail};
use clap::Args;
use clap_complete::ArgValueCandidates;

use super::batch::{self, Batch, confirm, show_workspaces};
use crate::{
    cli::{complete, ui},
    config::{ConfigDir, MeshState, RepoId},
    daemon::control::{self, Request, Response},
    repo::{JjRepo, Workspace},
};

/// Remove repos from the whole mesh
///
/// Every machine stops synchronizing the repos. The files stay untouched,
/// except on servers: their bare repo is deleted unless a workspace uses it.
///
/// If you want to stop synchronizing on this machine only, use `jj-mesh repo
/// forget` instead.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Names or ids of the repos in the mesh
    #[arg(required = true, add = ArgValueCandidates::new(complete::mesh_repos))]
    repos: Vec<String>,

    /// Also delete the workspaces of the repos on this machine
    #[arg(long)]
    delete_files: bool,

    /// Skip the confirmation prompt
    #[arg(long, short = 'y')]
    yes: bool,
}

/// A mesh repo to remove, checked.
struct Remove {
    id: RepoId,
    name: String,
    /// Its workspaces here, to delete.
    delete: Vec<Workspace>,
}

/// Runs the `repo remove` command.
pub fn run(args: RemoveArgs, dir: &ConfigDir) -> Result<()> {
    let RemoveArgs {
        repos,
        delete_files,
        yes,
    } = args;
    let state = MeshState::load(dir)?;
    let repos = batch::check(&state, &repos, |id| Remove::check(&state, id, delete_files))?;

    if !yes {
        for repo in &repos {
            repo.show();
        }
        let names = repos.iter().map(|repo| &*repo.name);
        if !confirm(
            "Remove",
            names,
            " from every machine on the mesh",
            delete_files,
        )? {
            return Ok(());
        }
    }

    // Files go once every repo is removed: a workspace may contain another
    // repo's.
    let mut batch = Batch::new(repos.len());
    let removed = batch.run(&repos, |repo| repo.apply(dir, delete_files));
    batch.run(removed, Remove::delete);
    batch.finish("failed")
}

impl Remove {
    /// Lists the workspaces of the mesh repo `id` to delete, when asked.
    fn check(state: &MeshState, id: RepoId, delete_files: bool) -> Result<Self> {
        let delete = match state.repos.get(&id) {
            Some(repo) if delete_files => JjRepo::local_workspaces_at(&repo.path)?,
            _ => Vec::new(),
        };
        Ok(Remove {
            name: state.repo_name(&id).unwrap_or("?").to_owned(),
            id,
            delete,
        })
    }

    /// Lists the workspaces to delete.
    fn show(&self) {
        if !self.delete.is_empty() {
            ui::info(format_args!("{}:", self.name));
            show_workspaces(&self.delete);
        }
    }

    /// Removes the repo through the daemon.
    fn apply(&self, dir: &ConfigDir, delete_files: bool) -> Result<()> {
        let name = &self.name;
        let request = Request::RemoveRepo {
            name: self.id.to_string(),
        };
        let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)
            .with_context(|| format!("cannot remove {name}"))?;
        let Response::RepoRemoved { was_local } = response else {
            bail!("unexpected response from the daemon: {response:?}");
        };
        ui::success(format_args!("Removed {name} from the mesh"));
        if was_local && !delete_files {
            ui::info("Its files on this machine are left untouched");
        }
        Ok(())
    }

    /// Deletes the workspaces listed for deletion.
    fn delete(&self) -> Result<()> {
        Workspace::delete_all(&self.delete)
            .with_context(|| format!("removed {}, but could not delete all its files", self.name))
    }
}

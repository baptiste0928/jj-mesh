//! `jj-mesh repo forget`: stop syncing repos on this machine only.

use clap::Args;
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{OptionExt as _, Result, WrapErr as _, bail};

use super::batch::{self, Batch, confirm, show_workspaces};
use crate::{
    cli::{complete, ui},
    config::{ConfigDir, MeshState, Repo, RepoId},
    daemon::control::{self, Request, Response},
    repo::{JjRepo, Workspace},
};

/// Forget the local instance of repos
///
/// This machine stops synchronizing each repo, with all its workspaces here,
/// and unregisters it. Other machines keep syncing it.
///
/// If you want to completely remove a repo from the mesh, use `jj-mesh repo
/// remove`.
#[derive(Debug, Args)]
pub struct ForgetArgs {
    /// Names or ids of the repos in the mesh (defaults to the current repo)
    #[arg(add = ArgValueCandidates::new(complete::registered_repos))]
    repos: Vec<String>,

    /// Also delete the workspaces of the repos on this machine
    #[arg(long)]
    delete_files: bool,

    /// Skip the confirmation prompt
    #[arg(long, short = 'y')]
    yes: bool,
}

/// A registered repo to forget, checked.
struct Forget {
    id: RepoId,
    name: String,
    action: Action,
    /// Its workspaces here, empty when they cannot be listed.
    workspaces: Vec<Workspace>,
    delete: bool,
}

/// What forgetting a repo does.
enum Action {
    /// Its storage moves into a bare repo, made non-colocated first.
    Store { colocated: bool },
    /// It is unregistered here.
    Unregister {
        /// Whether its directory is gone.
        missing: bool,
        /// The other machines holding it.
        holders: Vec<String>,
    },
}

impl Action {
    /// The verb for errors.
    fn verb(&self) -> &'static str {
        match self {
            Action::Store { .. } => "move",
            Action::Unregister { .. } => "forget",
        }
    }
}

/// Runs the `repo forget` command.
pub fn run(args: ForgetArgs, dir: &ConfigDir) -> Result<()> {
    let ForgetArgs {
        repos,
        delete_files,
        yes,
    } = args;
    let state = MeshState::load(dir)?;
    let check = |id| Forget::check(&state, id, delete_files);
    let repos = if repos.is_empty() {
        vec![check(current_repo(&state)?)?]
    } else {
        batch::check(&state, &repos, check)?
    };

    if !yes {
        for repo in &repos {
            repo.show();
        }
        let (verb, scope) = if repos
            .iter()
            .all(|repo| matches!(repo.action, Action::Store { .. }))
        {
            ("Move", " into bare repos")
        } else {
            ("Forget", " on this machine")
        };
        let names = repos.iter().map(|repo| &*repo.name);
        if !confirm(verb, names, scope, delete_files)? {
            return Ok(());
        }
    }

    // Files go once every repo is forgotten: a workspace may contain
    // another repo's.
    let mut batch = Batch::new(repos.len());
    let forgotten = batch.run(&repos, |repo| repo.apply(dir));
    batch.run(forgotten, Forget::delete);
    batch.finish()
}

impl Forget {
    /// Checks that the registered repo `id` can be forgotten.
    fn check(state: &MeshState, id: RepoId, delete: bool) -> Result<Self> {
        let name = state.repo_name(&id).unwrap_or("?").to_owned();
        let Some(repo) = state.repos.get(&id) else {
            bail!("{name} is not registered on this machine");
        };
        let missing = JjRepo::is_missing(&repo.path);
        let action = if state.machine.server && !repo.bare && !missing {
            state.check_store(&id)?;
            let colocated = JjRepo::at(&repo.path)?.is_colocated()?;
            Action::Store { colocated }
        } else {
            state.check_forget(&id, missing)?;
            let holders = state.holders(&id).into_iter().map(str::to_owned);
            Action::Unregister {
                missing,
                holders: holders.collect(),
            }
        };
        Ok(Forget {
            workspaces: workspaces(repo, delete || matches!(action, Action::Store { .. }))?,
            id,
            name,
            action,
            delete,
        })
    }

    /// Describes what forgetting does, on stderr.
    fn show(&self) {
        match self.action {
            Action::Store { .. } => eprintln!("{}: moves to a bare repo", self.name),
            Action::Unregister { .. } => eprintln!("{}:", self.name),
        }
        show_workspaces(&self.workspaces);
        let note = |note: &str| eprintln!("  {}", ui::dim(note).for_stderr());
        match &self.action {
            Action::Store { colocated: true } => note("(colocation will be disabled)"),
            Action::Store { colocated: false } => {}
            Action::Unregister { missing, holders } => {
                if *missing {
                    note("(directory missing)");
                }
                if !holders.is_empty() {
                    eprintln!("  Held by: {}", holders.join(", "));
                }
            }
        }
    }

    /// Forgets the repo through the daemon.
    fn apply(&self, dir: &ConfigDir) -> Result<()> {
        let name = &self.name;
        let repo = self.id.to_string();
        let request = match self.action {
            Action::Store { .. } => Request::StoreRepo { repo },
            Action::Unregister { .. } => Request::ForgetRepo { name: repo },
        };
        let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)
            .wrap_err_with(|| format!("cannot {} {name}", self.action.verb()))?;
        let message = match response {
            Response::RepoForgotten { .. } => format!("Forgot {name}"),
            Response::RepoStored { path } => {
                format!("Moved {name} to {}", ui::display_path(&path))
            }
            response => bail!("unexpected response from the daemon: {response:?}"),
        };
        println!("{}", ui::good(message));
        Ok(())
    }

    /// Deletes the workspaces, when asked.
    fn delete(&self) -> Result<()> {
        if !self.delete {
            return Ok(());
        }
        Workspace::delete_all(&self.workspaces).wrap_err_with(|| {
            let done = match self.action {
                Action::Store { .. } => "moved",
                Action::Unregister { .. } => "forgot",
            };
            format!("{done} {}, but could not delete all its files", self.name)
        })
    }
}

/// The workspaces of `repo` here. Only informative unless `needed`: a
/// broken repo must remain forgettable.
fn workspaces(repo: &Repo, needed: bool) -> Result<Vec<Workspace>> {
    match JjRepo::local_workspaces_at(&repo.path) {
        Err(_) if !needed => Ok(Vec::new()),
        listed => listed,
    }
}

/// The registered repo of the current directory.
fn current_repo(state: &MeshState) -> Result<RepoId> {
    let cwd = std::env::current_dir().wrap_err("cannot read the current directory")?;
    let repo = JjRepo::discover(&cwd)?;
    state
        .repo_at(repo.root())
        .cloned()
        .ok_or_eyre("the current repo is not registered on this machine")
}

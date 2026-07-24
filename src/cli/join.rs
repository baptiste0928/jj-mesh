//! `jj-mesh join`: bootstrap a mesh repo onto this machine.
//!
//! Creates a fresh jj repo, gives its workspace a machine-unique name
//! (mesh machines must never share one), asks the daemon to pull the mesh
//! repo's full state from a peer, registers the repo, and lets jj merge
//! the fresh workspace into the replicated history.

use std::{path::PathBuf, process::Command, time::Duration};

use clap::Args;
use color_eyre::eyre::{Result, WrapErr as _, bail, ensure, eyre};

use crate::{
    config::{ConfigDir, ConfigEdit, Repo, RepoId},
    daemon::control::{ControlClient, Request, Response},
};

/// The daemon's pull may transfer an entire repository.
const JOIN_WAIT: Duration = Duration::from_mins(31);

/// Join a repo another machine added to the mesh
///
/// Find the repo id with `jj-mesh status` on the machine that has the
/// repo. The daemon must be running and connected to that machine.
#[derive(Debug, Args)]
pub struct JoinArgs {
    /// Mesh id of the repo (32 hex characters)
    repo_id: String,

    /// Directory to create the repo in (must not exist yet)
    path: PathBuf,

    /// Name of the repo in the mesh configuration (defaults to the
    /// directory name)
    #[arg(long)]
    name: Option<String>,

    /// This machine's workspace name in the repo (defaults to the
    /// hostname). Must differ from every other machine's.
    #[arg(long)]
    workspace: Option<String>,
}

/// Runs the `join` command.
pub fn run(args: JoinArgs, dir: &ConfigDir) -> Result<()> {
    let repo_id = RepoId::try_from(args.repo_id).map_err(|err| eyre!(err))?;

    let edit = ConfigEdit::from_config(dir)?;
    if let Some((name, _)) = edit
        .config()
        .repos
        .iter()
        .find(|(_, repo)| repo.id == repo_id)
    {
        bail!("repo {repo_id} is already registered here as `{name}`");
    }

    ensure!(
        !args.path.exists(),
        "{} already exists; join creates the directory itself",
        args.path.display(),
    );
    let name = match &args.name {
        Some(name) => name.clone(),
        None => args
            .path
            .file_name()
            .ok_or_else(|| eyre!("cannot derive a name from the path, use --name"))?
            .to_string_lossy()
            .into_owned(),
    };
    let workspace = match args.workspace {
        Some(name) => name,
        None => gethostname::gethostname().to_string_lossy().into_owned(),
    };

    // Fresh repo with a machine-unique workspace name, using the user's
    // own jj (their config applies to the repo from the start).
    jj(None, &["git", "init", &args.path.to_string_lossy()])?;
    jj(Some(&args.path), &["workspace", "rename", &workspace])?;

    println!("Pulling repo {repo_id} from the mesh...");
    let pulled = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(pull(dir, &repo_id, &args.path));
    let (ops, git_objects) = pulled.wrap_err_with(|| {
        format!(
            "the repo directory {} was created but not registered; remove it before retrying",
            args.path.display(),
        )
    })?;

    let mut edit = ConfigEdit::from_config(dir)?;
    edit.add_repo(
        name.clone(),
        Repo {
            id: repo_id.clone(),
            path: std::fs::canonicalize(&args.path)?,
        },
    )?;
    edit.save()?;

    // Any jj command merges the fresh workspace into the pulled history;
    // doing it here leaves the repo ready to use.
    jj(Some(&args.path), &["status"])?;

    println!("Joined `{name}` ({repo_id}): {ops} operations, {git_objects} git objects");
    Ok(())
}

/// Asks the daemon to pull the repo's state from a peer.
async fn pull(dir: &ConfigDir, repo_id: &RepoId, path: &PathBuf) -> Result<(u64, u64)> {
    let Some(mut client) = ControlClient::connect(dir).await? else {
        bail!("the jj-mesh daemon is not running");
    };
    client
        .send(&Request::JoinRepo {
            repo: repo_id.clone(),
            path: std::fs::canonicalize(path)?,
        })
        .await?;
    match client.recv(Some(JOIN_WAIT)).await? {
        Response::Joined { ops, git_objects } => Ok((ops, git_objects)),
        Response::Error(message) => bail!(message),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

/// Runs a jj command, surfacing its stderr on failure.
fn jj(dir: Option<&PathBuf>, args: &[&str]) -> Result<()> {
    let mut command = Command::new("jj");
    if let Some(dir) = dir {
        command.current_dir(dir);
    }
    let out = command
        .args(args)
        .output()
        .wrap_err("cannot run jj (is it installed?)")?;
    ensure!(
        out.status.success(),
        "jj {} failed:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr),
    );
    Ok(())
}

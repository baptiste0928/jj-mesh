//! `jj-mesh join`: bootstrap a mesh repo onto this machine.
//!
//! Creates a fresh jj repo, gives its workspace a machine-unique name
//! (mesh machines must never share one), asks the daemon to pull the mesh
//! repo's full state from a peer and register it, and lets jj merge the
//! fresh workspace into the replicated history.

use std::{path::PathBuf, process::Command};

use clap::Args;
use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};

use crate::{
    config::{ConfigDir, MeshState},
    daemon::control::{self, Request, Response},
};

/// Join a repo another machine added to the mesh
///
/// Find the repo name with `jj-mesh status` on the machine that has the
/// repo. The daemon must be running and connected to that machine.
#[derive(Debug, Args)]
pub struct JoinArgs {
    /// Name of the repo in the mesh
    name: String,

    /// Directory to create the repo in (must not exist yet; defaults to
    /// the repo name in the current directory)
    path: Option<PathBuf>,

    /// This machine's workspace name in the repo (defaults to the
    /// hostname). Must differ from every other machine's.
    #[arg(long)]
    workspace: Option<String>,
}

/// Runs the `join` command.
pub fn run(args: JoinArgs, dir: &ConfigDir) -> Result<()> {
    let name = args.name;
    let path = args.path.unwrap_or_else(|| PathBuf::from(&name));

    // Best-effort pre-check against the stored state, so an obviously
    // doomed join fails before anything is created on disk; the daemon
    // re-validates authoritatively before registering.
    MeshState::load(dir)?.validate_new_repo(&name, &path)?;
    ensure!(
        !path.exists(),
        "{} already exists; join creates the directory itself",
        path.display(),
    );

    let workspace = match args.workspace {
        Some(name) => name,
        None => gethostname::gethostname().to_string_lossy().into_owned(),
    };

    // Fresh repo with a machine-unique workspace name, using the user's
    // own jj (their config applies to the repo from the start).
    jj(None, &["git", "init", &path.to_string_lossy()])?;
    jj(Some(&path), &["workspace", "rename", &workspace])?;

    println!("Pulling repo `{name}` from the mesh...");
    let request = Request::JoinRepo {
        name: name.clone(),
        path: std::fs::canonicalize(&path)?,
    };
    let pulled = control::request_blocking(dir, &request, control::JOIN_WAIT);
    let response = pulled.wrap_err_with(|| {
        format!(
            "the repo directory {} was created but not registered; remove it before retrying",
            path.display(),
        )
    })?;
    let Response::Joined { ops, git_objects } = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    // Any jj command merges the fresh workspace into the pulled history;
    // doing it here leaves the repo ready to use.
    jj(Some(&path), &["status"])?;

    println!("Joined `{name}`: {ops} operations, {git_objects} git objects");
    Ok(())
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

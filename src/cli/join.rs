//! `jj-mesh join`: bootstrap a mesh repo onto this machine.
//!
//! Creates a fresh jj repo, gives its workspace a machine-unique name
//! (mesh machines must never share one), asks the daemon to pull the mesh
//! repo's full state from a peer and register it, and lets jj merge the
//! fresh workspace into the replicated history.
//!
//! The repo is never colocated: the view's `git_head` is single-valued
//! but mirrors machine-local state (the colocated `.git`'s HEAD), so a
//! mesh repo supports at most one colocated checkout. A second one makes
//! every jj command re-import the local HEAD as a working-copy move,
//! resurrecting rewritten commits as divergent changes.

use std::{path::PathBuf, process::Command};

use clap::Args;
use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};

use crate::{
    config::{ConfigDir, MeshState},
    daemon::control::{self, JoinProgress, Request, Response},
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
    // own jj (their config applies to the repo from the start). Never
    // colocated (see the module docs), whatever the user's `git.colocate`.
    jj(
        None,
        &["git", "init", "--no-colocate", &path.to_string_lossy()],
    )?;
    jj(Some(&path), &["workspace", "rename", &workspace])?;

    println!("Pulling repo `{name}` from the mesh...");
    let request = Request::JoinRepo {
        name: name.clone(),
        path: std::fs::canonicalize(&path)?,
    };
    // The progress line only makes sense on a terminal: redirected output
    // would collect thousands of carriage-returned fragments.
    let tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let mut progressing = false;
    let pulled =
        control::request_streaming_blocking(dir, &request, control::JOIN_IDLE_WAIT, |progress| {
            if tty {
                progressing = true;
                show_progress(&progress);
            }
        });
    // The progress line is overwritten in place; finish it before anything
    // else (the outcome or an error report) prints.
    if progressing {
        println!();
    }
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

/// Renders a progress frame in place on the current line.
fn show_progress(progress: &JoinProgress) {
    use std::io::Write as _;

    use crate::repo::transfer::TransferPhase;

    let JoinProgress { peer, transfer } = progress;
    let (current, bytes) = (transfer.current, transfer.bytes);
    let line = if peer.is_empty() {
        // Seeded state before the daemon picked a source peer.
        "Contacting peers...".to_owned()
    } else {
        match (transfer.phase, transfer.total) {
            (TransferPhase::Ops, Some(total)) => {
                format!("Pulling history from `{peer}`: {current}/{total} operations")
            }
            (TransferPhase::Ops, None) => {
                format!("Pulling history from `{peer}`: {current} operations")
            }
            (TransferPhase::Git, Some(total)) => format!(
                "Pulling files from `{peer}`: {current}/{total} objects ({})",
                human_bytes(bytes),
            ),
            (TransferPhase::Git, None) => {
                format!("Pulling files from `{peer}`: {}", human_bytes(bytes))
            }
            (TransferPhase::Apply, _) => "Writing the repo...".to_owned(),
        }
    };
    // Clear-to-end covers a new line shorter than the previous one.
    print!("\r{line}\x1b[K");
    let _ = std::io::stdout().flush();
}

/// Formats a byte count for the progress line.
fn human_bytes(bytes: u64) -> String {
    const KIB: u64 = 1 << 10;
    const MIB: u64 = 1 << 20;
    if bytes < KIB {
        format!("{bytes} B")
    } else if bytes < MIB {
        format!("{} KiB", bytes / KIB)
    } else {
        format!("{}.{} MiB", bytes / MIB, (bytes % MIB) * 10 / MIB)
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

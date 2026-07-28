//! `jj-mesh repo clone`: bootstrap a mesh repo onto this machine.
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
    daemon::control::{self, CloneProgress, Request, Response},
};

/// Clone a repo from another machine
///
/// The repo must have been added to the mesh with `jj-mesh repo add`.
///
/// Note that due to a limitation of `jj`, only one instance of each repo across
/// the mesh can be co-located. This command will clone the repo without
/// co-location enabled.
#[derive(Debug, Args)]
pub struct CloneArgs {
    /// Name of the repo in the mesh
    name: String,

    /// Directory to create the repo in (defaults to the repo name)
    ///
    /// The target directly must not exist before the clone. There is currently
    /// no way of adding a previous clone back into the mesh.
    path: Option<PathBuf>,

    /// This machine's workspace name in the repo (defaults to the
    /// hostname)
    ///
    /// We assign a workspace for each copy of the repo across the mesh, so the
    /// current head of each machine is displayed in `jj log`.
    #[arg(long)]
    workspace: Option<String>,
}

/// Runs the `repo clone` command.
pub fn run(args: CloneArgs, dir: &ConfigDir) -> Result<()> {
    let name = args.name;
    let path = args.path.unwrap_or_else(|| PathBuf::from(&name));

    // Best-effort pre-check against the stored state, so an obviously
    // doomed clone fails before anything is created on disk; the daemon
    // re-validates authoritatively before registering.
    MeshState::load(dir)?.validate_new_repo(&name, &path)?;
    ensure!(
        !path.exists(),
        "{} already exists; clone creates the directory itself",
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
    let request = Request::CloneRepo {
        name: name.clone(),
        path: std::fs::canonicalize(&path)?,
    };
    // The progress line only makes sense on a terminal: redirected output
    // would collect thousands of carriage-returned fragments.
    let tty = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let mut progressing = false;
    let pulled =
        control::request_streaming_blocking(dir, &request, control::CLONE_IDLE_WAIT, |progress| {
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
    let Response::Cloned { ops, git_objects } = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    // Any jj command merges the fresh workspace into the pulled history;
    // doing it here leaves the repo ready to use. A re-clone after
    // `repo forget` finds the previous instance's same-name workspace in
    // the pulled history, which leaves the fresh working copy stale:
    // recover it rather than fail a clone that already succeeded.
    if let Err(err) = jj(Some(&path), &["status"])
        && jj(Some(&path), &["workspace", "update-stale"]).is_err()
    {
        return Err(err);
    }

    println!("Cloned `{name}`: {ops} operations, {git_objects} git objects");
    Ok(())
}

/// Renders a progress frame in place on the current line.
fn show_progress(progress: &CloneProgress) {
    use std::io::Write as _;

    use crate::repo::transfer::TransferPhase;

    let CloneProgress { peer, transfer } = progress;
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

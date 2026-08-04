//! `jj-mesh repo clone`: bootstrap a mesh repo onto this machine.
//!
//! Creates a fresh jj repo, gives its workspace a machine-unique name
//! (mesh machines must never share one), asks the daemon to pull the mesh
//! repo's full state from a peer and register it, lets jj merge the
//! fresh workspace into the replicated history, and starts the working
//! copy on trunk.

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Args, ValueHint};
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};

use super::jj;
use crate::{
    cli::{complete, hostname, ui},
    config::{ConfigDir, MeshState},
    daemon::control::{self, CloneProgress, Request, Response, TransferPhase},
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
    #[arg(add = ArgValueCandidates::new(complete::clonable_repos))]
    name: String,

    /// Directory to create the repo in (defaults to the repo name)
    ///
    /// The target directory must not exist before the clone. There is currently
    /// no way of adding a previous clone back into the mesh.
    #[arg(value_hint = ValueHint::DirPath)]
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

    // Best-effort pre-checks, so an obviously doomed clone fails before
    // anything is created on disk: the daemon must be up (it is only
    // contacted once the local repo exists), and the stored state must
    // accept the repo (the daemon re-validates authoritatively before
    // registering).
    control::ensure_daemon_blocking(dir)?;
    MeshState::load(dir)?.validate_new_repo(&name, &path)?;
    ensure!(
        !path.exists(),
        "{} already exists (clone creates the directory)",
        path.display(),
    );

    let workspace = args.workspace.unwrap_or_else(hostname);

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
        path: std::fs::canonicalize(&path)
            .wrap_err_with(|| format!("cannot resolve {}", path.display()))?,
    };

    let bar = ProgressBar::new_spinner().with_style(spinner_style());
    bar.set_message("Contacting peers...");
    bar.enable_steady_tick(Duration::from_millis(100));
    let pulled =
        control::request_streaming_blocking(dir, &request, control::CLONE_IDLE_WAIT, |progress| {
            show_progress(&bar, &progress);
        });

    bar.finish_and_clear();
    let response = pulled.wrap_err_with(|| {
        format!(
            "the repo directory {} was created but not registered, remove it before retrying",
            path.display(),
        )
    })?;
    let Response::Cloned { .. } = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    // Any jj command merges the fresh workspace into the pulled history;
    // doing it here leaves the repo ready to use. A re-clone after
    // `repo forget` finds the previous instance's same-name workspace in
    // the pulled history, which leaves the fresh working copy stale:
    // recover it rather than fail a clone that already succeeded, and keep
    // its recovered position instead of jumping to trunk.
    match jj(Some(&path), &["status"]) {
        Ok(()) => start_on_trunk(&path),
        Err(err) => {
            if jj(Some(&path), &["workspace", "update-stale"]).is_err() {
                return Err(err);
            }
        }
    }

    println!(
        "{}",
        ui::good(format_args!("Cloned `{name}` in {}", path.display())),
    );
    Ok(())
}

/// Moves the working copy from the init commit off the root (where `jj git
/// init` left it, a confusing place to start) onto trunk; jj abandons the
/// init commit as it is empty and undescribed. jj's stock `trunk()` only
/// matches remote bookmarks, which a mesh-only repo has none of, so the
/// usual bookmark names are tried as local ones next; with none of them
/// the init position is kept. Best-effort: the clone itself succeeded.
fn start_on_trunk(path: &Path) {
    const LOCAL_TRUNK: &str =
        r#"latest(bookmarks(exact:"main") | bookmarks(exact:"master") | bookmarks(exact:"trunk"))"#;
    let _ = jj(Some(path), &["new", "latest(trunk() ~ root())"])
        .or_else(|_| jj(Some(path), &["new", LOCAL_TRUNK]));
}

/// Template while a phase has no known end: a spinner and a message.
fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {msg}").expect("static template")
}

/// Template once a phase's total is known: `prefix` names the phase and
/// source peer, `msg` carries the unit (and byte count for git objects).
fn counted_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.cyan} {prefix} {bar:24.cyan/blue} {pos}/{len} {msg}")
        .expect("static template")
}

/// Applies a progress frame to the bar, switching templates as phases and
/// totals come and go.
fn show_progress(bar: &ProgressBar, progress: &CloneProgress) {
    let CloneProgress { peer, transfer } = progress;
    let spin = |message: String| {
        bar.set_style(spinner_style());
        bar.set_message(message);
    };
    let counted = |total: u64| {
        bar.set_style(counted_style());
        bar.set_length(total);
        bar.set_position(transfer.current);
    };

    if peer.is_empty() {
        // Seeded state before the daemon picked a source peer.
        return spin("Contacting peers...".to_owned());
    }
    match transfer.phase {
        TransferPhase::Ops => match transfer.total {
            None => spin(format!(
                "Pulling history from `{peer}`: {} operations",
                transfer.current,
            )),
            Some(total) => {
                counted(total);
                bar.set_prefix(format!("Pulling history from `{peer}`"));
                bar.set_message("operations");
            }
        },
        TransferPhase::Git => match transfer.total {
            None => spin(format!(
                "Pulling files from `{peer}`: {}",
                HumanBytes(transfer.bytes),
            )),
            Some(total) => {
                counted(total);
                bar.set_prefix(format!("Pulling files from `{peer}`"));
                bar.set_message(format!("objects ({})", HumanBytes(transfer.bytes)));
            }
        },
        TransferPhase::Apply => spin("Writing the repo...".to_owned()),
    }
}

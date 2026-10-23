//! `jj-mesh repo clone`: bootstrap a mesh repo onto this machine.
//!
//! Creates a fresh jj repo, gives its workspace a machine-unique name, asks the
//! daemon to pull the mesh repo's full state from a peer and register it, lets
//! jj merge the fresh workspace into the replicated history, and starts the
//! working copy on trunk. A failed clone removes the directory it created.
//!
//! A repo already registered here gets a new workspace instead, which the
//! daemon syncs like the others. On a server, the workspace of a bare repo
//! is added through jj_lib (see [`JjRepo::add_workspace`]), since the jj CLI
//! needs a workspace to run in.
//!
//! Settings flags go to the repo config of a fresh clone or of a bare repo
//! without workspaces, and to the workspace config otherwise (see the
//! `settings` module).

use std::{
    path::{Component, Path, PathBuf},
    time::Duration,
};

use clap::{Args, ValueHint};
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{OptionExt as _, Report, Result, WrapErr as _, bail, ensure};
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use pollster::FutureExt as _;

use super::{
    jj,
    settings::{Scope, SettingsArgs},
};
use crate::{
    cli::{complete, ui},
    config::{ConfigDir, MeshRepo, MeshState, ObjectHash, Repo, validate_name},
    daemon::control::{self, CloneProgress, Request, Response, TransferPhase, WorkspaceHold},
    repo::{Identity, JjRepo},
};

/// Clone a repo from another machine
///
/// The repo must have been added to the mesh with `jj-mesh add`. The
/// clone follows jj's `git.colocate` setting unless `--colocate` or
/// `--no-colocate` is given. Cloning a repo already on this machine adds a
/// workspace to it instead.
#[derive(Debug, Args)]
pub struct CloneArgs {
    /// Name or id of the repo in the mesh (asked when omitted)
    #[arg(add = ArgValueCandidates::new(complete::mesh_repos))]
    name: Option<String>,

    /// Directory to create the repo in (defaults to the repo name)
    ///
    /// The target directory must not exist before the clone. There is currently
    /// no way of adding a previous clone back into the mesh.
    #[arg(value_hint = ValueHint::DirPath)]
    path: Option<PathBuf>,

    /// Override the workspace name (defaults to this machine name)
    ///
    /// We assign a workspace for each copy of the repo across the mesh, so the
    /// current head of each machine is displayed in `jj log`.
    #[arg(long)]
    workspace: Option<String>,

    /// Colocate the repo with a `.git` usable by plain git tools
    #[arg(long, conflicts_with = "no_colocate")]
    colocate: bool,

    /// Keep the git store inside `.jj`, hidden from git tools
    #[arg(long)]
    no_colocate: bool,

    #[command(flatten)]
    settings: SettingsArgs,
}

/// Runs the `repo clone` command.
pub fn run(args: CloneArgs, dir: &ConfigDir) -> Result<()> {
    let state = MeshState::load(dir)?;
    let name = match args.name {
        Some(name) => name,
        None => pick(&state)?,
    };
    // The daemon has the last word on names: the state read here may lag
    // behind, not knowing the name yet.
    let id = match state.resolve_repo(&name) {
        Ok(id) => Some(id),
        Err(_) if !state.repo_names().contains(name.as_str()) => None,
        Err(err) => return Err(err),
    };
    let path = match args.path {
        Some(path) => path,
        // `name` may be an id.
        None => default_path(
            id.as_ref()
                .and_then(|id| state.repo_name(id))
                .unwrap_or(&name),
        )?,
    };
    let colocate: &[&str] = match (args.colocate, args.no_colocate) {
        (true, _) => &["--colocate"],
        (_, true) => &["--no-colocate"],
        _ => &[],
    };
    let settings = &args.settings;
    match id.as_ref().and_then(|id| state.repos.get(id)) {
        Some(repo) if repo.bare => {
            ensure!(
                colocate.is_empty(),
                "`{name}` is a bare repo kept by server mode, whose git store cannot be colocated",
            );
            let bare = JjRepo::at(&repo.path)?;
            let workspace = match args.workspace {
                Some(workspace) => workspace,
                None => bare_workspace_name(&bare, &state.machine.name, &path)?,
            };
            add_bare_workspace(dir, &name, &bare, &path, &workspace, settings)
        }
        Some(repo) => {
            let workspace = match args.workspace {
                Some(workspace) => workspace,
                None => extra_workspace_name(&state.machine.name, &path)?,
            };
            add_workspace(
                dir, &name, &repo.path, &path, &workspace, colocate, settings,
            )
        }
        None => {
            // Best-effort pre-check (the daemon re-validates
            // authoritatively before registering), so an obviously doomed
            // clone fails before anything is created on disk.
            let mut object_hash = ObjectHash::default();
            if let Some(id) = &id {
                let repo = Repo {
                    path: path.clone(),
                    bare: false,
                };
                state.check_clone(id, repo)?;
                if let Some((_, hash)) = state.mesh_repos.get(id).and_then(MeshRepo::present) {
                    object_hash = hash;
                }
            }
            let workspace = args.workspace.unwrap_or_else(|| state.machine.name.clone());
            clone(
                dir,
                &name,
                &path,
                &workspace,
                object_hash,
                colocate,
                settings,
            )
        }
    }
}

/// Clones the mesh repo `name` in `path`, with its git `object_hash`,
/// which the user's jj config must not override.
fn clone(
    dir: &ConfigDir,
    name: &str,
    path: &Path,
    workspace: &str,
    object_hash: ObjectHash,
    colocate: &[&str],
    settings: &SettingsArgs,
) -> Result<()> {
    // The daemon must be up: it is only contacted once the local repo
    // exists.
    control::ensure_daemon_blocking(dir)?;
    ensure!(
        !path.exists(),
        "{} already exists (clone creates the directory)",
        path.display(),
    );

    // Fresh repo with a machine-unique workspace name, using the user's
    // own jj (their config applies to the repo from the start).
    let object_hash = object_hash.to_string();
    let path_arg = path.to_string_lossy();
    jj(
        None,
        &[
            &["git", "init", "--object-hash", &object_hash],
            colocate,
            &["--", &path_arg],
        ]
        .concat(),
    )?;

    // The directory exists from here on: a failed pull removes it again so
    // a retry starts clean. Settings are written before the daemon
    // registers the repo.
    if let Err(err) = settings
        .write(path, Scope::Repo)
        .and_then(|()| pull(dir, name, path, workspace))
    {
        return Err(clean_failed_clone(dir, name, path, err));
    }

    // Any jj command merges the fresh workspace into the pulled history;
    // doing it here leaves the repo ready to use. A re-clone after
    // `repo forget` finds the previous instance's same-name workspace in
    // the pulled history, which leaves the fresh working copy stale:
    // recover it rather than fail a clone that already succeeded, and keep
    // its recovered position instead of jumping to trunk.
    match jj(Some(path), &["status"]) {
        Ok(_) => start_on_trunk(path),
        Err(err) => {
            if jj(Some(path), &["workspace", "update-stale"]).is_err() {
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

/// Adds a workspace of `name`, registered here at `repo`, in `path`.
fn add_workspace(
    dir: &ConfigDir,
    name: &str,
    repo: &Path,
    path: &Path,
    workspace: &str,
    colocate: &[&str],
    settings: &SettingsArgs,
) -> Result<()> {
    ensure!(
        !path.exists(),
        "{} already exists and `{name}` is already cloned in {}. Pass the directory of a new workspace",
        path.display(),
        repo.display(),
    );
    // The daemon would leave a workspace with an invalid name unsynced.
    validate_name("workspace", workspace)?;

    // jj runs from the repo, resolving relative paths from there.
    let path =
        std::path::absolute(path).wrap_err_with(|| format!("cannot resolve {}", path.display()))?;
    let path_arg = path.to_string_lossy();
    create_workspace(dir, name, workspace, &path, settings, || {
        jj(
            Some(repo),
            &[
                &["workspace", "add", "--name", workspace],
                colocate,
                &["--", &path_arg],
            ]
            .concat(),
        )
        .map(drop)
    })?;

    println!(
        "{}",
        ui::good(format_args!(
            "`{name}` is already cloned in {}, added workspace `{workspace}` in {}",
            repo.display(),
            path.display(),
        )),
    );
    Ok(())
}

/// Adds a workspace of the bare repo of `name` in `path`, then starts its
/// working copy on trunk.
fn add_bare_workspace(
    dir: &ConfigDir,
    name: &str,
    bare: &JjRepo,
    path: &Path,
    workspace: &str,
    settings: &SettingsArgs,
) -> Result<()> {
    ensure!(
        !path.exists(),
        "{} already exists (clone creates the directory)",
        path.display(),
    );
    validate_name("workspace", workspace)?;

    // jj_lib records the identity the user's jj config holds.
    let config = |key| {
        jj(None, &["config", "get", key])
            .map_or_else(|_| String::new(), |value| value.trim().to_owned())
    };
    let user = Identity {
        name: config("user.name"),
        email: config("user.email"),
    };
    if bare.has_workspaces()? {
        create_workspace(dir, name, workspace, path, settings, || {
            bare.add_workspace(path, workspace, &user)
        })?;
    } else {
        // The first workspace: the settings go to the repo config, where
        // the next ones also find them.
        settings.write(bare.root(), Scope::Repo)?;
        bare.add_workspace(path, workspace, &user)?;
    }
    start_on_trunk(path);

    println!(
        "{}",
        ui::good(format_args!(
            "Added workspace `{workspace}` of the bare repo of `{name}` in {}",
            path.display(),
        )),
    );
    Ok(())
}

/// Runs `create`, which adds the workspace `workspace` of the registered
/// repo `repo` at `root`, then writes `settings` to its workspace config.
/// The daemon holds the workspace meanwhile, so it never starts without
/// them.
fn create_workspace(
    dir: &ConfigDir,
    repo: &str,
    workspace: &str,
    root: &Path,
    settings: &SettingsArgs,
    create: impl FnOnce() -> Result<()>,
) -> Result<()> {
    if settings.is_empty() {
        return create();
    }
    let hold = WorkspaceHold::take(dir, repo, workspace)?;
    create()?;
    settings
        .write(root, Scope::Workspace)
        .wrap_err("the workspace is created, but its settings could not be written")?;
    if !hold.is_held() {
        eprintln!(
            "{} the daemon restarted while the workspace was created and may run it with \
             other settings. Restart it with `jj-mesh service restart`",
            ui::warn("warning:").for_stderr(),
        );
    }
    Ok(())
}

/// The default name of a workspace of a bare repo: the machine name, or
/// the [`extra_workspace_name`] once a workspace holds it.
fn bare_workspace_name(bare: &JjRepo, machine: &str, path: &Path) -> Result<String> {
    let repo = bare.open()?;
    let heads = repo.op_heads().block_on()?;
    if repo.workspace_names(&heads).block_on()?.contains(machine) {
        extra_workspace_name(machine, path)
    } else {
        Ok(machine.to_owned())
    }
}

/// The default name of another workspace of a local repo: the machine name
/// and the workspace directory name.
fn extra_workspace_name(machine: &str, path: &Path) -> Result<String> {
    let dir_name = path
        .file_name()
        .ok_or_eyre("cannot derive a workspace name, use --workspace")?;
    Ok(format!("{machine}-{}", dir_name.to_string_lossy()))
}

/// Asks which of the mesh repos to clone.
fn pick(state: &MeshState) -> Result<String> {
    let available: Vec<&str> = state.repo_names().into_iter().collect();
    ensure!(
        !available.is_empty(),
        "no repo to clone. Add one with `jj-mesh add` on another machine",
    );
    let name = ui::select("Repo to clone", &available)?
        .ok_or_eyre("the name of the repo to clone is required")?;
    Ok((*name).to_owned())
}

/// The directory named after the repo. Names come from peers, so one that
/// is not a plain directory name (`..`, separators) must not pick where the
/// clone is written.
fn default_path(name: &str) -> Result<PathBuf> {
    let path = PathBuf::from(name);
    ensure!(
        matches!(
            path.components().collect::<Vec<_>>()[..],
            [Component::Normal(_)]
        ),
        "`{name}` is not a directory name. Pass the directory to clone into",
    );
    Ok(path)
}

/// Names the workspace, then asks the daemon to pull and register the
/// repo, streaming its progress. On failure the repo stays unregistered,
/// except when the response of a successful pull was lost (see
/// [`clean_failed_clone`]).
fn pull(dir: &ConfigDir, name: &str, path: &Path, workspace: &str) -> Result<()> {
    jj(Some(path), &["workspace", "rename", workspace])?;

    println!("Pulling repo `{name}` from the mesh...");
    let request = Request::CloneRepo {
        name: name.to_owned(),
        path: std::fs::canonicalize(path)
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
    match pulled? {
        Response::Cloned { .. } => Ok(()),
        response => bail!("unexpected response from the daemon: {response:?}"),
    }
}

/// Removes the directory a failed clone created. The daemon registers the
/// repo only once the pull succeeded, so the directory is normally
/// unregistered; when the state lists it anyway (the pull succeeded but its
/// response was lost), it holds a valid clone and is kept.
fn clean_failed_clone(dir: &ConfigDir, name: &str, path: &Path, err: Report) -> Report {
    let registered = MeshState::load(dir).is_ok_and(|state| {
        let path = std::fs::canonicalize(path);
        path.is_ok_and(|path| state.repos.values().any(|repo| repo.path == path))
    });
    if registered {
        return err.wrap_err(format!(
            "the pull succeeded and `{name}` is registered at {}, but its result was lost",
            path.display(),
        ));
    }
    match std::fs::remove_dir_all(path) {
        Ok(()) => err,
        Err(remove_err) => err.wrap_err(format!(
            "cannot clean up the repo directory {} ({remove_err}), remove it before retrying",
            path.display(),
        )),
    }
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
        TransferPhase::Index => {
            spin("Indexing commits...".to_owned());
        }
    }
}

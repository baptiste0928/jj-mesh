//! Server mode: a bare repo of every mesh repo.
//!
//! A server registers every mesh repo it does not hold as a bare repo (see
//! [`JjRepo::init_bare`]) in [`ConfigDir::bare_dir`], which then syncs
//! like any other repo. `jj-mesh clone` adds workspaces to a bare repo
//! instead of cloning it again.
//!
//! One task reconciles the bare repos with the mesh state, on every state
//! change, periodically, and on orphan announcements in server mode:
//!
//! ```text
//! state change ────┐   ┌─► unregister the bare repos that are gone, and
//! CHECK_INTERVAL ──┼─► │   those no workspace uses once server mode is off
//! orphan announce ─┘   ├─► delete unregistered bare repos no workspace uses
//!                      └─► server on: pull the mesh repos not held here
//! ```
//!
//! Bare repos are pulled into `<id>.tmp` and renamed to `<id>` once
//! complete, so an `<id>` directory always holds a complete repo. Only
//! directories named after repo ids in the bare repo directory are deleted,
//! and only when no workspace uses them (see [`JjRepo::has_workspaces`]).
//!
//! A repo registered with its own storage can move into a bare repo
//! instead of being pulled again, its workspaces then using the bare repo
//! (see [`BareRepos::store`]).

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, ensure};
use tokio::sync::{Mutex, Notify};
use tracing::{debug, info, warn};

use super::{
    backoff::Backoff,
    hub::{CloneSource, SyncHub},
    sleep_until,
    store::MeshStore,
};
use crate::{
    config::{ConfigDir, MeshRepo, MeshState, ObjectHash, Repo, RepoId, create_private},
    repo::{JjRepo, RelocationError, run_jj},
};

/// How often to reconcile without a wakeup: workspaces come and go without
/// any mesh state change.
const CHECK_INTERVAL: Duration = Duration::from_mins(5);

/// Delay before pulling again a repo whose pull failed; doubles up to
/// [`RETRY_MAX`].
const RETRY_MIN: Duration = Duration::from_secs(30);

/// Ceiling of the pull retry delay.
const RETRY_MAX: Duration = Duration::from_mins(30);

/// Budget for making a repo non-colocated before moving it: jj moves the
/// git store with a rename.
const COLOCATION_TIMEOUT: Duration = Duration::from_secs(5);

/// The bare repo directory, and the wakeups of the bare repo task.
#[derive(Debug)]
pub struct BareRepos {
    root: PathBuf,
    changed: Notify,
    /// Held while dropping and deleting bare repos, and while moving one in.
    cleanup: Mutex<()>,
}

impl BareRepos {
    pub fn new(dir: &ConfigDir) -> Self {
        BareRepos {
            root: dir.bare_dir().to_owned(),
            changed: Notify::new(),
            cleanup: Mutex::new(()),
        }
    }

    /// Notes a mesh state change; the task reconciles with it.
    pub fn sync(&self) {
        self.changed.notify_one();
    }

    /// Moves the storage of the registered repo `id` into a bare repo,
    /// returning its path (see [`Relocation`]). Holds the cleanup lock so
    /// the task does not drop the bare repo before it is complete.
    pub async fn store(&self, store: &MeshStore, id: &RepoId) -> Result<PathBuf> {
        let _cleanup = self.cleanup.lock().await;
        let state = store.snapshot();
        state.check_store(id)?;
        let repo = state.repos[id].clone();
        let (root, path, dir_name) = (self.root.clone(), repo.path.clone(), id.to_string());
        let (relocation, target) = crate::spawn_blocking(move || -> Result<_> {
            create_private(&root)?;
            let target = fs::canonicalize(&root)
                .context("cannot resolve the bare repos")?
                .join(dir_name);
            Ok((JjRepo::at(&path)?.relocation(&target)?, target))
        })
        .await
        .context("repo move task failed")??;

        store.update(|state| {
            ensure!(
                state.repos.get(id) == Some(&repo),
                "the repo changed meanwhile"
            );
            state.store_repo(id, target.clone())
        })?;
        let disabled = if relocation.colocated() {
            let args = ["git", "colocation", "disable"];
            run_jj(&repo.path, &args, COLOCATION_TIMEOUT).await
        } else {
            Ok(())
        };
        let moved = match disabled {
            Ok(()) => crate::spawn_blocking(move || relocation.apply())
                .await
                .unwrap_or_else(|err| Err(RelocationError::Unmoved(err.into()))),
            Err(err) => Err(RelocationError::Unmoved(err)),
        };
        match moved {
            Ok(()) => Ok(target),
            Err(RelocationError::Moved(err)) => Err(err),
            Err(RelocationError::Unmoved(err)) => {
                restore(store, id, repo);
                Err(err)
            }
        }
    }
}

/// Registers `repo` again after its storage failed to move.
fn restore(store: &MeshStore, id: &RepoId, repo: Repo) {
    let restored = store.update(|state| {
        state.repos.insert(id.clone(), repo);
        Ok(())
    });
    if let Err(err) = restored {
        warn!("cannot register the repo again: {err:#}");
    }
}

/// Reconciles the bare repos forever (see the module docs).
pub async fn run(store: Arc<MeshStore>, hub: Arc<SyncHub>, bare: Arc<BareRepos>) {
    let mut task = Task {
        bare: bare.clone(),
        store,
        hub,
        retries: BTreeMap::new(),
    };
    loop {
        let server = task.reconcile().await;
        // Announcements only matter to pulls: with server mode off, they
        // would reopen the remaining bare repos on every peer change.
        let orphans = async {
            if server {
                task.hub.orphans_changed().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            () = bare.changed.notified() => {}
            () = orphans => {}
            () = tokio::time::sleep(CHECK_INTERVAL) => {}
            () = sleep_until(task.next_retry()) => {}
        }
    }
}

struct Task {
    bare: Arc<BareRepos>,
    store: Arc<MeshStore>,
    hub: Arc<SyncHub>,
    /// Repos whose pull failed, with their backoff and next attempt.
    retries: BTreeMap<RepoId, (Backoff, Instant)>,
}

impl Task {
    /// Runs one reconciliation, returning whether server mode is on.
    async fn reconcile(&mut self) -> bool {
        {
            let _cleanup = self.bare.cleanup.lock().await;
            self.drop_bare().await;
            if let Err(err) = self.sweep().await {
                warn!("cannot clean up the bare repos: {err:#}");
            }
        }

        let mut state = self.store.snapshot();
        let server = state.machine.server;
        let clonable: BTreeSet<RepoId> = state.clonable_repos().map(|(id, _)| id.clone()).collect();
        self.retries.retain(|id, _| server && clonable.contains(id));
        for id in clonable {
            if !state.machine.server {
                break;
            }
            // A pull may take long: a fresh state notices server mode going
            // off and repos registered or removed meanwhile.
            if self.pull(&state, &id).await {
                state = self.store.snapshot();
            }
        }
        server
    }

    /// The next pull retry, if any is pending.
    fn next_retry(&self) -> Option<Instant> {
        let now = Instant::now();
        self.retries
            .values()
            .map(|(_, at)| *at)
            .filter(|at| *at > now)
            .min()
    }

    /// Unregisters the bare repos whose directory is gone, to pull them
    /// again, and those no workspace uses once server mode is off.
    async fn drop_bare(&self) {
        let state = self.store.snapshot();
        let mut dropped = Vec::new();
        for (id, repo) in state.repos.iter().filter(|(_, repo)| repo.bare) {
            let reason = if JjRepo::is_missing(&repo.path) {
                "its directory is gone"
            } else if !state.machine.server && !is_used(&repo.path).await {
                "server mode is off"
            } else {
                continue;
            };
            let name = state.repo_name(id).unwrap_or("?");
            info!(repo = %name, "dropping bare repo: {reason}");
            dropped.push(id);
        }
        if dropped.is_empty() {
            return;
        }
        if let Err(err) = self.store.update(|state| {
            for id in dropped {
                state.drop_bare(id);
            }
            Ok(())
        }) {
            warn!("cannot drop bare repos: {err:#}");
        }
    }

    /// Deletes the bare repos that are not registered and no workspace
    /// uses, and the leftovers of interrupted pulls.
    async fn sweep(&self) -> Result<()> {
        let entries = match fs::read_dir(&self.bare.root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err).context("cannot list the bare repos"),
        };
        // Registered paths are canonical.
        let root = fs::canonicalize(&self.bare.root).context("cannot resolve the bare repos")?;
        let state = self.store.snapshot();
        let registered: BTreeSet<&Path> = state.repos.values().map(|repo| &*repo.path).collect();
        for entry in entries {
            let path = entry.context("cannot list the bare repos")?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            // Pulls run on this task, so no `.tmp` is in progress.
            let (id, partial) = match name.strip_suffix(".tmp") {
                Some(id) => (id, true),
                None => (name, false),
            };
            if RepoId::try_from(id.to_owned()).is_err() {
                continue;
            }
            if !partial
                && (registered.contains(&*root.join(name))
                    || !JjRepo::is_missing(&path) && is_used(&path).await)
            {
                continue;
            }
            info!(%id, "deleting unused bare repo");
            if let Err(err) = remove(path).await {
                warn!("{err:#}");
            }
        }
        Ok(())
    }

    /// Pulls the mesh repo `id` into a new bare repo and registers it,
    /// unless a pull failed recently or no peer announces it. Returns
    /// whether it pulled.
    async fn pull(&mut self, state: &MeshState, id: &RepoId) -> bool {
        if self
            .retries
            .get(id)
            .is_some_and(|(_, at)| *at > Instant::now())
        {
            return false;
        }
        let Some((name, object_hash)) = state.mesh_repos.get(id).and_then(MeshRepo::present) else {
            return false;
        };
        let target = self.hub.clone_sources(id, name).and_then(|sources| {
            let repo = Repo {
                path: self.bare.root.join(id.to_string()),
                bare: true,
            };
            Ok((state.check_clone(id, repo)?, sources))
        });
        let (repo, sources) = match target {
            Ok(target) => target,
            Err(err) => {
                debug!(repo = %name, "cannot pull the repo: {err:#}");
                return false;
            }
        };

        info!(repo = %name, "pulling bare repo");
        match self.pull_into(id, name, repo, object_hash, sources).await {
            Ok(()) => {
                self.retries.remove(id);
            }
            Err(err) => {
                warn!(repo = %name, "bare repo pull failed: {err:#}");
                let (backoff, at) = self
                    .retries
                    .entry(id.clone())
                    .or_insert_with(|| (Backoff::new(RETRY_MIN, RETRY_MAX), Instant::now()));
                *at = Instant::now() + backoff.next_delay();
            }
        }
        true
    }

    /// Pulls into `<id>.tmp`, then moves the complete repo to `<id>` and
    /// registers it while server mode is on. A failure leaves directories
    /// the next sweep deletes.
    async fn pull_into(
        &self,
        id: &RepoId,
        name: &str,
        repo: Repo,
        object_hash: ObjectHash,
        sources: Vec<CloneSource>,
    ) -> Result<()> {
        let partial = self.bare.root.join(format!("{id}.tmp"));
        {
            let (root, partial) = (self.bare.root.clone(), partial.clone());
            crate::spawn_blocking(move || -> Result<()> {
                create_private(&root)?;
                JjRepo::init_bare(&partial, object_hash)?;
                Ok(())
            })
            .await
            .context("bare repo init task failed")??;
        }

        let outcome = self.hub.pull(id, sources, &partial, &|_, _| {}).await?;

        fs::rename(&partial, &repo.path)
            .with_context(|| format!("cannot move the repo to {}", repo.path.display()))?;
        self.store.update(|state| {
            ensure!(state.machine.server, "server mode is off");
            state.add_clone(id.clone(), repo)
        })?;
        info!(repo = %name, ops = outcome.ops, objects = outcome.git_objects, "pulled bare repo");
        Ok(())
    }
}

/// Whether workspaces use the bare repo at `path`. One whose workspaces
/// cannot be read counts as used, so it is never deleted.
async fn is_used(path: &Path) -> bool {
    let path = path.to_owned();
    crate::spawn_blocking(move || JjRepo::at(&path)?.has_workspaces())
        .await
        .context("workspace check task failed")
        .and_then(|used| used)
        .unwrap_or_else(|err| {
            debug!("cannot check the bare repo workspaces: {err:#}");
            true
        })
}

/// Deletes a bare repo directory, on a blocking thread.
async fn remove(path: PathBuf) -> Result<()> {
    crate::spawn_blocking(move || {
        fs::remove_dir_all(&path).with_context(|| format!("cannot delete {}", path.display()))
    })
    .await
    .context("bare repo deletion task failed")?
}

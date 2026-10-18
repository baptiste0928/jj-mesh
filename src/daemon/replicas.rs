//! Server mode: a bare replica of every mesh repo.
//!
//! A server registers every mesh repo it does not hold as a bare repo (see
//! [`JjRepo::init_bare`]) in [`ConfigDir::replicas_dir`], which then syncs
//! like any other repo. `jj-mesh clone` adds workspaces to a replica
//! instead of cloning it again.
//!
//! One task reconciles the replicas with the mesh state, on every state
//! change, periodically, and on orphan announcements in server mode:
//!
//! ```text
//! state change ────┐   ┌─► unregister the replicas that are gone, and those
//! CHECK_INTERVAL ──┼─► │   no workspace uses once server mode is off
//! orphan announce ─┘   ├─► delete unregistered replicas no workspace uses
//!                      └─► server on: pull the mesh repos not held here
//! ```
//!
//! Replicas are pulled into `<id>.tmp` and renamed to `<id>` once complete,
//! so an `<id>` directory always holds a complete repo. Deleting replicas
//! is the only place jj-mesh removes repo data: only directories named
//! after repo ids, in the replicas directory, that no workspace uses (see
//! [`JjRepo::has_workspaces`]).

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use color_eyre::eyre::{Result, WrapErr as _};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use super::{
    backoff::Backoff,
    hub::{CloneSource, SyncHub},
    sleep_until,
    store::MeshStore,
};
use crate::{
    config::{ConfigDir, MeshRepo, MeshState, ObjectHash, Repo, RepoId, create_private},
    repo::JjRepo,
};

/// How often to reconcile without a wakeup: workspaces come and go without
/// any mesh state change.
const CHECK_INTERVAL: Duration = Duration::from_mins(5);

/// Delay before pulling again a replica whose pull failed; doubles up to
/// [`RETRY_MAX`].
const RETRY_MIN: Duration = Duration::from_secs(30);

/// Ceiling of the pull retry delay.
const RETRY_MAX: Duration = Duration::from_mins(30);

/// Wakes the replica task on mesh state changes.
#[derive(Debug, Default)]
pub struct Replicas {
    changed: Notify,
}

impl Replicas {
    /// Notes a mesh state change; the task reconciles with it.
    pub fn sync(&self) {
        self.changed.notify_one();
    }
}

/// Reconciles the replicas forever (see the module docs).
pub async fn run(
    dir: ConfigDir,
    store: Arc<MeshStore>,
    hub: Arc<SyncHub>,
    replicas: Arc<Replicas>,
) {
    let mut task = Task {
        root: dir.replicas_dir().to_owned(),
        store,
        hub,
        retries: BTreeMap::new(),
    };
    loop {
        let server = task.reconcile().await;
        let now = Instant::now();
        let retry = task
            .retries
            .values()
            .map(|(_, at)| *at)
            .filter(|at| *at > now)
            .min();
        // Announcements only matter to pulls: with server mode off, they
        // would reopen the remaining replicas on every peer change.
        let orphans = async {
            if server {
                task.hub.orphans_changed().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            () = replicas.changed.notified() => {}
            () = orphans => {}
            () = tokio::time::sleep(CHECK_INTERVAL) => {}
            () = sleep_until(retry) => {}
        }
    }
}

struct Task {
    /// The replicas directory.
    root: PathBuf,
    store: Arc<MeshStore>,
    hub: Arc<SyncHub>,
    /// Repos whose pull failed, with their backoff and next attempt.
    retries: BTreeMap<String, (Backoff, Instant)>,
}

impl Task {
    /// Runs one reconciliation, returning whether server mode is on.
    async fn reconcile(&mut self) -> bool {
        self.drop_replicas(&self.store.snapshot()).await;

        let state = self.store.snapshot();
        if let Err(err) = self.sweep(&state).await {
            warn!("cannot clean up the replicas: {err:#}");
        }

        let server = state.machine.server;
        let clonable: BTreeSet<&str> = state.clonable_repo_names().collect();
        self.retries
            .retain(|name, _| server && clonable.contains(name.as_str()));
        if server {
            for name in clonable {
                self.replicate(&state, name).await;
            }
        }
        server
    }

    /// Unregisters the replicas whose directory is gone, to pull them
    /// again, and those no workspace uses once server mode is off.
    async fn drop_replicas(&self, state: &MeshState) {
        for (name, repo) in state.repos.iter().filter(|(_, repo)| repo.bare) {
            let reason = match use_of(&repo.path).await {
                Use::Gone => "its directory is gone",
                Use::Unused if !state.machine.server => "server mode is off",
                Use::Unused | Use::Used => continue,
            };
            info!(repo = %name, "dropping replica: {reason}");
            if let Err(err) = self.store.update(|state| {
                state.drop_replica(name);
                Ok(())
            }) {
                warn!(repo = %name, "cannot drop replica: {err:#}");
            }
        }
    }

    /// Deletes the replicas that are not registered and no workspace uses,
    /// and the leftovers of interrupted pulls.
    async fn sweep(&self, state: &MeshState) -> Result<()> {
        let entries = match fs::read_dir(&self.root) {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err).wrap_err("cannot list the replicas"),
        };
        let registered: BTreeSet<&RepoId> = state.repos.values().map(|repo| &repo.id).collect();
        for entry in entries {
            let path = entry.wrap_err("cannot list the replicas")?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let (id, partial) = match name.strip_suffix(".tmp") {
                Some(id) => (id, true),
                None => (name, false),
            };
            let Ok(id) = RepoId::try_from(id.to_owned()) else {
                continue;
            };
            if registered.contains(&id) || (!partial && use_of(&path).await == Use::Used) {
                continue;
            }
            info!(%id, "deleting unused replica");
            remove(path).await?;
        }
        Ok(())
    }

    /// Pulls the mesh repo `name` into a new replica and registers it,
    /// unless a pull failed recently or no peer announces it under its
    /// mesh id.
    async fn replicate(&mut self, state: &MeshState, name: &str) {
        if self
            .retries
            .get(name)
            .is_some_and(|(_, at)| *at > Instant::now())
        {
            return;
        }
        let Ok((id, sources)) = self.hub.clone_sources(name) else {
            return;
        };
        let Some((mesh_id, object_hash)) = state.mesh_repos.get(name).and_then(MeshRepo::present)
        else {
            return;
        };
        if *mesh_id != id {
            return debug!(repo = %name, "peers announce another repo under this name");
        }

        info!(repo = %name, "replicating repo");
        match self.pull(name, &id, object_hash, sources).await {
            Ok(()) => {
                self.retries.remove(name);
            }
            Err(err) => {
                warn!(repo = %name, "replication failed: {err:#}");
                let (backoff, at) = self
                    .retries
                    .entry(name.to_owned())
                    .or_insert_with(|| (Backoff::new(RETRY_MIN, RETRY_MAX), Instant::now()));
                *at = Instant::now() + backoff.next_delay();
            }
        }
    }

    /// Pulls into `<id>.tmp`, then moves the complete repo to `<id>` and
    /// registers it. A failure leaves directories the next sweep deletes.
    async fn pull(
        &self,
        name: &str,
        id: &RepoId,
        object_hash: ObjectHash,
        sources: Vec<CloneSource>,
    ) -> Result<()> {
        let path = self.root.join(id.to_string());
        let partial = self.root.join(format!("{id}.tmp"));
        {
            let (root, partial) = (self.root.clone(), partial.clone());
            crate::spawn_blocking(move || -> Result<()> {
                create_private(&root)?;
                JjRepo::init_bare(&partial, object_hash)?;
                Ok(())
            })
            .await
            .wrap_err("replica init task failed")??;
        }

        let (ops, git_objects) = self
            .hub
            .pull(name, id, sources, &partial, &|_, _| {})
            .await?;

        fs::rename(&partial, &path)
            .wrap_err_with(|| format!("cannot move the replica to {}", path.display()))?;
        self.store.update(|state| {
            state.add_clone(
                name.to_owned(),
                Repo {
                    id: id.clone(),
                    path: path.clone(),
                    bare: true,
                },
            )
        })?;
        info!(repo = %name, ops, objects = git_objects, "replicated repo");
        Ok(())
    }
}

/// Whether workspaces use a replica.
#[derive(Debug, PartialEq, Eq)]
enum Use {
    Used,
    Unused,
    /// Missing, or not a complete repo.
    Gone,
}

/// How workspaces use the replica at `path`. A replica whose workspaces
/// cannot be read counts as used, so it is never deleted.
async fn use_of(path: &Path) -> Use {
    let path = path.to_owned();
    let found =
        crate::spawn_blocking(move || JjRepo::at(&path).map(|repo| repo.has_workspaces())).await;
    match found {
        Ok(Err(_)) => Use::Gone,
        Ok(Ok(Ok(false))) => Use::Unused,
        Ok(Ok(Ok(true))) => Use::Used,
        Ok(Ok(Err(err))) => {
            debug!("cannot check the replica workspaces: {err:#}");
            Use::Used
        }
        Err(err) => {
            debug!("workspace check task failed: {err}");
            Use::Used
        }
    }
}

/// Deletes a replica directory, on a blocking thread.
async fn remove(path: PathBuf) -> Result<()> {
    crate::spawn_blocking(move || {
        fs::remove_dir_all(&path).wrap_err_with(|| format!("cannot delete {}", path.display()))
    })
    .await
    .wrap_err("replica deletion task failed")?
}

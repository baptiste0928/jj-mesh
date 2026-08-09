//! Registered repo management.
//!
//! One watch task per registered repo syncs it (see the `task` submodule);
//! [`RepoSet`] keeps the tasks aligned with the mesh state, spawning and
//! aborting them as repos are registered and removed.

mod task;
#[cfg(test)]
mod tests;

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime},
};

use tokio::sync::Notify;
use tracing::info;

use self::task::spawn_repo;
use super::{control, hub::SyncHub};
use crate::{
    config::{MeshState, RepoId, Settings},
    net::sync::{RepoHealth, RepoHealthState},
};

/// The set of managed repos, synced from the mesh state and keyed by their
/// mesh-wide name.
#[derive(Debug)]
pub struct RepoSet {
    hub: Arc<SyncHub>,
    repos: Mutex<BTreeMap<String, RepoHandle>>,
    /// Pinged on every repo state change, driving the status broadcast.
    changed: Arc<Notify>,
    /// Daemon settings, loaded once at start and shared with every repo
    /// task.
    settings: Arc<Settings>,
}

/// Book-keeping for one repo task.
#[derive(Debug)]
struct RepoHandle {
    id: RepoId,
    path: PathBuf,
    state: Arc<Mutex<RepoState>>,
    task: tokio::task::JoinHandle<()>,
}

/// Live state of one repo watch, shared between its task and status
/// snapshots.
#[derive(Debug)]
enum RepoState {
    Opening,
    Watching {
        op_heads: usize,
        last_change: Option<SystemTime>,
        last_sync: Option<SystemTime>,
    },
    /// Rebuilding the commit index for op heads that lack one; jj commands
    /// in the repo would otherwise pay for the rebuild themselves.
    Indexing,
    Backoff {
        until: Instant,
        error: String,
    },
    /// The repo directory itself is gone: an unmounted disk, or a repo the
    /// user deleted without `jj-mesh repo forget`. Retried like a backoff, but
    /// surfaced distinctly so the status can suggest the fix.
    Missing {
        until: Instant,
    },
}

impl RepoSet {
    pub fn new(hub: Arc<SyncHub>, settings: Arc<Settings>) -> Self {
        RepoSet {
            hub,
            repos: Mutex::new(BTreeMap::new()),
            changed: Arc::new(Notify::new()),
            settings,
        }
    }

    /// Resolves when any repo's state may have changed since the last
    /// call. Wakeups coalesce (this is a [`Notify`]): consumers snapshot
    /// [`Self::statuses`] on every wake, so a missed ping only delays,
    /// never loses, state.
    pub async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Aligns the managed repos with the mesh state: spawns tasks for new
    /// repos and shuts down removed ones. A repo whose path or id changed
    /// is respawned.
    pub fn sync(&self, state: &MeshState) {
        let mut repos = self.repos.lock().unwrap();

        repos.retain(|name, handle| {
            let keep = state
                .repos
                .get(name)
                .is_some_and(|repo| handle.path == repo.path && handle.id == repo.id);
            if !keep {
                info!(repo = %name, "removing repo watch");
                handle.task.abort();
                self.hub.unregister_repo(name);
            }
            keep
        });

        for (name, repo) in &state.repos {
            repos.entry(name.clone()).or_insert_with(|| {
                info!(repo = %name, path = %repo.path.display(), "managing repo");
                let announcements = self.hub.register_repo(name.clone(), repo.id.clone());
                spawn_repo(
                    repo.id.clone(),
                    name.clone(),
                    repo.path.clone(),
                    self.hub.clone(),
                    announcements,
                    self.changed.clone(),
                    self.settings.clone(),
                )
            });
        }
        self.changed.notify_one();
    }

    /// Condenses every repo's state into the health report peers see.
    /// Local detail (paths, error messages) deliberately stays out: error
    /// strings embed filesystem paths, which never leave this machine.
    pub fn health(&self) -> Vec<RepoHealth> {
        let paused = self.hub.paused_repos();
        let repos = self.repos.lock().unwrap();
        repos
            .iter()
            .map(|(name, handle)| {
                let state = if paused.contains_key(name) {
                    RepoHealthState::Paused
                } else {
                    match &*handle.state.lock().unwrap() {
                        // Opening and indexing are transitions, not faults.
                        RepoState::Opening | RepoState::Watching { .. } | RepoState::Indexing => {
                            RepoHealthState::Ok
                        }
                        RepoState::Backoff { .. } => RepoHealthState::Failed,
                        RepoState::Missing { .. } => RepoHealthState::Missing,
                    }
                };
                RepoHealth {
                    name: name.clone(),
                    state,
                }
            })
            .collect()
    }

    /// Snapshots the state of every repo for the control socket.
    pub fn statuses(&self) -> Vec<control::RepoStatus> {
        let repos = self.repos.lock().unwrap();

        repos
            .iter()
            .map(|(name, handle)| {
                let watch = match &*handle.state.lock().unwrap() {
                    RepoState::Opening => control::WatchStatus::Opening,
                    RepoState::Indexing => control::WatchStatus::Indexing,
                    RepoState::Watching {
                        op_heads,
                        last_change,
                        last_sync,
                    } => control::WatchStatus::Watching {
                        op_heads: *op_heads as u64,
                        last_change_secs: last_change
                            .map(|at| at.elapsed().unwrap_or_default().as_secs()),
                        last_sync_secs: last_sync
                            .map(|at| at.elapsed().unwrap_or_default().as_secs()),
                    },
                    RepoState::Backoff { until, error } => control::WatchStatus::Failed {
                        error: error.clone(),
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                    RepoState::Missing { until } => control::WatchStatus::Missing {
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                };

                control::RepoStatus {
                    name: name.clone(),
                    path: handle.path.clone(),
                    watch,
                }
            })
            .collect()
    }
}

//! Registered repo management.
//!
//! One task per registered repo opens it and watches its op-heads directory:
//! every mutating jj command atomically swaps head marker files there, so a
//! change event means new operations to announce. The task publishes its
//! head set through the sync hub (on change and on watch start) and fetches
//! operations peers announce; serving peer fetches is dispatched by the hub
//! directly (never through this task's loop, which may itself be fetching).
//!
//! Change detection compares the head set against the last one seen, which
//! also absorbs event bursts and spurious wakeups; the task's own head
//! writes (applying fetched operations) fold into that baseline before the
//! comparison, so self-triggered events are suppressed the same way.
//!
//! When auto-snapshotting is enabled, the task also watches the working
//! copy files: the first edit arms a snapshot one interval later (never
//! immediately), and edits during the wait do not postpone it, so
//! continuous editing snapshots at the configured cadence. The snapshot
//! runs through the jj binary and produces a regular operation, which the
//! op-heads watch then picks up and announces like any local change.
//!
//! Syncing operations from peers can leave the local working copy stale
//! (updated by an operation the working copy never saw). When enabled,
//! `jj workspace update-stale` runs after every sync that applied
//! operations, and once on watch start for staleness accrued while the
//! daemon was down, but only while the op head is single. Any jj
//! command reconciles divergent op heads by writing a merge operation,
//! so daemons doing this on both ends of a divergence would ping-pong
//! fresh merge operations at each other. Divergence is left to the next
//! actual jj activity (a user command, an auto-snapshot), whose merge
//! then arrives here as a single head.

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
                        // Opening is a moment, not a health state.
                        RepoState::Opening | RepoState::Watching { .. } => RepoHealthState::Ok,
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

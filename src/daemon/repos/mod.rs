//! Registered repo management.
//!
//! [`RepoSet`] keeps one watch task per registered repo (see the `task`
//! submodule), spawning and aborting them as repos are registered and
//! removed. Each repo task runs one task per local workspace, keeping its
//! working copy fresh (see the `workspace` submodule) with the settings
//! it reads from jj config (see the `settings` submodule).
//!
//! Repo tasks decide which workspaces this machine claims (see
//! [`crate::config::WorkspaceClaims`]), which round-trip through the store:
//!
//! ```text
//! RepoSet::sync ──RepoClaims──► repo task ──► workspace tasks
//!       ▲                           │
//!       └───── store ◄─ClaimUpdate──┘
//! ```

mod settings;
mod task;
#[cfg(test)]
mod tests;
mod workspace;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime},
};

use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, mpsc, watch};
use tracing::debug;

use self::{settings::Settings, task::spawn_repo};
use super::{control, hub::SyncHub};
use crate::{
    config::{MeshState, RepoClaims, RepoId},
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
    /// Settings for every workspace instead of their jj config (tests).
    settings: Option<Settings>,
    /// Where repo tasks send the claims to persist.
    claim: mpsc::UnboundedSender<ClaimUpdate>,
}

/// Whether this machine keeps a local workspace fresh (see the `task`
/// module docs for the claim rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceState {
    /// Claimed here: auto-snapshot and update-stale run.
    Claimed,
    /// Claimed here and by these peers: left alone everywhere.
    Contested { machines: Vec<String> },
    /// Claimed by these peers only: a directory of theirs, left alone.
    Foreign { machines: Vec<String> },
    /// Stale when found: claimed once its working copy is updated.
    Stale,
    /// Not claimable: its name is not valid in the mesh, or the machine
    /// reached its claim cap.
    Unclaimable,
}

/// The workspaces of a repo this machine claims, to persist.
#[derive(Debug)]
pub struct ClaimUpdate {
    pub repo: RepoId,
    pub names: BTreeSet<String>,
}

/// Book-keeping for one repo task.
#[derive(Debug)]
struct RepoHandle {
    id: RepoId,
    path: PathBuf,
    state: Arc<Mutex<RepoState>>,
    /// The claims on the repo's workspaces, fed to its task.
    claims: watch::Sender<RepoClaims>,
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
        /// The workspaces on this machine.
        workspaces: Vec<control::WorkspaceStatus>,
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
    pub fn new(hub: Arc<SyncHub>, claim: mpsc::UnboundedSender<ClaimUpdate>) -> Self {
        RepoSet {
            hub,
            repos: Mutex::new(BTreeMap::new()),
            changed: Arc::new(Notify::new()),
            settings: None,
            claim,
        }
    }

    /// A repo set whose workspaces all use `settings`.
    #[cfg(test)]
    fn with_settings(
        hub: Arc<SyncHub>,
        settings: Settings,
        claim: mpsc::UnboundedSender<ClaimUpdate>,
    ) -> Self {
        RepoSet {
            settings: Some(settings),
            ..Self::new(hub, claim)
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
    /// repos, shuts down removed ones and passes on workspace claims. A
    /// repo whose path or id changed is respawned.
    pub fn sync(&self, state: &MeshState) {
        let mut repos = self.repos.lock().unwrap();

        repos.retain(|name, handle| {
            let keep = state
                .repos
                .get(name)
                .is_some_and(|repo| handle.path == repo.path && handle.id == repo.id);
            if !keep {
                debug!(repo = %name, "removing repo watch");
                handle.task.abort();
                self.hub.unregister_repo(name);
            }
            keep
        });

        for (name, repo) in &state.repos {
            let claims = state.repo_claims(&repo.id);
            if let Some(handle) = repos.get(name) {
                if *handle.claims.borrow() != claims {
                    handle.claims.send_replace(claims);
                }
            } else {
                debug!(repo = %name, "managing repo");
                let handle = spawn_repo(self, name.clone(), repo, claims);
                repos.insert(name.clone(), handle);
            }
        }
        self.changed.notify_one();
    }

    /// Condenses every repo's state into the health report peers see.
    /// Local detail (paths, error messages) deliberately stays out: error
    /// strings embed filesystem paths, which never leave this machine.
    pub fn health(&self) -> Vec<RepoHealth> {
        let repos = self.repos.lock().unwrap();
        repos
            .iter()
            .map(|(name, handle)| {
                let state = match &*handle.state.lock().unwrap() {
                    // Opening and indexing are transitions, not faults.
                    RepoState::Opening | RepoState::Watching { .. } | RepoState::Indexing => {
                        RepoHealthState::Ok
                    }
                    RepoState::Backoff { .. } => RepoHealthState::Failed,
                    RepoState::Missing { .. } => RepoHealthState::Missing,
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
                let mut workspaces = Vec::new();
                let watch = match &*handle.state.lock().unwrap() {
                    RepoState::Opening => control::WatchStatus::Opening,
                    RepoState::Indexing => control::WatchStatus::Indexing,
                    RepoState::Watching {
                        op_heads,
                        last_change,
                        last_sync,
                        workspaces: local,
                    } => {
                        workspaces.clone_from(local);
                        control::WatchStatus::Watching {
                            op_heads: *op_heads as u64,
                            last_change_secs: last_change
                                .map(|at| at.elapsed().unwrap_or_default().as_secs()),
                            last_sync_secs: last_sync
                                .map(|at| at.elapsed().unwrap_or_default().as_secs()),
                        }
                    }
                    RepoState::Backoff { until, error } => control::WatchStatus::Failed {
                        error: error.clone(),
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                    RepoState::Missing { until } => control::WatchStatus::Missing {
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                };

                for (workspace, machines) in &handle.claims.borrow().others {
                    workspaces.extend(machines.iter().map(|machine| control::WorkspaceStatus {
                        name: workspace.clone(),
                        place: control::WorkspacePlace::Peer {
                            machine: machine.clone(),
                        },
                    }));
                }
                control::RepoStatus {
                    name: name.clone(),
                    path: handle.path.clone(),
                    watch,
                    workspaces,
                }
            })
            .collect()
    }
}

/// Sleeps until `deadline`, or never when it is `None`, so an optional
/// deadline can sit in a `select!` uniformly.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at.into()).await,
        None => std::future::pending().await,
    }
}

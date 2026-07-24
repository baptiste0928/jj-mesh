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
//! also absorbs event bursts and spurious wakeups. The daemon's own future
//! head writes (the sync apply path) will update the tracked set before
//! writing, so self-triggered events are suppressed the same way.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};
use jj_lib::{object_id::ObjectId as _, op_store::OperationId};
use tracing::{debug, info, warn};

use super::{
    control,
    hub::{Inbox, PeerAnnounce, SyncHub},
};
use crate::{
    config::{Config, RepoId},
    repo::{JjRepo, MeshRepo, transfer},
    watch::DirWatcher,
};

/// Retry delay after a failure to open or watch; doubles up to
/// [`BACKOFF_MAX`]. Covers repos on unmounted disks or with unsupported
/// backends without hot-looping.
const BACKOFF_MIN: Duration = Duration::from_secs(1);

/// Ceiling of the retry delay.
const BACKOFF_MAX: Duration = Duration::from_mins(1);

/// A watch surviving this long resets the backoff.
const STABLE_WATCH: Duration = Duration::from_secs(10);

/// Debounce window for op-heads events: one jj command swaps marker files
/// in a quick burst. Kept short, it bounds the sync latency.
const DEBOUNCE: Duration = Duration::from_millis(100);

/// Cap on the total debounce, so a busy repo cannot starve change handling.
const DEBOUNCE_MAX: Duration = Duration::from_secs(1);

/// How often to verify the watched directory still exists while idle: an
/// unmount kills the watch without emitting any event.
const LIVENESS_INTERVAL: Duration = Duration::from_mins(1);

/// Cap on stored error strings: they embed bytes read from repo files, so
/// their length is not ours to trust, and they are cloned into every
/// status response.
const MAX_ERROR_LEN: usize = 256;

/// Cap on head ids accepted in one announcement; legitimate divergence is
/// a few heads, anything more is a hostile or broken peer.
const MAX_ANNOUNCED_HEADS: usize = 64;

/// Hard budget on one fetch from a peer: a stalled or hostile server must
/// not pin the repo task forever (announcement handling and local change
/// publication pause while a fetch runs).
const FETCH_TIMEOUT: Duration = Duration::from_mins(30);

/// The set of managed repos, synced from the configuration.
#[derive(Debug)]
pub struct RepoSet {
    hub: Arc<SyncHub>,
    repos: Mutex<BTreeMap<RepoId, RepoHandle>>,
}

/// Book-keeping for one repo task.
#[derive(Debug)]
struct RepoHandle {
    name: String,
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
        last_change: Option<Instant>,
        last_sync: Option<Instant>,
    },
    Backoff {
        until: Instant,
        error: String,
    },
}

impl RepoSet {
    pub fn new(hub: Arc<SyncHub>) -> Self {
        RepoSet {
            hub,
            repos: Mutex::new(BTreeMap::new()),
        }
    }

    /// Aligns the managed repos with the configuration: spawns tasks for
    /// new repos and shuts down removed ones. A repo whose path changed is
    /// respawned; a renamed one keeps its task.
    pub fn sync(&self, config: &Config) {
        let mut desired: BTreeMap<&RepoId, (&str, &Path)> = BTreeMap::new();
        for (name, repo) in &config.repos {
            let previous = desired.insert(&repo.id, (name.as_str(), repo.path.as_path()));
            if let Some((shadowed, _)) = previous {
                // Ids collide only in a hand-edited config; nothing else
                // reports it, so at least leave a trace.
                warn!(
                    id = %repo.id,
                    "repos `{shadowed}` and `{name}` share the same id; only `{name}` is watched",
                );
            }
        }

        let mut repos = self.repos.lock().unwrap();

        repos.retain(|id, handle| {
            let Some((name, path)) = desired.get(id) else {
                info!(repo = %handle.name, "removing repo");
                handle.task.abort();
                self.hub.unregister_repo(id);
                return false;
            };

            if handle.path != *path {
                info!(repo = %handle.name, "repo moved, restarting its watch");
                handle.task.abort();
                self.hub.unregister_repo(id);
                return false;
            }

            if handle.name != *name {
                info!(old = %handle.name, new = %name, "renaming repo");
                (*name).clone_into(&mut handle.name);
            }
            true
        });

        for (id, (name, path)) in desired {
            repos.entry(id.clone()).or_insert_with(|| {
                info!(repo = %name, path = %path.display(), "managing repo");
                let announcements = self.hub.register_repo(id.clone());
                spawn_repo(
                    id.clone(),
                    name.to_owned(),
                    path.to_owned(),
                    self.hub.clone(),
                    announcements,
                )
            });
        }
    }

    /// Snapshots the state of every repo for the control socket.
    pub fn statuses(&self) -> Vec<control::RepoStatus> {
        let repos = self.repos.lock().unwrap();

        repos
            .values()
            .map(|handle| {
                let watch = match &*handle.state.lock().unwrap() {
                    RepoState::Opening => control::WatchStatus::Opening,
                    RepoState::Watching {
                        op_heads,
                        last_change,
                        last_sync,
                    } => control::WatchStatus::Watching {
                        op_heads: *op_heads as u64,
                        last_change_secs: last_change.map(|at| at.elapsed().as_secs()),
                        last_sync_secs: last_sync.map(|at| at.elapsed().as_secs()),
                    },
                    RepoState::Backoff { until, error } => control::WatchStatus::Failed {
                        error: error.clone(),
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                };

                control::RepoStatus {
                    name: handle.name.clone(),
                    id: handle.id.clone(),
                    path: handle.path.clone(),
                    watch,
                }
            })
            .collect()
    }
}

/// Spawns the watch task for one repo.
fn spawn_repo(
    id: RepoId,
    name: String,
    path: PathBuf,
    hub: Arc<SyncHub>,
    announcements: Arc<Inbox>,
) -> RepoHandle {
    let state = Arc::new(Mutex::new(RepoState::Opening));

    let task = tokio::spawn(run_repo(RepoTask {
        id: id.clone(),
        name: name.clone(),
        path: path.clone(),
        state: state.clone(),
        hub,
        announcements,
    }));

    RepoHandle {
        name,
        id,
        path,
        state,
        task,
    }
}

/// Everything a repo task owns.
struct RepoTask {
    id: RepoId,
    name: String,
    path: PathBuf,
    state: Arc<Mutex<RepoState>>,
    hub: Arc<SyncHub>,
    announcements: Arc<Inbox>,
}

/// Opens and watches one repo forever, reopening with backoff on failure.
async fn run_repo(task: RepoTask) {
    let mut backoff = BACKOFF_MIN;

    loop {
        task.set_state(RepoState::Opening);
        let started = Instant::now();

        let err = task.watch().await.unwrap_err();
        warn!(repo = %task.name, "repo watch failed: {err:#}");
        // The stores may be stale (moved disk, replaced repo): stop
        // serving fetches from them until the reopen succeeds.
        task.hub.repo_closed(&task.id);

        if started.elapsed() >= STABLE_WATCH {
            backoff = BACKOFF_MIN;
        }
        task.set_state(RepoState::Backoff {
            until: Instant::now() + backoff,
            error: truncated_error(&err),
        });
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

impl RepoTask {
    /// Watches the repo's op heads until something fails, announcing local
    /// changes through the hub, fetching announced changes from peers, and
    /// serving peer fetches.
    ///
    /// The head reads here are cheap single-shot store calls (one readdir),
    /// safe from async context; see the `repo::mesh` module docs. Opening
    /// the repo is heavier (gix opens the git repo), so it runs on a
    /// blocking thread: a hung disk must stall this repo, not the daemon.
    async fn watch(&self) -> Result<std::convert::Infallible> {
        let path = self.path.clone();
        let (jj, repo) = tokio::task::spawn_blocking(move || -> Result<(JjRepo, MeshRepo)> {
            let jj = JjRepo::discover(&path)?;
            let repo = jj.open()?;
            Ok((jj, repo))
        })
        .await
        .wrap_err("repo open task failed")??;
        let repo = Arc::new(repo);
        // Fetch serving is dispatched by the hub, never by this loop: a
        // fetch below may block on the very peer being served.
        self.hub.repo_opened(&self.id, repo.clone());

        // Watch before the first read: changes racing the setup produce at
        // worst a no-change wakeup.
        let heads_dir = jj.op_heads_dir();
        let mut watch = DirWatcher::new(&heads_dir, DEBOUNCE, DEBOUNCE_MAX, |_| true)?;
        let mut heads = sorted_heads(&repo).await?;
        let mut last_change = None;
        let mut last_sync = None;

        info!(repo = %self.name, "watching repo");
        // Publishing on watch start doubles as anti-entropy: changes made
        // while the watch was down are absorbed into the baseline above and
        // would otherwise never be announced.
        self.hub.publish(&self.id, wire_heads(&heads));
        self.set_state(RepoState::Watching {
            op_heads: heads.len(),
            last_change,
            last_sync,
        });

        loop {
            tokio::select! {
                changed = watch.changed_or_idle(LIVENESS_INTERVAL) => {
                    if !changed? {
                        // No events for a while: check the watch is not
                        // dead in a way that produces none (unmount).
                        ensure!(heads_dir.is_dir(), "the op heads directory is gone");
                    }
                }
                () = self.announcements.changed() => {}
            }

            // Announcements are handled before the head re-read below:
            // fetching updates the heads, so the re-read then picks the
            // change up in this same iteration and the baseline update
            // suppresses the watcher events our own writes caused.
            for announce in self.announcements.drain() {
                if self.handle_announce(&repo, &announce).await? == Some(true) {
                    last_sync = Some(Instant::now());
                }
            }

            // Heads are re-read and the inbox drained on every wake: both
            // are cheap, wakes are debounced or rare, and the select above
            // can cancel a watch signal mid-debounce, so no single wake
            // source is relied on.
            let new = sorted_heads(&repo).await?;
            if new != heads {
                heads = new;
                last_change = Some(Instant::now());
                info!(repo = %self.name, op_heads = heads.len(), "op heads changed");
                self.hub.publish(&self.id, wire_heads(&heads));
            }
            self.set_state(RepoState::Watching {
                op_heads: heads.len(),
                last_change,
                last_sync,
            });
        }
    }

    /// Handles a peer's head announcement: fetches announced heads that
    /// are missing locally. Returns whether anything was fetched (`None`
    /// for malformed announcements).
    async fn handle_announce(
        &self,
        repo: &Arc<MeshRepo>,
        announce: &PeerAnnounce,
    ) -> Result<Option<bool>> {
        let id_len = repo.root_operation_id().as_bytes().len();
        if announce.heads.len() > MAX_ANNOUNCED_HEADS
            || announce.heads.iter().any(|head| head.len() != id_len)
        {
            debug!(repo = %self.name, peer = %announce.peer, "ignoring malformed announcement");
            return Ok(None);
        }

        let mut missing = Vec::new();
        for head in &announce.heads {
            let head = OperationId::new(head.clone());
            if !repo.has_operation(&head).await? {
                missing.push(head);
            }
        }
        if missing.is_empty() {
            debug!(repo = %self.name, peer = %announce.peer, "in sync with peer");
            return Ok(Some(false));
        }

        // Sync failures must not kill the watch: the repo is fine, the
        // peer or network is not. The next announcement retries.
        match self.fetch_missing(repo, announce.peer, &missing).await {
            Ok(outcome) => {
                info!(
                    repo = %self.name, peer = %announce.peer,
                    ops = outcome.ops, objects = outcome.git_objects,
                    "synced from peer",
                );
                Ok(Some(true))
            }
            Err(err) => {
                warn!(repo = %self.name, peer = %announce.peer, "sync failed: {err:#}");
                Ok(Some(false))
            }
        }
    }

    /// Fetches missing op heads from the announcing peer over a fresh
    /// bidirectional stream.
    async fn fetch_missing(
        &self,
        repo: &Arc<MeshRepo>,
        peer: iroh::EndpointId,
        wants: &[OperationId],
    ) -> Result<transfer::FetchOutcome> {
        let conn = self
            .hub
            .connection(&peer)
            .ok_or_else(|| eyre!("peer is no longer connected"))?;
        let (mut send, mut recv) = conn.open_bi().await?;
        let fetch = transfer::fetch(repo, &self.id, wants, &mut send, &mut recv);
        let outcome = tokio::time::timeout(FETCH_TIMEOUT, fetch)
            .await
            .map_err(|_| eyre!("fetch timed out"))??;
        let _ = send.finish();
        Ok(outcome)
    }

    fn set_state(&self, state: RepoState) {
        *self.state.lock().unwrap() = state;
    }
}

/// Reads the current op heads as a sorted set, comparable across reads.
async fn sorted_heads(repo: &MeshRepo) -> Result<Vec<OperationId>> {
    let mut heads = repo.op_heads().await?;
    heads.sort_unstable();
    Ok(heads)
}

/// Converts op head ids to their wire form.
fn wire_heads(heads: &[OperationId]) -> Vec<Vec<u8>> {
    heads.iter().map(|head| head.as_bytes().to_vec()).collect()
}

/// Formats an error for storage, bounded by [`MAX_ERROR_LEN`].
fn truncated_error(err: &color_eyre::Report) -> String {
    let mut msg = format!("{err:#}");
    if msg.chars().count() > MAX_ERROR_LEN {
        msg = msg.chars().take(MAX_ERROR_LEN).collect();
        msg.push('…');
    }
    msg
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{config::Repo, tests::Fixture};

    /// Polls until `pred` holds on the statuses, panicking after 10s.
    async fn wait_for(set: &RepoSet, pred: impl Fn(&[control::RepoStatus]) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if pred(&set.statuses()) {
                return;
            }
            assert!(Instant::now() < deadline, "condition not reached in time");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn config_with(name: &str, path: &Path) -> Config {
        let mut config = Config::default();
        config.repos.insert(
            name.to_owned(),
            Repo {
                id: RepoId::generate(),
                path: path.to_owned(),
            },
        );
        config
    }

    #[tokio::test]
    async fn watches_and_detects_head_changes() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");

        let set = RepoSet::new(Arc::new(SyncHub::new()));
        set.sync(&config_with("a", &dir));

        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching {
                        op_heads: 1,
                        last_change_secs: None,
                        ..
                    },
                    ..
                }]
            )
        })
        .await;

        fx.jj(&dir, &["new", "-m", "change"]);

        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching {
                        last_change_secs: Some(_),
                        ..
                    },
                    ..
                }]
            )
        })
        .await;

        set.sync(&Config::default());
        assert!(set.statuses().is_empty());
    }

    /// Removing and recreating a watched repo must not leave a dead watch:
    /// the task reopens and keeps detecting changes at the same path.
    #[tokio::test]
    async fn recovers_after_repo_recreation() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");

        let set = RepoSet::new(Arc::new(SyncHub::new()));
        set.sync(&config_with("a", &dir));
        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching { .. },
                    ..
                }]
            )
        })
        .await;

        std::fs::remove_dir_all(&dir).unwrap();
        fx.init_repo("a");

        // The dead watch must be noticed (Failed), then rebuilt (Watching);
        // both states persist long enough for the 50ms polling to see them.
        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Failed { .. },
                    ..
                }]
            )
        })
        .await;
        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching { .. },
                    ..
                }]
            )
        })
        .await;

        fx.jj(&dir, &["new", "-m", "after-recreation"]);
        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching {
                        last_change_secs: Some(_),
                        ..
                    },
                    ..
                }]
            )
        })
        .await;
    }

    #[tokio::test]
    async fn reports_failure_for_invalid_repo() {
        let fx = Fixture::new();
        let set = RepoSet::new(Arc::new(SyncHub::new()));
        set.sync(&config_with("ghost", &fx.path().join("missing")));

        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Failed { .. },
                    ..
                }]
            )
        })
        .await;
    }
}

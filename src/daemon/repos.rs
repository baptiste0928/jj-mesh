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
//! daemon was down — but only while the op head is single. Any jj
//! command reconciles divergent op heads by writing a merge operation,
//! so daemons doing this on both ends of a divergence would ping-pong
//! fresh merge operations at each other. Divergence is left to the next
//! actual jj activity (a user command, an auto-snapshot), whose merge
//! then arrives here as a single head.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};
use jj_lib::{object_id::ObjectId as _, op_store::OperationId};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use super::{
    backoff::Backoff,
    control,
    hub::{Inbox, PeerAnnounce, SyncHub},
};
use crate::{
    config::{MeshState, RepoId, Settings},
    repo::{JjRepo, MeshRepo, transfer},
    watch::{DirWatcher, TreeWatcher},
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

/// Delay before retrying a fetch that failed. The announcement was already
/// consumed, so without this a transient failure (a peer momentarily busy,
/// a dropped stream) would strand the change until the peer next announces
/// or reconnects. Kept coarse: the failures it covers are not urgent.
const FETCH_RETRY: Duration = Duration::from_secs(30);

/// Budget for one spawned jj command. Generous: the first snapshot of a
/// large working copy legitimately takes a while.
const JJ_TIMEOUT: Duration = Duration::from_mins(5);

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
    pub fn health(&self) -> Vec<crate::net::sync::RepoHealth> {
        use crate::net::sync::{RepoHealth, RepoHealthState};

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

/// Spawns the watch task for one repo.
fn spawn_repo(
    id: RepoId,
    name: String,
    path: PathBuf,
    hub: Arc<SyncHub>,
    announcements: Arc<Inbox>,
    changed: Arc<Notify>,
    settings: Arc<Settings>,
) -> RepoHandle {
    let state = Arc::new(Mutex::new(RepoState::Opening));

    let task = tokio::spawn(run_repo(RepoTask {
        id: id.clone(),
        name,
        path: path.clone(),
        state: state.clone(),
        hub,
        announcements,
        changed,
        settings,
    }));

    RepoHandle {
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
    /// The repo set's change notifier, pinged on every state change.
    changed: Arc<Notify>,
    /// Daemon settings, fixed for the daemon's lifetime.
    settings: Arc<Settings>,
}

/// Opens and watches one repo forever: reopening immediately when the
/// watch ends because the store configuration changed, with backoff when
/// it failed.
async fn run_repo(task: RepoTask) {
    let mut backoff = Backoff::new(BACKOFF_MIN, BACKOFF_MAX);

    loop {
        task.set_state(RepoState::Opening);
        let started = Instant::now();

        let err = match task.watch().await {
            // A reconfiguration is expected behavior, not a fault: reopen
            // cleanly instead of sitting out a backoff unserved.
            Ok(()) => {
                info!(repo = %task.name, "repo configuration changed; reopening");
                task.hub.repo_closed(&task.name, &task.id);
                backoff.reset();
                continue;
            }
            Err(err) => err,
        };
        warn!(repo = %task.name, "repo watch failed: {err:#}");
        // The stores may be stale (moved disk, replaced repo): stop
        // serving fetches from them until the reopen succeeds.
        task.hub.repo_closed(&task.name, &task.id);

        if started.elapsed() >= STABLE_WATCH {
            backoff.reset();
        }
        let delay = backoff.next_delay();
        let until = Instant::now() + delay;
        // A missing repo directory is not a repo problem to diagnose but a
        // gone repo (unmounted disk, or deleted without `jj-mesh repo forget`):
        // surfaced as its own state so the status can suggest the fix. The
        // stat runs on a blocking thread; a hung mount is one of the very
        // conditions being probed.
        let path = task.path.clone();
        let present = tokio::task::spawn_blocking(move || crate::repo::repo_present(&path))
            .await
            .unwrap_or(true);
        if present {
            task.set_state(RepoState::Backoff {
                until,
                error: truncated_error(&err),
            });
        } else {
            task.set_state(RepoState::Missing { until });
        }
        tokio::time::sleep(delay).await;
    }
}

impl RepoTask {
    /// Watches the repo's op heads until it stops: `Ok(())` when the store
    /// configuration changed underneath it (the caller reopens cleanly),
    /// an error when something failed. Announces local changes through the
    /// hub and fetches announced changes from peers; serving peer fetches
    /// is dispatched by the hub.
    ///
    /// The head reads here are cheap single-shot store calls (one readdir),
    /// safe from async context; see the `repo::mesh` module docs.
    async fn watch(&self) -> Result<()> {
        let (jj, repo, fingerprint) = self.open().await?;
        // Fetch serving is dispatched by the hub, never by this loop: a
        // fetch below may block on the very peer being served.
        self.hub.repo_opened(&self.name, &self.id, repo.clone());

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
        self.hub.publish(&self.name, &self.id, wire_heads(&heads));
        self.set_state(RepoState::Watching {
            op_heads: heads.len(),
            last_change,
            last_sync,
        });

        let mut snap = Snapshotting::default();
        let mut tree = match self.settings().snapshot_interval {
            Some(_) => self.watch_tree(jj.root()).await,
            None => None,
        };

        // Heal staleness accrued while the daemon was down (or right
        // before a crash); afterwards every applied sync triggers it
        // directly. The op-heads watch above is already live, so any
        // operation this creates is picked up like any other.
        if heads.len() == 1 {
            self.update_stale(&mut tree).await;
        }
        // Edits made while the watch was down produce no event, so the
        // working copy is snapshotted once on start for the same reason
        // the heads are published above: it is the only anti-entropy the
        // snapshot path has.
        snap.arm(self.settings().snapshot_interval);

        // When set, the time to wake and retry fetches that failed and were
        // requeued into the inbox. Requeued heads are re-drained on any
        // wake, so this is only a fallback that fires when nothing else
        // would wake the task first.
        let mut retry_at: Option<Instant> = None;

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
                () = sleep_until(retry_at) => {}
                outcome = tree_changed(&mut tree) => {
                    match outcome {
                        Ok(()) => snap.arm(self.settings().snapshot_interval),
                        Err(err) => {
                            warn!(
                                repo = %self.name,
                                "working copy watch failed, auto-snapshot \
                                 disabled until the repo reopens: {err:#}",
                            );
                            tree = None;
                        }
                    }
                    // Edits are frequent and arming is all that is needed:
                    // skip the fingerprint and head re-checks below.
                    continue;
                }
                () = sleep_until(snap.deadline) => {
                    self.snapshot(&mut snap, &mut tree).await;
                    // The snapshot operation wakes the op-heads watch,
                    // which then re-reads and announces the heads.
                    continue;
                }
            }

            // jj_lib resolved the store configuration once at open and
            // never re-reads it, so a repo reconfigured underneath the
            // daemon (converted colocation, swapped backend, replaced
            // repo) leaves `repo` silently operating on stale stores.
            // Re-checked on every wake: a handful of tiny reads (on a
            // blocking thread, against hung mounts), and every failure
            // mode below (a sync writing through a stale git path, most
            // of all) starts with a wake.
            if store_fingerprint(&jj).await? != fingerprint {
                return Ok(());
            }

            // Announcements are handled before the head re-read below:
            // fetching updates the heads, so the re-read then picks the
            // change up in this same iteration and the baseline update
            // suppresses the watcher events our own writes caused.
            let drained = self.drain_announcements(&repo).await?;
            if drained.synced {
                last_sync = Some(SystemTime::now());
            }
            retry_at = drained.retry.then(|| Instant::now() + FETCH_RETRY);

            // Heads are re-read and the inbox drained on every wake: both
            // are cheap, wakes are debounced or rare, and the select above
            // can cancel a watch signal mid-debounce, so no single wake
            // source is relied on.
            let new = sorted_heads(&repo).await?;
            if new != heads {
                heads = new;
                last_change = Some(SystemTime::now());
                info!(repo = %self.name, op_heads = heads.len(), "op heads changed");
                self.hub.publish(&self.name, &self.id, wire_heads(&heads));
            }

            // The applied operations may have left the working copy
            // stale. Only a single head is caught up on (see the module
            // docs on divergence).
            if drained.synced && heads.len() == 1 {
                self.update_stale(&mut tree).await;
                // update-stale snapshots the working copy itself, so a
                // pending snapshot has just been done.
                snap.done();
            }
            self.set_state(RepoState::Watching {
                op_heads: heads.len(),
                last_change,
                last_sync,
            });
        }
    }

    /// Opens the repo's stores. Opening is heavy (gix opens the git repo,
    /// the self-check reads whole views), so it runs on a blocking
    /// thread: a hung disk must stall this repo, not the daemon.
    async fn open(&self) -> Result<(JjRepo, Arc<MeshRepo>, crate::repo::StoreFingerprint)> {
        use pollster::FutureExt as _;

        let path = self.path.clone();
        let (jj, repo, fingerprint) =
            tokio::task::spawn_blocking(move || -> Result<(JjRepo, MeshRepo, _)> {
                let jj = JjRepo::discover(&path)?;
                // The fingerprint is captured before the open: taken after,
                // a reconfiguration racing the open could leave stale
                // stores behind a matching fingerprint.
                let fingerprint = jj.fingerprint()?;
                let repo = jj.open()?;
                // Formats this build cannot decode fail the repo here,
                // before it is served or announced anywhere.
                repo.self_check().block_on()?;
                Ok((jj, repo, fingerprint))
            })
            .await
            .wrap_err("repo open task failed")??;
        Ok((jj, Arc::new(repo), fingerprint))
    }

    /// Builds the working-copy watcher, degrading to `None` instead of
    /// failing the repo: op sync must survive a tree too large or busted
    /// to watch. `None` means no snapshots for this repo, nothing else.
    async fn watch_tree(&self, root: &std::path::Path) -> Option<TreeWatcher> {
        match TreeWatcher::new(root).await {
            Ok(tree) => Some(tree),
            Err(err) => {
                warn!(
                    repo = %self.name,
                    "cannot watch working copy files, auto-snapshot disabled: {err:#}",
                );
                None
            }
        }
    }

    /// The effective settings for this repo.
    fn settings(&self) -> crate::config::RepoSettings {
        self.settings.for_repo(&self.name)
    }

    /// Drains the announcement inbox, handling every entry. Failed
    /// fetches are requeued (a newer announcement or a reconnect
    /// supersedes them) and reported for a retry wakeup.
    async fn drain_announcements(&self, repo: &Arc<MeshRepo>) -> Result<Drained> {
        let mut drained = Drained {
            synced: false,
            retry: false,
        };
        for announce in self.announcements.drain() {
            match self.handle_announce(repo, &announce).await? {
                Handled::Fetched => drained.synced = true,
                Handled::Failed => {
                    self.announcements
                        .requeue(announce.peer, announce.seq, announce.heads);
                    drained.retry = true;
                }
                Handled::Idle => {}
            }
        }
        Ok(drained)
    }

    /// Handles a peer's head announcement: fetches announced heads that are
    /// missing locally.
    async fn handle_announce(
        &self,
        repo: &Arc<MeshRepo>,
        announce: &PeerAnnounce,
    ) -> Result<Handled> {
        let id_len = repo.root_operation_id().as_bytes().len();
        if announce.heads.len() > MAX_ANNOUNCED_HEADS
            || announce.heads.iter().any(|head| head.len() != id_len)
        {
            debug!(repo = %self.name, peer = %announce.peer, "ignoring malformed announcement");
            return Ok(Handled::Idle);
        }
        // This check is what enforces the fetch side of the colocation
        // pause: the hub keeps routing announcements while paused, and
        // requeueing them here (silently, at the fetch-retry cadence)
        // means the heads are fetched as soon as the pause lifts instead
        // of being lost until the peer's next change.
        if self.hub.is_paused(&self.name) {
            debug!(repo = %self.name, "sync is paused; holding announcement");
            return Ok(Handled::Failed);
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
            return Ok(Handled::Idle);
        }

        // Sync failures must not kill the watch: the repo is fine, the peer
        // or network is not. The caller requeues for a later retry.
        match self.fetch_missing(repo, announce.peer, &missing).await {
            Ok(outcome) => {
                info!(
                    repo = %self.name, peer = %announce.peer,
                    ops = outcome.ops, objects = outcome.git_objects,
                    "synced from peer",
                );
                Ok(Handled::Fetched)
            }
            Err(err) => {
                warn!(repo = %self.name, peer = %announce.peer, "sync failed: {err:#}");
                Ok(Handled::Failed)
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
        let fetch = transfer::fetch(
            repo,
            &self.name,
            &self.id,
            wants,
            crate::net::sync::GitTransferFormat::Loose,
            &mut send,
            &mut recv,
            transfer::ProgressSink::default(),
        );
        let outcome = tokio::time::timeout(FETCH_TIMEOUT, fetch)
            .await
            .map_err(|_| eyre!("fetch timed out"))??;
        let _ = send.finish();
        Ok(outcome)
    }

    /// Runs `jj workspace update-stale` when enabled for this repo.
    /// Note that it snapshots the working copy before checking staleness,
    /// so it is never free even when nothing is stale. Failures only
    /// warn: the working copy may be locked by an ongoing command, and
    /// the next sync retries.
    async fn update_stale(&self, tree: &mut Option<TreeWatcher>) {
        if !self.settings().update_stale {
            return;
        }
        debug!(repo = %self.name, "checking for a stale working copy");
        self.run_jj(&["workspace", "update-stale"], tree).await;
    }

    /// Snapshots the working copy through the jj binary, which applies
    /// the user's snapshot configuration and takes the working-copy lock.
    async fn snapshot(&self, snap: &mut Snapshotting, tree: &mut Option<TreeWatcher>) {
        debug!(repo = %self.name, "snapshotting working copy");
        let started = Instant::now();
        self.run_jj(&["util", "snapshot"], tree).await;
        snap.finished(started);
    }

    /// Runs one working-copy jj command, then drops the events it caused:
    /// update-stale writes working-copy files, and letting the watcher
    /// see them would schedule a snapshot of the daemon's own work, on
    /// and on. Failures only warn, the repo is fine either way.
    async fn run_jj(&self, args: &[&str], tree: &mut Option<TreeWatcher>) {
        if let Err(err) = crate::repo::run_jj(&self.path, args, JJ_TIMEOUT).await {
            warn!(repo = %self.name, "jj {} failed: {err:#}", args.join(" "));
        }
        if let Some(watcher) = tree
            && let Err(err) = watcher.discard_queued().await
        {
            warn!(
                repo = %self.name,
                "working copy watch failed, auto-snapshot disabled until the \
                 repo reopens: {err:#}",
            );
            *tree = None;
        }
    }

    fn set_state(&self, state: RepoState) {
        *self.state.lock().unwrap() = state;
        self.changed.notify_one();
    }
}

/// Scheduling state of one repo's auto-snapshots.
///
/// The first edit arms a snapshot one interval out and later edits never
/// postpone it, so continuous editing snapshots at the configured
/// cadence. A snapshot walks the whole working copy though, which on a
/// large repo can take longer than the interval; the last one's duration
/// therefore also sets a floor on the gap to the next, so the daemon
/// cannot end up snapshotting an unbounded fraction of the time.
#[derive(Debug, Default)]
struct Snapshotting {
    /// When the pending snapshot is due, if one is pending.
    deadline: Option<Instant>,
    /// Earliest acceptable time for the next snapshot, from the cost of
    /// the last one.
    earliest: Option<Instant>,
}

/// How much of the time a repeated snapshot may occupy, as the ratio of
/// the enforced gap to the snapshot's own duration. Only binds on repos
/// where a snapshot outlasts the configured interval.
const SNAPSHOT_DUTY_DIVISOR: u32 = 2;

impl Snapshotting {
    /// Schedules a snapshot `interval` from now unless one is already
    /// pending, or auto-snapshotting is off (`interval` is `None`).
    fn arm(&mut self, interval: Option<Duration>) {
        let Some(interval) = interval else {
            return;
        };
        if self.deadline.is_some() {
            return;
        }
        let due = Instant::now() + interval;
        self.deadline = Some(self.earliest.map_or(due, |floor| due.max(floor)));
    }

    /// Records a snapshot that ran, from the instant it started.
    fn finished(&mut self, started: Instant) {
        self.done();
        self.earliest = Some(Instant::now() + started.elapsed() * SNAPSHOT_DUTY_DIVISOR);
    }

    /// Clears the pending snapshot, after something else did the work.
    fn done(&mut self) {
        self.deadline = None;
    }
}

/// Outcome of one announcement inbox drain.
struct Drained {
    /// Whether any fetch applied new operations.
    synced: bool,
    /// Whether any fetch failed and deserves a retry wakeup.
    retry: bool,
}

/// Outcome of handling one peer announcement.
enum Handled {
    /// New operations were fetched and applied.
    Fetched,
    /// Nothing to do: already in sync, or the announcement was malformed.
    Idle,
    /// A fetch was attempted but failed; the heads are worth retrying.
    Failed,
}

/// Sleeps until `deadline`, or never when it is `None`, so an optional
/// retry deadline can sit in a `select!` uniformly.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at.into()).await,
        None => std::future::pending().await,
    }
}

/// Waits for the next working-copy change, or forever when tree watching
/// is disabled, so it can sit in a `select!` uniformly.
async fn tree_changed(tree: &mut Option<TreeWatcher>) -> Result<()> {
    match tree {
        Some(tree) => tree.changed().await,
        None => std::future::pending().await,
    }
}

/// Re-captures the store fingerprint: a handful of tiny reads, but on a
/// blocking thread since a hung mount is one of the probed conditions.
async fn store_fingerprint(jj: &JjRepo) -> Result<crate::repo::StoreFingerprint> {
    let jj = jj.clone();
    tokio::task::spawn_blocking(move || jj.fingerprint())
        .await
        .wrap_err("fingerprint task failed")?
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
    use crate::{config::Repo, testing::Fixture};

    /// A repo set with the given `config.toml` contents.
    fn repo_set(config: &str) -> RepoSet {
        let settings = Arc::new(toml::from_str(config).unwrap());
        RepoSet::new(Arc::new(SyncHub::new()), settings)
    }

    /// A repo set with auto-snapshot and update-stale disabled: hermetic
    /// (the daemon spawns no jj, which would read the user's real config).
    fn quiet_repo_set() -> RepoSet {
        repo_set("snapshot-interval = 0\nupdate-stale = false")
    }

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

    fn state_with(name: &str, path: &std::path::Path) -> MeshState {
        let mut state = MeshState::default();
        state.repos.insert(
            name.to_owned(),
            Repo {
                id: RepoId::generate(),
                path: path.to_owned(),
            },
        );
        state
    }

    #[tokio::test]
    async fn watches_and_detects_head_changes() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");

        let set = quiet_repo_set();
        set.sync(&state_with("a", &dir));

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

        set.sync(&MeshState::default());
        assert!(set.statuses().is_empty());
    }

    /// Removing and recreating a watched repo must not leave a dead watch:
    /// the task reopens and keeps detecting changes at the same path.
    #[tokio::test]
    async fn recovers_after_repo_recreation() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");

        let set = quiet_repo_set();
        set.sync(&state_with("a", &dir));
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

        // The dead watch must be noticed (Failed, or Missing when the
        // failure lands in the removed-not-yet-recreated window), then
        // rebuilt (Watching); both states persist long enough for the 50ms
        // polling to see them.
        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Failed { .. }
                        | control::WatchStatus::Missing { .. },
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

    /// A path with no repo directory at all is `Missing` (the state that
    /// suggests `jj-mesh repo forget`), not a generic failure.
    #[tokio::test]
    async fn reports_missing_for_absent_repo_dir() {
        let fx = Fixture::new();
        let set = quiet_repo_set();
        set.sync(&state_with("ghost", &fx.path().join("missing")));

        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Missing { .. },
                    ..
                }]
            )
        })
        .await;
    }

    /// A directory that exists but is not a usable repo is a `Failed`
    /// watch, with the open error preserved.
    #[tokio::test]
    async fn reports_failure_for_invalid_repo() {
        let fx = Fixture::new();
        let dir = fx.path().join("broken");
        std::fs::create_dir_all(dir.join(".jj")).unwrap();

        let set = quiet_repo_set();
        set.sync(&state_with("broken", &dir));

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

    /// An edit to a working-copy file must produce a snapshot operation
    /// one interval later, visible as an op-heads change.
    #[tokio::test]
    async fn auto_snapshots_working_copy_edits() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");

        let set = repo_set("snapshot-interval = 1\nupdate-stale = false");
        set.sync(&state_with("a", &dir));
        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching {
                        last_change_secs: None,
                        ..
                    },
                    ..
                }]
            )
        })
        .await;

        std::fs::write(dir.join("edited.txt"), "content").unwrap();

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

    /// A stale working copy (op head advanced without updating it, which
    /// is what applying synced operations does) is healed on watch start.
    #[tokio::test]
    async fn updates_stale_working_copy() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        // Change the working-copy commit's tree without updating the
        // working copy: jj only considers it stale when the trees differ.
        std::fs::write(dir.join("f.txt"), "content").unwrap();
        fx.jj(&dir, &["status"]);
        fx.jj(&dir, &["--ignore-working-copy", "abandon", "@"]);
        assert!(
            !fx.jj_ok(&dir, &["status"]),
            "the working copy must start stale for this test to mean anything",
        );

        let set = repo_set("snapshot-interval = 0\nupdate-stale = true");
        set.sync(&state_with("a", &dir));

        let deadline = Instant::now() + Duration::from_secs(10);
        while !fx.jj_ok(&dir, &["status"]) {
            assert!(Instant::now() < deadline, "working copy still stale");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Changing a watched repo's store configuration must be detected on
    /// the next wake and reopen the repo against the new configuration
    /// instead of continuing on stale stores.
    #[tokio::test]
    async fn reopens_when_store_configuration_changes() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        let repo = JjRepo::discover(&dir).unwrap();
        let before = repo.fingerprint().unwrap();

        let set = quiet_repo_set();
        set.sync(&state_with("a", &dir));
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

        // Point git_target somewhere unusable, then wake the watch with a
        // transient file in the watched op-heads directory (jj itself can
        // no longer run against the broken configuration): the fingerprint
        // change must force a reopen, which fails against the broken
        // configuration. Watching on stale stores would sail right past
        // this.
        let target = dir.join(".jj/repo/store/git_target");
        let original = std::fs::read_to_string(&target).unwrap();
        std::fs::write(&target, "does-not-exist").unwrap();
        assert_ne!(repo.fingerprint().unwrap(), before);
        let wake = dir.join(".jj/repo/op_heads/heads/.wake");
        std::fs::write(&wake, "").unwrap();
        std::fs::remove_file(&wake).unwrap();
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

        // Restoring the configuration heals the repo on the next retry.
        std::fs::write(&target, original).unwrap();
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
    }
}

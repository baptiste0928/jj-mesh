//! The per-repo watch task: open, watch op heads, announce local changes,
//! fetch announced changes from peers, and run the workspace tasks.
//!
//! The task watches the repo's op-heads directory: every mutating jj
//! command atomically swaps head marker files there, so a change event
//! means new operations to announce. The task publishes its head set
//! through the sync hub (on change and on watch start) and fetches
//! operations peers announce; serving peer fetches is dispatched by the
//! hub directly (never through this task's loop, which may itself be
//! fetching).
//!
//! Change detection compares the head set against the last one seen, which
//! also absorbs event bursts and spurious wakeups; the task's own head
//! writes (applying fetched operations) fold into that baseline before the
//! comparison, so self-triggered events are suppressed the same way.
//!
//! The workspaces are found again whenever the op heads change (adding,
//! forgetting and renaming a workspace are operations), when the claims
//! change and on idle liveness checks. Only claimed workspaces get a task,
//! once the store has recorded the claim. A workspace is claimed when:
//! - its name is valid, no peer claims it, and the machine's cap allows;
//! - it is the main workspace, or a fresh one (its working copy reflects
//!   the op head): a stale one may be a forgotten workspace that a peer
//!   reusing its name brought back into the view (jj keeps forgotten
//!   workspaces in its store).
//!
//! A claim is released only when its name leaves the view, so a workspace
//! briefly gone from disk (an unmounted disk) keeps it.
//!
//! On watch start the task also rebuilds the commit index for op heads
//! that lack one (a fetch whose build failed, a repo synced by an older
//! jj-mesh), showing the repo as indexing meanwhile: without it the
//! user's next jj command would pay for the rebuild.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};
use iroh::EndpointId;
use jj_lib::{object_id::ObjectId as _, op_store::OperationId};
use pollster::FutureExt as _;
use tokio::sync::{Notify, mpsc, watch};
use tracing::{Instrument as _, debug, info, info_span, warn};

use super::{
    ClaimUpdate, RepoHandle, RepoSet, RepoState, WorkspaceState, sleep_until,
    workspace::{RepoContext, WorkspaceHandle, may_be_stale, spawn_workspace},
};
use crate::{
    config::{Repo, RepoClaims, RepoId, Settings, validate_name},
    daemon::{
        backoff::Backoff,
        control::{WorkspacePlace, WorkspaceStatus},
        hub::{Inbox, PeerAnnounce, SyncHub},
    },
    net::fetch::GitTransferFormat,
    repo::{JjRepo, OpenRepo, StoreFingerprint, Workspace, repo_present, transfer},
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

/// Deadline for the network-facing work of one fetch from a peer: a
/// stalled or hostile server must not pin the repo task forever
/// (announcement handling and local change publication pause while a
/// fetch runs). The fetch's local apply and index work runs unbounded.
const FETCH_NET_TIMEOUT: Duration = Duration::from_mins(30);

/// Budget for opening the fetch stream on an established connection: a
/// peer that never grants stream credit must not hang the task.
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// Delay before retrying a fetch that failed. The announcement was already
/// consumed, so without this a transient failure (a peer momentarily busy,
/// a dropped stream) would strand the change until the peer next announces
/// or reconnects. Kept coarse: the failures it covers are not urgent.
const FETCH_RETRY: Duration = Duration::from_secs(30);

/// Spawns the watch task for the repo registered as `name`, in a span
/// that gives its events (and its workspace tasks') the repo field.
pub(super) fn spawn_repo(
    set: &RepoSet,
    name: String,
    repo: &Repo,
    claims: RepoClaims,
) -> RepoHandle {
    let state = Arc::new(Mutex::new(RepoState::Opening));
    let announcements = set.hub.register_repo(name.clone(), repo.id.clone());
    let (claims, claims_rx) = watch::channel(claims);

    let span = info_span!("repo", repo = %name);
    let task = tokio::spawn(
        run_repo(RepoTask {
            id: repo.id.clone(),
            name,
            path: repo.path.clone(),
            state: state.clone(),
            hub: set.hub.clone(),
            announcements,
            changed: set.changed.clone(),
            settings: set.settings.clone(),
            claims: claims_rx,
            claim: set.claim.clone(),
        })
        .instrument(span),
    );

    RepoHandle {
        id: repo.id.clone(),
        path: repo.path.clone(),
        state,
        claims,
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
    /// The claims on the repo's workspaces.
    claims: watch::Receiver<RepoClaims>,
    claim: mpsc::UnboundedSender<ClaimUpdate>,
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
                info!("repo configuration changed; reopening");
                task.hub.repo_closed(&task.name, &task.id);
                backoff.reset();
                continue;
            }
            Err(err) => err,
        };
        warn!("repo watch failed: {err:#}");
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
        let present = crate::spawn_blocking(move || repo_present(&path))
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
    /// safe from async context; see the [`crate::repo::OpenRepo`] docs.
    async fn watch(&self) -> Result<()> {
        let (jj, repo, fingerprint) = self.open().await?;
        // Fetch serving is dispatched by the hub, never by this loop: a
        // fetch below may block on the very peer being served.
        self.hub.repo_opened(&self.name, &self.id, repo.clone());

        // Watch before the first read: changes racing the setup produce at
        // worst a no-change wakeup.
        let heads_dir = jj.op_heads_dir();
        let mut watch = DirWatcher::new(&heads_dir, DEBOUNCE, DEBOUNCE_MAX)?;
        let mut heads = sorted_heads(&repo).await?;
        let mut last_change = None;
        let mut last_sync = None;

        info!(path = %self.path.display(), "watching repo");
        // Publishing on watch start doubles as anti-entropy: changes made
        // while the watch was down are absorbed into the baseline above and
        // would otherwise never be announced.
        self.hub.publish(&self.name, &self.id, wire_heads(&heads));
        // Heal heads left without a commit index (a fetch whose index
        // build failed, or a repo synced by an older jj-mesh) before any
        // jj run below pays for the rebuild.
        self.heal_index(&repo, &heads).await;
        self.heal_git_refs(&repo).await;

        // The op-heads watch above is already live, so any operation the
        // workspace tasks create is picked up like any other.
        let ctx = Arc::new(RepoContext::new(
            repo.clone(),
            self.settings.for_repo(&self.name),
        ));
        let (synced, _) = watch::channel(None);
        let mut claims = self.claims.clone();
        let mut workspaces = Workspaces::default();
        let current = claims.borrow_and_update().clone();
        self.track_workspaces(&jj, &ctx, &heads, &synced, &current, &mut workspaces)
            .await;
        self.set_state(RepoState::Watching {
            op_heads: heads.len(),
            last_change,
            last_sync,
            workspaces: workspaces.listed.clone(),
        });

        // When set, the time to wake and retry fetches that failed and were
        // requeued into the inbox. Requeued heads are re-drained on any
        // wake, so this is only a fallback that fires when nothing else
        // would wake the task first.
        let mut retry_at: Option<Instant> = None;

        loop {
            let mut rescan = false;
            tokio::select! {
                changed = watch.changed_or_idle(LIVENESS_INTERVAL) => {
                    if !changed? {
                        // No events for a while: check the watch is not
                        // dead in a way that produces none (unmount).
                        ensure!(heads_dir.is_dir(), "the op heads directory is gone");
                        rescan = true;
                    }
                }
                () = self.announcements.changed() => {}
                Ok(()) = claims.changed() => rescan = true,
                () = sleep_until(retry_at) => {}
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
                // Synced operations were logged by their fetch; a local
                // change landing in the same wake only shows at debug.
                if drained.synced {
                    debug!(op_heads = heads.len(), "op heads changed");
                } else {
                    info!("announcing local change");
                }
                self.hub.publish(&self.name, &self.id, wire_heads(&heads));
                rescan = true;
            }
            if rescan {
                let current = claims.borrow_and_update().clone();
                self.track_workspaces(&jj, &ctx, &heads, &synced, &current, &mut workspaces)
                    .await;
            }

            // The applied operations may have left the working copies
            // stale.
            if drained.synced {
                synced.send_replace(single_head(&heads));
            }
            self.set_state(RepoState::Watching {
                op_heads: heads.len(),
                last_change,
                last_sync,
                workspaces: workspaces.listed.clone(),
            });
        }
    }

    /// Opens the repo's stores. Opening is heavy (gix opens the git repo,
    /// the self-check reads whole views), so it runs on a blocking
    /// thread: a hung disk must stall this repo, not the daemon.
    async fn open(&self) -> Result<(JjRepo, Arc<OpenRepo>, StoreFingerprint)> {
        let path = self.path.clone();
        let (jj, repo, fingerprint) =
            crate::spawn_blocking(move || -> Result<(JjRepo, OpenRepo, _)> {
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

    /// Aligns the workspace tasks and this machine's claims with the
    /// workspaces found on disk.
    async fn track_workspaces(
        &self,
        jj: &JjRepo,
        ctx: &Arc<RepoContext>,
        heads: &[OperationId],
        synced: &watch::Sender<Option<OperationId>>,
        claims: &RepoClaims,
        workspaces: &mut Workspaces,
    ) {
        if workspaces.sent.as_ref() == Some(&claims.ours) {
            workspaces.sent = None;
        }
        let mut held = claims.ours.clone();
        held.extend(workspaces.sent.iter().flatten().cloned());
        let scan = match find_workspaces(jj, &ctx.repo, heads, claims, held).await {
            Ok(scan) => scan,
            Err(err) => return warn!("cannot list workspaces: {err:#}"),
        };

        if scan.claimed != claims.ours && workspaces.sent.as_ref() != Some(&scan.claimed) {
            let _ = self.claim.send(ClaimUpdate {
                repo: self.id.clone(),
                names: scan.claimed.clone(),
            });
            workspaces.sent = Some(scan.claimed.clone());
        }

        let automated = |found: &Found| {
            found.state == WorkspaceState::Claimed && claims.ours.contains(&found.name)
        };
        workspaces.tasks.retain(|root, handle| {
            let keep = !handle.is_finished()
                && scan.found.iter().any(|found| {
                    found.workspace.root() == root && found.name == handle.name && automated(found)
                });
            if !keep {
                info!(workspace = %handle.name, "stopping workspace watch");
            }
            keep
        });
        workspaces.listed = scan.listed();
        for found in scan.found {
            if !automated(&found) {
                continue;
            }
            let root = found.workspace.root().to_owned();
            workspaces.tasks.entry(root).or_insert_with(|| {
                info!(
                    workspace = %found.name,
                    path = %found.workspace.root().display(), "watching workspace",
                );
                spawn_workspace(
                    ctx.clone(),
                    found.name,
                    found.workspace,
                    single_head(heads),
                    synced.subscribe(),
                )
            });
        }
    }

    /// Drains the announcement inbox, handling every entry. Failed
    /// fetches are requeued (a newer announcement or a reconnect
    /// supersedes them) and reported for a retry wakeup.
    async fn drain_announcements(&self, repo: &Arc<OpenRepo>) -> Result<Drained> {
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
        repo: &Arc<OpenRepo>,
        announce: &PeerAnnounce,
    ) -> Result<Handled> {
        let peer = self.hub.peer_name(&announce.peer);
        let id_len = repo.root_operation_id().as_bytes().len();
        if announce.heads.len() > MAX_ANNOUNCED_HEADS
            || announce.heads.iter().any(|head| head.len() != id_len)
        {
            debug!(%peer, "ignoring malformed announcement");
            return Ok(Handled::Idle);
        }
        // Runs on a blocking thread: the check may walk the op log.
        let heads: Vec<OperationId> = announce
            .heads
            .iter()
            .map(|head| OperationId::new(head.clone()))
            .collect();
        let missing = {
            let repo = repo.clone();
            crate::spawn_blocking(move || repo.missing_heads(&heads))
                .await
                .wrap_err("announcement check task failed")?
        };
        let missing = match missing {
            Ok(missing) => missing,
            // Retried like a failed fetch.
            Err(err) => {
                warn!(%peer, "cannot check announcement: {err:#}");
                return Ok(Handled::Failed);
            }
        };
        if missing.is_empty() {
            debug!(%peer, "in sync with peer");
            return Ok(Handled::Idle);
        }

        // Sync failures must not kill the watch: the repo is fine, the peer
        // or network is not. The caller requeues for a later retry.
        match self.fetch_missing(repo, announce.peer, &missing).await {
            Ok(outcome) => {
                info!(
                    %peer,
                    ops = outcome.ops, objects = outcome.git_objects,
                    "synced from peer",
                );
                Ok(Handled::Fetched)
            }
            Err(err) => {
                warn!(%peer, "sync failed: {err:#}");
                Ok(Handled::Failed)
            }
        }
    }

    /// Fetches missing op heads from the announcing peer over a fresh
    /// bidirectional stream.
    async fn fetch_missing(
        &self,
        repo: &Arc<OpenRepo>,
        peer: EndpointId,
        wants: &[OperationId],
    ) -> Result<transfer::FetchOutcome> {
        let conn = self
            .hub
            .connection(&peer)
            .ok_or_else(|| eyre!("peer is no longer connected"))?;
        let (mut send, mut recv) = tokio::time::timeout(OPEN_STREAM_TIMEOUT, conn.open_bi())
            .await
            .map_err(|_| eyre!("timed out opening the fetch stream"))??;
        let outcome = transfer::fetch(
            repo,
            transfer::RepoIdent {
                name: &self.name,
                id: &self.id,
            },
            wants,
            transfer::FetchOptions {
                format: GitTransferFormat::Loose,
                net_timeout: FETCH_NET_TIMEOUT,
            },
            &mut send,
            &mut recv,
            transfer::ProgressSink::default(),
        )
        .await?;
        let _ = send.finish();
        Ok(outcome)
    }

    /// Builds the commit index for any op head missing one, on a blocking
    /// thread, showing the repo as `Indexing` meanwhile. Normally a no-op
    /// (fetches index before publishing): this heals heads published
    /// without an index, which would otherwise silently stall the user's
    /// next jj command on a rebuild. Failures only warn; the next watch
    /// start retries.
    async fn heal_index(&self, repo: &Arc<OpenRepo>, heads: &[OperationId]) {
        let missing = repo.unindexed(heads).await;
        if missing.is_empty() {
            return;
        }
        info!(heads = missing.len(), "building the commit index");
        self.set_state(RepoState::Indexing);
        let build = {
            let repo = repo.clone();
            crate::spawn_blocking(move || repo.build_commit_indexes(&missing))
        };
        if let Err(err) = build.await {
            warn!("index build task failed: {err}");
        }
    }

    /// Brings the git refs of a repo whose git store only jj writes in
    /// line with the synced views. Failures only warn; the next watch
    /// start retries.
    async fn heal_git_refs(&self, repo: &Arc<OpenRepo>) {
        let heal = {
            let repo = repo.clone();
            crate::spawn_blocking(move || transfer::mirror::heal(&repo))
        };
        match heal.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!("git ref repair failed: {err:#}"),
            Err(err) => warn!("git ref repair task failed: {err}"),
        }
    }

    fn set_state(&self, state: RepoState) {
        *self.state.lock().unwrap() = state;
        self.changed.notify_one();
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

/// Re-captures the store fingerprint: a handful of tiny reads, but on a
/// blocking thread since a hung mount is one of the probed conditions.
async fn store_fingerprint(jj: &JjRepo) -> Result<StoreFingerprint> {
    let jj = jj.clone();
    crate::spawn_blocking(move || jj.fingerprint())
        .await
        .wrap_err("fingerprint task failed")?
}

/// The workspace tasks of one repo watch, and what status shows of its
/// local workspaces.
#[derive(Default)]
struct Workspaces {
    tasks: BTreeMap<PathBuf, WorkspaceHandle>,
    listed: Vec<WorkspaceStatus>,
    /// The claims last sent, until the store records them.
    sent: Option<BTreeSet<String>>,
}

/// The outcome of one workspace scan.
struct Scan {
    /// The local workspaces the view names.
    found: Vec<Found>,
    /// The names this machine claims, found or not.
    claimed: BTreeSet<String>,
}

/// A local workspace, and whether this machine claims it.
struct Found {
    name: String,
    workspace: Workspace,
    state: WorkspaceState,
}

impl Scan {
    /// The local workspaces as status shows them: those found, then the
    /// claimed ones whose directory is not.
    fn listed(&self) -> Vec<WorkspaceStatus> {
        let found = self.found.iter().map(|found| WorkspaceStatus {
            name: found.name.clone(),
            place: WorkspacePlace::Local {
                path: found.workspace.root().to_owned(),
                state: found.state.clone(),
            },
        });
        let missing = self
            .claimed
            .iter()
            .filter(|name| !self.found.iter().any(|found| found.name == **name))
            .map(|name| WorkspaceStatus {
                name: name.clone(),
                place: WorkspacePlace::Missing,
            });
        found.chain(missing).collect()
    }
}

/// Finds the local workspaces the views of `heads` name and decides the
/// claims on them (see the module docs), `held` being the names claimed
/// so far. Runs on a blocking thread: decodes a view per head, and two per
/// freshness check.
async fn find_workspaces(
    jj: &JjRepo,
    repo: &Arc<OpenRepo>,
    heads: &[OperationId],
    claims: &RepoClaims,
    held: BTreeSet<String>,
) -> Result<Scan> {
    let (jj, repo, heads, claims) = (jj.clone(), repo.clone(), heads.to_vec(), claims.clone());
    crate::spawn_blocking(move || {
        let names = repo.workspace_names(&heads).block_on()?;
        let single = single_head(&heads);
        let fresh = |workspace: &Workspace| {
            single.as_ref().is_some_and(|head| {
                matches!(may_be_stale(workspace, &repo, head).block_on(), Ok(false))
            })
        };

        let mut claimed: BTreeSet<String> = held.intersection(&names).cloned().collect();
        let mut found = Vec::new();
        for (name, workspace) in jj.workspaces(&names)? {
            let state = match claims.others.get(&name) {
                Some(machines) if claimed.contains(&name) => WorkspaceState::Contested {
                    machines: machines.clone(),
                },
                Some(machines) => WorkspaceState::Foreign {
                    machines: machines.clone(),
                },
                None if claimed.contains(&name) => WorkspaceState::Claimed,
                None if validate_name("workspace", &name).is_err()
                    || claimed.len() >= claims.limit =>
                {
                    WorkspaceState::Unclaimable
                }
                None if workspace.is_main() || fresh(&workspace) => {
                    claimed.insert(name.clone());
                    WorkspaceState::Claimed
                }
                None => WorkspaceState::Stale,
            };
            found.push(Found {
                name,
                workspace,
                state,
            });
        }
        Ok(Scan { found, claimed })
    })
    .await
    .wrap_err("workspace search task failed")?
}

/// The op head, when single: working copies are only caught up on then
/// (see the `workspace` module docs on divergence).
fn single_head(heads: &[OperationId]) -> Option<OperationId> {
    match heads {
        [head] => Some(head.clone()),
        _ => None,
    }
}

/// Reads the current op heads as a sorted set, comparable across reads.
async fn sorted_heads(repo: &OpenRepo) -> Result<Vec<OperationId>> {
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

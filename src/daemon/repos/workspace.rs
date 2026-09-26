//! The per-workspace task: auto-snapshot and update-stale for one working
//! copy of a watched repo.
//!
//! Workspace tasks share their repo task's open stores (see
//! [`RepoContext`]) and learn of every sync that applied operations
//! through [`Synced`]. A task whose working copy watch dies stops, and the
//! repo task's next workspace scan respawns it.
//!
//! When auto-snapshotting is enabled, the task watches the working copy
//! files and snapshots them on the cadence [`Snapshotting`] sets. The
//! snapshot runs through the jj binary and produces a regular operation,
//! which the repo task's op-heads watch then picks up and announces like
//! any local change.
//!
//! Syncing operations from peers can leave the working copy stale (updated
//! by an operation the working copy never saw). When enabled, `jj workspace
//! update-stale` runs after every sync that applied operations, and once on
//! task start for staleness accrued while the daemon was down, but only
//! while the op head is single and the head moved the working-copy commit
//! since the working copy's last update: the command snapshots the whole
//! working copy before checking anything, so it must not run on syncs that
//! cannot have made it stale. Any jj command reconciles divergent op heads
//! by writing a merge operation, so daemons doing this on both ends of a
//! divergence would ping-pong fresh merge operations at each other.
//! Divergence is left to the next actual jj activity (a user command, an
//! auto-snapshot), whose merge then syncs as a single head.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use color_eyre::eyre::Result;
use jj_lib::op_store::OperationId;
use pollster::FutureExt as _;
use tokio::sync::{Mutex, watch};
use tracing::{debug, warn};

use super::sleep_until;
use crate::{
    config::RepoSettings,
    repo::{OpenRepo, Workspace, run_jj},
    watch::TreeWatcher,
};

/// Budget for one spawned jj command. Generous: the first snapshot of a
/// large working copy legitimately takes a while.
const JJ_TIMEOUT: Duration = Duration::from_mins(5);

/// The op head after the last sync that applied operations, `None` when
/// divergent.
pub(super) type Synced = watch::Receiver<Option<OperationId>>;

/// What the workspace tasks of one repo share.
pub(super) struct RepoContext {
    /// The repo's mesh-wide name.
    name: String,
    pub repo: Arc<OpenRepo>,
    settings: RepoSettings,
    /// Serializes the jj runs of all workspaces: concurrent ones would
    /// each write an operation on the same head, diverging the op log.
    jj: Mutex<()>,
}

impl RepoContext {
    pub fn new(name: String, repo: Arc<OpenRepo>, settings: RepoSettings) -> Self {
        RepoContext {
            name,
            repo,
            settings,
            jj: Mutex::new(()),
        }
    }
}

/// A running workspace task, aborted on drop.
#[derive(Debug)]
pub(super) struct WorkspaceHandle {
    pub name: String,
    task: tokio::task::JoinHandle<()>,
}

impl WorkspaceHandle {
    /// Whether the task stopped on a failed working copy watch.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

impl Drop for WorkspaceHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Spawns the task of workspace `name`, first catching up on staleness at
/// `head`, the current single op head.
pub(super) fn spawn_workspace(
    ctx: Arc<RepoContext>,
    name: String,
    workspace: Workspace,
    head: Option<OperationId>,
    synced: Synced,
) -> WorkspaceHandle {
    let task = WorkspaceTask {
        ctx,
        name: name.clone(),
        workspace,
        synced,
    };
    WorkspaceHandle {
        name,
        task: tokio::spawn(task.run(head)),
    }
}

/// Everything a workspace task owns.
struct WorkspaceTask {
    ctx: Arc<RepoContext>,
    name: String,
    workspace: Workspace,
    synced: Synced,
}

impl WorkspaceTask {
    /// Keeps the working copy fresh until the repo watch ends or the
    /// working copy watch fails.
    async fn run(mut self, head: Option<OperationId>) {
        if let Err(err) = self.keep_fresh(head).await {
            warn!(
                repo = %self.ctx.name, workspace = %self.name,
                "working copy watch failed: {err:#}",
            );
        }
    }

    /// Runs the auto-snapshot and update-stale loop. Errors when the
    /// working copy watch dies.
    async fn keep_fresh(&mut self, head: Option<OperationId>) -> Result<()> {
        let interval = self.ctx.settings.snapshot_interval;
        let mut snap = Snapshotting::default();
        let mut tree = match interval {
            Some(_) => self.watch_tree(self.workspace.root()).await,
            None => None,
        };

        if let Some(head) = &head {
            self.update_stale(head, &mut tree).await?;
        }
        // Edits made while the watch was down produce no event, so the
        // working copy is snapshotted once on start: it is the only
        // anti-entropy the snapshot path has.
        snap.arm(interval);

        loop {
            tokio::select! {
                changed = self.synced.changed() => {
                    let Ok(()) = changed else {
                        return Ok(());
                    };
                    let head = self.synced.borrow_and_update().clone();
                    if let Some(head) = head
                        && self.update_stale(&head, &mut tree).await?
                    {
                        // update-stale snapshots the working copy itself,
                        // so a pending snapshot has just been done.
                        snap.done();
                    }
                }
                changed = tree_changed(&mut tree) => {
                    changed?;
                    snap.arm(interval);
                }
                () = sleep_until(snap.deadline) => self.snapshot(&mut snap, &mut tree).await?,
            }
        }
    }

    /// Builds the working-copy watcher, degrading to `None` instead of
    /// failing: `None` means no snapshots for this workspace, nothing else.
    async fn watch_tree(&self, root: &Path) -> Option<TreeWatcher> {
        match TreeWatcher::new(root).await {
            Ok(tree) => Some(tree),
            Err(err) => {
                warn!(
                    repo = %self.ctx.name, workspace = %self.name,
                    "cannot watch working copy files, auto-snapshot disabled: {err:#}",
                );
                None
            }
        }
    }

    /// Runs `jj workspace update-stale` when enabled and the working copy
    /// may be stale at `head`; returns whether it ran.
    async fn update_stale(
        &self,
        head: &OperationId,
        tree: &mut Option<TreeWatcher>,
    ) -> Result<bool> {
        if !self.ctx.settings.update_stale {
            return Ok(false);
        }
        // Blocking: reads the checkout file and two views.
        let stale = {
            let (workspace, repo, head) =
                (self.workspace.clone(), self.ctx.repo.clone(), head.clone());
            tokio::task::spawn_blocking(move || may_be_stale(&workspace, &repo, &head).block_on())
                .await
                .unwrap_or_else(|err| Err(err.into()))
        };
        match stale {
            Ok(false) => return Ok(false),
            Ok(true) => {}
            Err(err) => debug!(
                repo = %self.ctx.name, workspace = %self.name,
                "cannot check staleness: {err:#}",
            ),
        }
        debug!(repo = %self.ctx.name, workspace = %self.name, "checking for a stale working copy");
        self.run_jj(&["workspace", "update-stale"], tree).await?;
        Ok(true)
    }

    /// Snapshots the working copy through the jj binary, which applies
    /// the user's snapshot configuration and takes the working-copy lock.
    async fn snapshot(
        &self,
        snap: &mut Snapshotting,
        tree: &mut Option<TreeWatcher>,
    ) -> Result<()> {
        debug!(repo = %self.ctx.name, workspace = %self.name, "snapshotting working copy");
        let started = Instant::now();
        self.run_jj(&["util", "snapshot"], tree).await?;
        snap.finished(started);
        Ok(())
    }

    /// Runs one working-copy jj command, then drops the events it caused:
    /// update-stale writes working-copy files, and letting the watcher
    /// see them would schedule a snapshot of the daemon's own work, on
    /// and on. A failed command only warns: the working copy may be locked
    /// by an ongoing command, and the next trigger retries.
    async fn run_jj(&self, args: &[&str], tree: &mut Option<TreeWatcher>) -> Result<()> {
        let result = {
            let _serial = self.ctx.jj.lock().await;
            run_jj(self.workspace.root(), args, JJ_TIMEOUT).await
        };
        if let Err(err) = result {
            warn!(
                repo = %self.ctx.name, workspace = %self.name,
                "jj {} failed: {err:#}", args.join(" "),
            );
        }
        match tree {
            Some(watcher) => watcher.discard_queued().await,
            None => Ok(()),
        }
    }
}

/// Scheduling state of one workspace's auto-snapshots.
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

/// Waits for the next working-copy change, or forever when tree watching
/// is disabled, so it can sit in a `select!` uniformly.
async fn tree_changed(tree: &mut Option<TreeWatcher>) -> Result<()> {
    match tree {
        Some(tree) => tree.changed().await,
        None => std::future::pending().await,
    }
}

/// Whether the working copy may be stale at the op head `head`: it was
/// last updated at another operation, one whose view gave its workspace a
/// different working-copy commit. Same commit means same tree, which jj
/// treats as fresh, whatever else the operations changed.
pub(super) async fn may_be_stale(
    workspace: &Workspace,
    repo: &OpenRepo,
    head: &OperationId,
) -> Result<bool> {
    let checkout = workspace.checkout()?;
    if &checkout.operation == head {
        return Ok(false);
    }
    let at_checkout = repo
        .wc_commit_id(&checkout.operation, &checkout.workspace)
        .await?;
    let at_head = repo.wc_commit_id(head, &checkout.workspace).await?;
    Ok(at_checkout != at_head)
}

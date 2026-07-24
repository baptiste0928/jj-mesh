//! Registered repo management.
//!
//! One task per registered repo opens it and watches its op-heads directory:
//! every mutating jj command atomically swaps head marker files there, so a
//! change event means new operations to announce. Announcing to peers plugs
//! in on top of the change signal; for now the task tracks the head set and
//! reports it on the control socket.
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

use color_eyre::eyre::{Result, WrapErr as _, ensure};
use jj_lib::op_store::OperationId;
use tracing::{info, warn};

use super::control;
use crate::{
    config::{Config, RepoId},
    repo::{JjRepo, MeshRepo},
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

/// The set of managed repos, synced from the configuration.
#[derive(Debug, Default)]
pub struct RepoSet {
    repos: Mutex<BTreeMap<RepoId, RepoHandle>>,
}

/// Book-keeping for one repo task.
#[derive(Debug)]
struct RepoHandle {
    name: String,
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
    },
    Backoff {
        until: Instant,
        error: String,
    },
}

impl RepoSet {
    pub fn new() -> Self {
        Self::default()
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
                return false;
            };

            if handle.path != *path {
                info!(repo = %handle.name, "repo moved, restarting its watch");
                handle.task.abort();
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
                spawn_repo(name.to_owned(), path.to_owned())
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
                    } => control::WatchStatus::Watching {
                        op_heads: *op_heads as u64,
                        last_change_secs: last_change.map(|at| at.elapsed().as_secs()),
                    },
                    RepoState::Backoff { until, error } => control::WatchStatus::Failed {
                        error: error.clone(),
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                };

                control::RepoStatus {
                    name: handle.name.clone(),
                    path: handle.path.clone(),
                    watch,
                }
            })
            .collect()
    }
}

/// Spawns the watch task for one repo.
fn spawn_repo(name: String, path: PathBuf) -> RepoHandle {
    let state = Arc::new(Mutex::new(RepoState::Opening));

    let task = tokio::spawn(run_repo(RepoTask {
        name: name.clone(),
        path: path.clone(),
        state: state.clone(),
    }));

    RepoHandle {
        name,
        path,
        state,
        task,
    }
}

/// Everything a repo task owns.
struct RepoTask {
    name: String,
    path: PathBuf,
    state: Arc<Mutex<RepoState>>,
}

/// Opens and watches one repo forever, reopening with backoff on failure.
async fn run_repo(task: RepoTask) {
    let mut backoff = BACKOFF_MIN;

    loop {
        task.set_state(RepoState::Opening);
        let started = Instant::now();

        let err = task.watch().await.unwrap_err();
        warn!(repo = %task.name, "repo watch failed: {err:#}");

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
    /// Watches the repo's op heads until something fails.
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

        // Watch before the first read: changes racing the setup produce at
        // worst a no-change wakeup.
        let heads_dir = jj.op_heads_dir();
        let mut watch = DirWatcher::new(&heads_dir, DEBOUNCE, DEBOUNCE_MAX, |_| true)?;
        let mut heads = sorted_heads(&repo).await?;
        let mut last_change = None;

        info!(repo = %self.name, "watching repo");
        self.set_state(RepoState::Watching {
            op_heads: heads.len(),
            last_change,
        });

        loop {
            if !watch.changed_or_idle(LIVENESS_INTERVAL).await? {
                // No events for a while: check the watch is not dead in a
                // way that produces none (unmount).
                ensure!(heads_dir.is_dir(), "the op heads directory is gone");
                continue;
            }

            let new = sorted_heads(&repo).await?;
            if new == heads {
                continue;
            }
            heads = new;
            last_change = Some(Instant::now());

            info!(repo = %self.name, op_heads = heads.len(), "op heads changed");
            // Announcing the new heads to peers plugs in here. Note that
            // changes made while the watch was down are absorbed into the
            // baseline above, so the sync layer must additionally announce
            // on every watch start and peer (re)connect, not rely on this
            // in-loop signal alone.

            self.set_state(RepoState::Watching {
                op_heads: heads.len(),
                last_change,
            });
        }
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

        let set = RepoSet::new();
        set.sync(&config_with("a", &dir));

        wait_for(&set, |s| {
            matches!(
                s,
                [control::RepoStatus {
                    watch: control::WatchStatus::Watching {
                        op_heads: 1,
                        last_change_secs: None,
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

        let set = RepoSet::new();
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
        let set = RepoSet::new();
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

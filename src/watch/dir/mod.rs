//! [`DirWatcher`]: a debounced non-recursive watch on a single directory.
//!
//! Events come from a per-platform source reporting [`Signal`]s: kqueue
//! on macOS, notify elsewhere.

#[cfg(target_os = "macos")]
mod kqueue;
#[cfg(not(target_os = "macos"))]
mod notify;

use std::{path::Path, time::Duration};

use anyhow::{Result, bail};
use tokio::time::Instant;

#[cfg(target_os = "macos")]
use self::kqueue::Events;
#[cfg(not(target_os = "macos"))]
use self::notify::Events;
use super::backend;

/// A change carries no payload here; only its arrival matters.
type Signal = backend::Signal<()>;

/// Why a watch died when its directory went away.
const DEATH: &str = "the watched directory was removed or moved";

/// A debounced watch on one directory.
pub struct DirWatcher {
    events: Events,
    debounce: Duration,
    debounce_max: Duration,
    /// Cap of the burst being debounced, if a change was received but not
    /// yet reported: [`changed_or_idle`](Self::changed_or_idle) dropped
    /// mid-debounce resumes here instead of losing the change.
    burst: Option<Instant>,
}

impl DirWatcher {
    /// Starts watching `dir` (non-recursively). Any event except access
    /// counts as a change.
    ///
    /// `debounce` is the quiet window required before reporting a change,
    /// capped at `debounce_max` total so a busy writer cannot starve the
    /// caller.
    pub fn new(dir: &Path, debounce: Duration, debounce_max: Duration) -> Result<Self> {
        Ok(DirWatcher {
            events: Events::new(dir)?,
            debounce,
            debounce_max,
            burst: None,
        })
    }

    /// Waits for the next change, debouncing event bursts, but gives up
    /// after `idle` without an event and returns `Ok(false)`, letting the
    /// caller run a liveness check: some watch deaths (unmounts) produce no
    /// event at all. Errors when the watch is dead and must be rebuilt.
    ///
    /// Cancel-safe: a change received before the future was dropped is
    /// reported by the next call.
    pub async fn changed_or_idle(&mut self, idle: Duration) -> Result<bool> {
        let deadline = if let Some(deadline) = self.burst {
            deadline
        } else {
            let Ok(signal) = tokio::time::timeout(idle, self.events.recv()).await else {
                return Ok(false);
            };
            check(signal)?;
            *self.burst.insert(Instant::now() + self.debounce_max)
        };
        // Wait out the burst, bounded by the cap.
        loop {
            match tokio::time::timeout(self.debounce, self.events.recv()).await {
                Ok(Some(Signal::Event(()))) if Instant::now() < deadline => {}
                Ok(Some(Signal::Failed(msg))) => bail!(msg),
                _ => break,
            }
        }
        self.burst = None;
        Ok(true)
    }
}

/// Turns a received signal into the watch's fate.
fn check(signal: Option<Signal>) -> Result<()> {
    match signal {
        Some(Signal::Event(())) => Ok(()),
        Some(Signal::Failed(msg)) => bail!(msg),
        None => bail!("filesystem watcher stopped"),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::testing;

    const DEBOUNCE: Duration = Duration::from_millis(50);
    const DEBOUNCE_MAX: Duration = Duration::from_millis(500);

    async fn watch(dir: &Path) -> DirWatcher {
        testing::watch::dir(dir, DEBOUNCE, DEBOUNCE_MAX).await
    }

    #[tokio::test]
    async fn reports_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut watch = watch(tmp.path()).await;

        fs::write(tmp.path().join("file"), "x").unwrap();

        let changed = watch.changed_or_idle(Duration::from_secs(5)).await.unwrap();
        assert!(changed, "no change detected");
    }

    #[tokio::test]
    async fn reports_watched_dir_removal_as_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("watched");
        fs::create_dir(&dir).unwrap();
        let mut watch = watch(&dir).await;

        fs::remove_dir(&dir).unwrap();

        let outcome = watch.changed_or_idle(Duration::from_secs(5)).await;
        assert!(outcome.is_err(), "removal must surface as a watch error");
    }

    #[tokio::test]
    async fn reports_watched_dir_move_as_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("watched");
        fs::create_dir(&dir).unwrap();
        let mut watch = watch(&dir).await;

        fs::rename(&dir, tmp.path().join("moved")).unwrap();

        let outcome = watch.changed_or_idle(Duration::from_secs(5)).await;
        assert!(outcome.is_err(), "a move must surface as a watch error");
    }
}

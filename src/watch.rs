//! Debounced directory watching, used by the per-repo op-heads watchers.
//!
//! Conditions that silently kill a notify watch are turned into errors so
//! callers can rebuild it instead of waiting on a dead channel forever:
//! removing the watched directory drops the kernel watch (with a final
//! event), moving it leaves the watch on the old inode, and backend errors
//! are only reported through the callback. Unmounts produce no event at
//! all; [`DirWatcher::changed_or_idle`] lets callers run their own
//! periodic liveness checks for that case.

use std::{path::Path, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _, bail};
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as _, event::ModifyKind,
};
use tokio::sync::mpsc;

/// What the notify callback reports to the async side.
#[derive(Debug)]
enum Signal {
    Changed,
    Failed(String),
}

/// A debounced watch on one directory.
///
/// Dropping it stops notify's background threads.
#[derive(Debug)]
pub struct DirWatcher {
    _watcher: RecommendedWatcher,
    signals: mpsc::UnboundedReceiver<Signal>,
    debounce: Duration,
    debounce_max: Duration,
}

impl DirWatcher {
    /// Starts watching `dir` (non-recursively). Only events passing
    /// `filter` count as changes; access events never do (the watcher's own
    /// reads must not feed back into it).
    ///
    /// `debounce` is the quiet window required before reporting a change,
    /// capped at `debounce_max` total so a busy writer cannot starve the
    /// caller.
    pub fn new(
        dir: &Path,
        debounce: Duration,
        debounce_max: Duration,
        filter: impl Fn(&Event) -> bool + Send + 'static,
    ) -> Result<Self> {
        let (tx, signals) = mpsc::unbounded_channel();
        let watched = dir.to_owned();

        let mut backend = notify::recommended_watcher(move |event: notify::Result<Event>| {
            let signal = match &event {
                Err(err) => Some(Signal::Failed(format!("watch backend error: {err}"))),
                Ok(event) if is_watch_death(event, &watched) => Some(Signal::Failed(
                    "the watched directory was removed or moved".to_owned(),
                )),
                Ok(event) if !event.kind.is_access() && filter(event) => Some(Signal::Changed),
                Ok(_) => None,
            };
            if let Some(signal) = signal {
                let _ = tx.send(signal);
            }
        })
        .wrap_err("cannot create filesystem watcher")?;

        backend
            .watch(dir, RecursiveMode::NonRecursive)
            .wrap_err_with(|| format!("cannot watch {}", dir.display()))?;

        Ok(DirWatcher {
            _watcher: backend,
            signals,
            debounce,
            debounce_max,
        })
    }

    /// Waits for the next change, debouncing event bursts, but gives up
    /// after `idle` without an event and returns `Ok(false)`, letting the
    /// caller run a liveness check: some watch deaths (unmounts) produce no
    /// event at all. Errors when the watch is dead and must be rebuilt.
    pub async fn changed_or_idle(&mut self, idle: Duration) -> Result<bool> {
        let Ok(signal) = tokio::time::timeout(idle, self.signals.recv()).await else {
            return Ok(false);
        };
        Self::consume(signal)?;
        self.debounce().await?;
        Ok(true)
    }

    /// Interprets one received signal.
    fn consume(signal: Option<Signal>) -> Result<()> {
        match signal {
            Some(Signal::Changed) => Ok(()),
            Some(Signal::Failed(msg)) => bail!(msg),
            None => bail!("filesystem watcher stopped"),
        }
    }

    /// Waits out an event burst, bounded by the debounce cap.
    async fn debounce(&mut self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + self.debounce_max;
        loop {
            match tokio::time::timeout(self.debounce, self.signals.recv()).await {
                Ok(Some(Signal::Changed)) if tokio::time::Instant::now() < deadline => {}
                Ok(Some(Signal::Failed(msg))) => bail!(msg),
                _ => return Ok(()),
            }
        }
    }
}

/// Whether an event means the watch itself is dead: the watched directory
/// was removed (kernel drops the watch) or renamed (the watch follows the
/// old inode). Both surface as a self-referential event.
fn is_watch_death(event: &Event, watched: &Path) -> bool {
    matches!(
        event.kind,
        EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
    ) && event.paths.iter().any(|path| path == watched)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    const DEBOUNCE: Duration = Duration::from_millis(50);
    const DEBOUNCE_MAX: Duration = Duration::from_millis(500);

    #[tokio::test]
    async fn reports_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut watch = DirWatcher::new(tmp.path(), DEBOUNCE, DEBOUNCE_MAX, |_| true).unwrap();

        fs::write(tmp.path().join("file"), "x").unwrap();

        let changed = watch.changed_or_idle(Duration::from_secs(5)).await.unwrap();
        assert!(changed, "no change detected");
    }

    #[tokio::test]
    async fn reports_watched_dir_removal_as_error() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("watched");
        fs::create_dir(&dir).unwrap();
        let mut watch = DirWatcher::new(&dir, DEBOUNCE, DEBOUNCE_MAX, |_| true).unwrap();

        fs::remove_dir(&dir).unwrap();

        let outcome = watch.changed_or_idle(Duration::from_secs(5)).await;
        assert!(outcome.is_err(), "removal must surface as a watch error");
    }

    #[tokio::test]
    async fn idle_elapses_without_events() {
        let tmp = tempfile::tempdir().unwrap();
        let mut watch = DirWatcher::new(tmp.path(), DEBOUNCE, DEBOUNCE_MAX, |_| true).unwrap();

        let changed = watch
            .changed_or_idle(Duration::from_millis(50))
            .await
            .unwrap();
        assert!(!changed);
    }
}

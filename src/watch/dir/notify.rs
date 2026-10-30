//! notify-backed event source for [`DirWatcher`](super::DirWatcher), used
//! off macOS where registering a watch is a cheap syscall.

use std::path::Path;

use anyhow::{Context as _, Result};
use notify::{
    Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as _, event::ModifyKind,
};
use tokio::sync::mpsc;

use super::{DEATH, Signal};
use crate::watch::backend;

/// Signals from a notify watch on one directory.
///
/// Dropping it stops notify's background threads.
pub(super) struct Events {
    _watcher: RecommendedWatcher,
    signals: mpsc::UnboundedReceiver<Signal>,
}

impl Events {
    pub(super) fn new(dir: &Path) -> Result<Self> {
        let (tx, signals) = mpsc::unbounded_channel();
        let target = dir.to_owned();
        let mut watcher = backend::watcher(
            move |event| {
                Some(if is_watch_death(&event, &target) {
                    Signal::Failed(DEATH.to_owned())
                } else {
                    Signal::Event(())
                })
            },
            move |signal| {
                let _ = tx.send(signal);
            },
        )?;
        watcher
            .watch(dir, RecursiveMode::NonRecursive)
            .with_context(|| format!("cannot watch {}", dir.display()))?;
        Ok(Events {
            _watcher: watcher,
            signals,
        })
    }

    /// Waits for the next signal. Cancel-safe.
    pub(super) async fn recv(&mut self) -> Option<Signal> {
        self.signals.recv().await
    }
}

/// Whether an event means the watch itself is dead: the watched directory
/// was removed (kernel drops the watch) or renamed (the watch follows the
/// old inode). Both surface as a self-referential event, reported under
/// the path the watch was registered with.
fn is_watch_death(event: &Event, watched: &Path) -> bool {
    matches!(
        event.kind,
        EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
    ) && event.paths.iter().any(|path| path == watched)
}

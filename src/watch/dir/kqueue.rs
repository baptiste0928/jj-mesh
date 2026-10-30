//! kqueue-backed event source for [`DirWatcher`](super::DirWatcher) on
//! macOS. notify's backend there, FSEvents, registers every watch with
//! `fseventsd` (hundreds of milliseconds, serialized system-wide); a vnode
//! filter on the directory's own descriptor is a couple of syscalls, and
//! the runtime polls the queue directly, without a thread.
//!
//! A directory's vnode events cover entries being added, removed or
//! renamed, which is all op heads do; edits to existing files would go
//! unseen.

use std::{fs::File, io, os::unix::fs::OpenOptionsExt as _, path::Path};

use anyhow::{Context as _, Result};
use kqueue::{EventData, EventFilter, FilterFlag, Vnode, Watcher};
use tokio::io::{Interest, unix::AsyncFd};

use super::{DEATH, Signal};

/// Signals from a kqueue watch on one directory. Dropping it closes the
/// queue, then the directory.
pub(super) struct Events {
    queue: AsyncFd<Watcher>,
    _dir: File,
}

impl Events {
    pub(super) fn new(dir: &Path) -> Result<Self> {
        let flags = FilterFlag::NOTE_WRITE
            | FilterFlag::NOTE_EXTEND
            | FilterFlag::NOTE_ATTRIB
            | FilterFlag::NOTE_LINK
            | FilterFlag::NOTE_DELETE
            | FilterFlag::NOTE_RENAME
            | FilterFlag::NOTE_REVOKE;
        // An event-only descriptor does not keep the volume busy: it can
        // still be ejected, which revokes the watch.
        let file = File::options()
            .read(true)
            .custom_flags(libc::O_EVTONLY)
            .open(dir)
            .with_context(|| format!("cannot open {}", dir.display()))?;
        let mut watcher = Watcher::new().context("cannot create kqueue")?;
        watcher
            .add_file(&file, EventFilter::EVFILT_VNODE, flags)
            .and_then(|()| watcher.watch())
            .with_context(|| format!("cannot watch {}", dir.display()))?;
        // A kqueue descriptor only supports read readiness: registering
        // write interest fails. SAFETY: the watcher owns its queue
        // descriptor for its whole lifetime and never replaces it.
        let queue = unsafe { AsyncFd::register_with_interest(watcher, Interest::READABLE) }
            .map_err(io::Error::from)
            .context("cannot poll kqueue")?;
        Ok(Events { queue, _dir: file })
    }

    /// Waits for the next signal. Cancel-safe: events stay queued in the
    /// kernel until read, and a read event is returned immediately.
    pub(super) async fn recv(&mut self) -> Option<Signal> {
        loop {
            let mut ready = match self.queue.readable().await {
                Ok(ready) => ready,
                Err(err) => return Some(Signal::Failed(format!("watch backend error: {err}"))),
            };
            let Some(event) = self.queue.get_ref().poll(None) else {
                ready.clear_ready();
                continue;
            };
            return Some(match event.data {
                // The descriptor follows the inode, so a move kills the
                // watch like a removal.
                EventData::Vnode(Vnode::Delete | Vnode::Rename) => Signal::Failed(DEATH.to_owned()),
                EventData::Vnode(Vnode::Revoke) => {
                    Signal::Failed("the watched directory was unmounted".to_owned())
                }
                // A signal interrupted the read; nothing was lost.
                EventData::Error(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                EventData::Error(err) => Signal::Failed(format!("watch backend error: {err}")),
                _ => Signal::Event(()),
            });
        }
    }
}

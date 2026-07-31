//! Filesystem watching: [`DirWatcher`] for one directory (op heads),
//! [`TreeWatcher`] for a whole working copy with ignore rules.
//!
//! Conditions that silently kill a notify watch are turned into errors so
//! callers can rebuild it instead of waiting on a dead channel forever:
//! removing the watched directory drops the kernel watch (with a final
//! event), moving it leaves the watch on the old inode, and backend errors
//! are only reported through the callback. Unmounts produce no event at
//! all; [`DirWatcher::changed_or_idle`] lets callers run their own
//! periodic liveness checks for that case.

mod backend;
mod dir;
mod tree;

pub use dir::DirWatcher;
pub use tree::TreeWatcher;

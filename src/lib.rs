//! jj-mesh is a peer-to-peer sync service for personal jj repositories. It
//! syncs op log and git objects across machines to instantly replicate changes.
//!
//! Machines are connected peer-to-peer using `iroh`.
//!
//! This crate hosts both the management CLI and the sync daemon, both exposed
//! as a single `jj-mesh` binary.

#![warn(clippy::pedantic)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]

pub mod cli;
pub mod config;
pub mod daemon;
pub mod net;
pub mod repo;
pub(crate) mod watch;

/// [`tokio::task::spawn_blocking`] in the caller's tracing span, so the
/// blocking work logs with its context (repo, workspace).
pub(crate) fn spawn_blocking<F, R>(f: F) -> tokio::task::JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let span = tracing::Span::current();
    #[allow(clippy::disallowed_methods)]
    tokio::task::spawn_blocking(move || span.in_scope(f))
}

#[cfg(any(test, feature = "test-util"))]
pub mod testing;

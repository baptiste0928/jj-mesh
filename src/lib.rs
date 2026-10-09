//! jj-mesh is a peer-to-peer sync service for personal jj repositories. It
//! replicates the op log and the git objects it references across machines,
//! connected with iroh, without a central server ever holding the data.
//!
//! This crate hosts both the management CLI and the sync daemon, both exposed
//! as a single `jj-mesh` binary:
//!
//! ```text
//! ┌─────┐ control  ┌────────┐    iroh (QUIC)    ┌──────────────┐
//! │ CLI │──socket─►│ daemon │◄─────────────────►│ peer daemons │
//! └─────┘          └───┬────┘                   └──────────────┘
//!               watch  │  sync
//!                      ▼
//!               jj repositories
//! ```
//!
//! - [`cli`]: the commands, which drive the daemon over its control socket.
//! - [`config`]: the machine key and the mesh state (`mesh.json`).
//! - [`daemon`]: peer connections, repo watches and the control socket.
//! - [`net`]: the iroh endpoint, and the pairing and sync protocols.
//! - [`repo`]: jj repo access and the op and git object transfer engine.
//! - `watch`: filesystem watching of op heads and working copies.
//! - `service`: the daemon as a systemd or launchd user service.
//!
//! # Trust model
//!
//! jj-mesh serves a single user across machines they operate. Every paired
//! machine has full read/write access to every mesh repo and can pair other
//! machines: there are no permissions. Connections are end-to-end encrypted
//! and mutually authenticated with per-machine keys, and only known peers
//! connect outside a pairing window. Authenticated does not mean trusted:
//! everything a peer sends is bounded and validated before it reaches
//! memory, disk or the terminal.
//!
//! # Consistency
//!
//! Sync replicates full history, so it must never lose or corrupt data:
//! - **Append-only**: no op or object is ever deleted, and jj reconciles
//!   divergent op logs. Only git refs move, to follow the synced state.
//! - **Raw bytes**: ops, views and git objects travel as the bytes stored on
//!   the sender and are never re-encoded (see the `repo::transfer` docs).
//! - **Crash-safe**: writes land in an order where a crash at any point
//!   leaves the repo consistent, and nothing becomes visible to jj before
//!   it is validated.
//! - **Pinned jj**: sync depends on jj internals, so `jj-lib` is pinned to
//!   an exact version and only the default backends are accepted.

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
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) mod service;
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

//! The control-socket vocabulary: the request/response types the CLI and
//! daemon exchange, and the timing budgets that bound the exchange. Shared
//! by [`super::server`] and [`super::client`], and re-exported for the CLI.

use std::{path::PathBuf, time::Duration};

use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use crate::{net::sync::StatusReport, repo::transfer::TransferProgress};

/// Maximum accepted size of a control message.
pub(super) const MAX_MESSAGE_SIZE: u32 = 1 << 20;

/// Time budget for the quick parts of an exchange (request, status answer).
pub(super) const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Time budget the CLI grants quick mutating requests (add, remove).
pub const MUTATE_WAIT: Duration = Duration::from_secs(10);

/// How long an issued pairing ticket stays valid. Kept here because the
/// CLI tells the user; the daemon enforces it.
pub const PAIR_TICKET_TTL: Duration = Duration::from_mins(3);

/// Deadline for the network-facing work of a clone's initial repo pull
/// (it may transfer an entire repository); the pull's local apply and
/// index work runs unbounded.
pub(super) const CLONE_PULL_NET_TIMEOUT: Duration = Duration::from_mins(30);

/// Cadence at which the daemon re-sends the latest clone progress. Sent
/// unconditionally, changed or not: the frames double as the liveness
/// signal behind [`CLONE_IDLE_WAIT`].
pub(super) const CLONE_PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

/// Time budget the CLI grants between clone frames, not the whole exchange:
/// the daemon heartbeats progress every [`CLONE_PROGRESS_INTERVAL`] while
/// the clone runs, so a silent daemon is a dead one. The transfer's
/// network phases stay bounded by the daemon's own
/// [`CLONE_PULL_NET_TIMEOUT`].
pub const CLONE_IDLE_WAIT: Duration = Duration::from_secs(30);

/// A request from the CLI to the daemon.
///
/// Postcard encodes variants by position: existing ones must keep their
/// position *and meaning* (renames are fine), and new ones are only
/// appended, so an exchange stays semantically sound across a CLI/daemon
/// version skew.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Report the daemon state; answered with [`Response::Status`].
    Status,
    /// Host a pairing: issue a fresh one-time ticket, revoking any
    /// outstanding one. Answered with [`Response::PairTicket`] or
    /// [`Response::Error`]. The exchange itself runs in the daemon; the
    /// ticket is valid for [`PAIR_TICKET_TTL`] or until redeemed or
    /// replaced.
    PairHost,
    /// Join a pairing hosted by another machine. Answered with
    /// [`Response::Paired`] or [`Response::Error`].
    PairJoin { ticket: String },
    /// Pull the full state of the mesh repo named `name` into a freshly
    /// initialized local repo at `path` and register it (see `jj-mesh repo
    /// clone`). Answered with a stream of [`Response::CloneProgress`] frames
    /// ending in [`Response::Cloned`] or [`Response::Error`]; disconnecting
    /// cancels the pull.
    CloneRepo { name: String, path: PathBuf },
    /// Register the repo at `path` under `name`, with a fresh id. Answered
    /// with [`Response::RepoAdded`] or [`Response::Error`].
    AddRepo { name: String, path: PathBuf },
    /// Retire a repo name from the mesh, unregistering it everywhere (its
    /// files are left alone). Answered with [`Response::RepoRemoved`] or
    /// [`Response::Error`].
    RemoveRepo { name: String },
    /// Remove a paired peer (by name, or endpoint id when names are
    /// ambiguous), disconnecting it. Answered with
    /// [`Response::PeerRemoved`] or [`Response::Error`].
    RemovePeer { peer: String },
    /// Unregister a repo on this machine only (its files are left alone);
    /// the mesh keeps the repo and it stays clonable here. Answered with
    /// [`Response::RepoForgotten`] or [`Response::Error`].
    ForgetRepo { name: String },
    /// Rename this machine; peers learn the new name through the gossip.
    /// Answered with [`Response::MachineRenamed`] or [`Response::Error`].
    RenameMachine { name: String },
}

/// A daemon answer to a [`Request`].
///
/// Postcard encodes variants by position: existing ones must keep their
/// position *and meaning* (renames are fine), and new ones are only
/// appended, so an exchange stays semantically sound across a CLI/daemon
/// version skew.
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Status(Status),
    /// The pairing ticket to transmit to the other machine.
    PairTicket(String),
    /// Pairing succeeded and the peer is saved in the mesh state.
    Paired {
        name: String,
        endpoint: EndpointId,
    },
    /// The clone pull completed and the repo is registered.
    Cloned {
        ops: u64,
        git_objects: u64,
    },
    /// The repo is registered (with a freshly generated internal id).
    RepoAdded,
    /// The repo is retired from the mesh; `was_local` tells whether it was
    /// registered on this machine.
    RepoRemoved {
        was_local: bool,
    },
    /// The peer is removed from the mesh state.
    PeerRemoved(EndpointId),
    /// The request failed.
    Error(String),
    /// Progress of an in-flight clone pull, re-sent at least every
    /// [`CLONE_PROGRESS_INTERVAL`] until the terminal answer.
    CloneProgress(CloneProgress),
    /// The repo is unregistered on this machine, at this path; the mesh
    /// still has it.
    RepoForgotten {
        path: PathBuf,
    },
    /// This machine's name is changed in the mesh state.
    MachineRenamed,
}

/// A snapshot of a running clone pull, for progress display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloneProgress {
    /// Paired name of the peer being pulled from, empty while the daemon
    /// has not picked one yet. Counters restart when the daemon falls back
    /// to another source peer.
    pub peer: String,
    /// The transfer counters, straight from the pull.
    pub transfer: TransferProgress,
}

/// Live daemon state, answering [`Request::Status`].
///
/// Anything peer-related is keyed by the peer's paired name, resolved by
/// the daemon: the CLI displays this structure as-is.
#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    /// This machine's name in the mesh.
    pub name: String,
    /// This machine's endpoint id.
    pub endpoint: EndpointId,
    /// Seconds since the daemon started.
    pub uptime_secs: u64,
    /// The jj version found on the daemon's PATH, `None` when jj is
    /// missing or unrecognizable. Compatibility is a warning, never an
    /// error: the daemon cannot know which jj binary writes the repos.
    pub jj_version: Option<String>,
    /// State of every configured peer.
    pub peers: Vec<PeerStatus>,
    /// Repos registered on this machine.
    pub repos: Vec<RepoStatus>,
    /// Mesh repos not registered here, clonable by name.
    pub available: Vec<String>,
    /// Repo names contested by peers announcing a different repo.
    pub conflicts: Vec<ConflictStatus>,
    /// Repos whose sync is paused because a peer also holds a colocated
    /// instance.
    pub paused: Vec<PausedStatus>,
    /// The latest health each connected peer reported for itself.
    pub peer_reports: Vec<PeerReport>,
}

/// A repo whose sync is paused by a colocation conflict: this machine's
/// instance and the named peers' are all colocated, which a mesh repo
/// supports on at most one machine (see the sync docs).
#[derive(Debug, Serialize, Deserialize)]
pub struct PausedStatus {
    pub repo: String,
    /// Names of the peers claiming a colocated instance.
    pub peers: Vec<String>,
}

/// One connected peer's self-reported health.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeerReport {
    /// The peer's paired name.
    pub peer: String,
    /// The report exactly as the peer sent it (sanitized on receipt).
    pub report: StatusReport,
}

/// A repo name contested by a peer: it announces a different repo (by id)
/// under a name registered here. Sync with that peer is suspended for the
/// repo until one side renames or removes it.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConflictStatus {
    pub repo: String,
    /// The contesting peer's paired name.
    pub peer: String,
}

/// Live state of one configured peer.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    pub name: String,
    pub endpoint: EndpointId,
    pub connection: ConnectionStatus,
}

/// State of the persistent connection to a peer.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConnectionStatus {
    /// Dialing, or waiting for the peer to dial us.
    Connecting {
        /// Consecutive failed attempts before this one.
        failures: u32,
    },
    /// Connection established.
    Connected {
        /// The selected network path, when already known.
        path: Option<PathInfo>,
        /// Seconds since the connection was established.
        since_secs: u64,
    },
    /// Last attempt failed; waiting before redialing.
    Backoff {
        /// Seconds until the next attempt.
        retry_in_secs: u64,
        /// Why the last attempt failed.
        error: String,
    },
}

/// The selected network path of a peer connection.
#[derive(Debug, Serialize, Deserialize)]
pub struct PathInfo {
    pub route: Route,
    /// Round-trip time on this path, in milliseconds.
    pub rtt_ms: u64,
}

/// How traffic reaches the peer.
#[derive(Debug, Serialize, Deserialize)]
pub enum Route {
    /// Hole-punched direct path to this socket address.
    Direct { addr: String },
    /// Traffic goes through this relay.
    Relay { url: String },
}

/// A registered repo.
#[derive(Debug, Serialize, Deserialize)]
pub struct RepoStatus {
    pub name: String,
    pub path: PathBuf,
    pub watch: WatchStatus,
}

/// State of the op-heads watch on a repo.
#[derive(Debug, Serialize, Deserialize)]
pub enum WatchStatus {
    /// The repo is being opened.
    Opening,
    /// Watching for op head changes.
    Watching {
        /// Current number of op heads (more than one means divergence).
        op_heads: u64,
        /// Seconds since the last observed change, if any since starting.
        last_change_secs: Option<u64>,
        /// Seconds since operations were last fetched from a peer.
        last_sync_secs: Option<u64>,
    },
    /// Opening or watching failed; waiting before retrying.
    Failed { error: String, retry_in_secs: u64 },
    /// The repo directory is gone (unmounted disk, or deleted without
    /// `jj-mesh repo forget`); retried in the background.
    Missing { retry_in_secs: u64 },
    /// The daemon is rebuilding the repo's commit index, so jj commands
    /// there do not pay for it.
    Indexing,
}

//! The control-socket vocabulary: the request/response types the CLI and
//! daemon exchange, and the timing budgets that bound the exchange. Shared
//! by [`super::server`] and [`super::client`], and re-exported for the CLI.

use std::{path::PathBuf, time::Duration};

use iroh::EndpointId;
use serde::{Deserialize, Serialize};

/// Maximum accepted size of a control message.
pub(super) const MAX_MESSAGE_SIZE: u32 = 1 << 20;

/// Time budget for the quick parts of an exchange (request, status answer).
pub(super) const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Time budget the CLI grants quick mutating requests (add, remove).
pub const MUTATE_WAIT: Duration = Duration::from_secs(10);

/// How long an issued pairing ticket stays valid. Kept here because the
/// CLI tells the user; the daemon enforces it.
pub const PAIR_TICKET_TTL: Duration = Duration::from_mins(3);

/// Time budget for a join's initial repo pull; it may transfer an entire
/// repository.
pub(super) const JOIN_PULL_TIMEOUT: Duration = Duration::from_mins(30);

/// Time budget the CLI grants a whole join request: the pull budget plus a
/// margin for validation and registration. Kept here next to the pull
/// budget so the two cannot drift apart.
pub const JOIN_WAIT: Duration = JOIN_PULL_TIMEOUT.saturating_add(Duration::from_mins(1));

/// A request from the CLI to the daemon.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Report the daemon state; answered with [`Response::Status`].
    Status,
    /// Host a pairing: issue a fresh one-time ticket, revoking any
    /// outstanding one. Answered with [`Response::PairTicket`] or
    /// [`Response::Error`]. The exchange itself runs in the daemon; the
    /// ticket is valid for [`PAIR_TICKET_TTL`] or until redeemed or
    /// replaced.
    PairHost { name: String },
    /// Join a pairing hosted by another machine. Answered with
    /// [`Response::Paired`] or [`Response::Error`].
    PairJoin { ticket: String, name: String },
    /// Pull the full state of the mesh repo named `name` into a freshly
    /// initialized local repo at `path` and register it (see `jj-mesh
    /// join`). Answered with [`Response::Joined`] or [`Response::Error`].
    JoinRepo { name: String, path: PathBuf },
    /// Register the repo at `path` under `name`, with a fresh id. Answered
    /// with [`Response::RepoAdded`] or [`Response::Error`].
    AddRepo { name: String, path: PathBuf },
    /// Retire a repo name from the mesh, unregistering it everywhere (its
    /// files are left alone). Answered with [`Response::RepoForgotten`] or
    /// [`Response::Error`].
    ForgetRepo { name: String },
    /// Remove a paired peer (by name, or endpoint id when names are
    /// ambiguous), disconnecting it. Answered with
    /// [`Response::PeerRemoved`] or [`Response::Error`].
    RemovePeer { peer: String },
}

/// A daemon answer to a [`Request`].
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
    /// The join pull completed and the repo is registered.
    Joined {
        ops: u64,
        git_objects: u64,
    },
    /// The repo is registered (with a freshly generated internal id).
    RepoAdded,
    /// The repo is retired from the mesh; `was_local` tells whether it was
    /// registered on this machine.
    RepoForgotten {
        was_local: bool,
    },
    /// The peer is removed from the mesh state.
    PeerRemoved(EndpointId),
    /// The request failed.
    Error(String),
}

/// Live daemon state, answering [`Request::Status`].
#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    /// This machine's endpoint id.
    pub endpoint: EndpointId,
    /// Seconds since the daemon started.
    pub uptime_secs: u64,
    /// State of every configured peer.
    pub peers: Vec<PeerStatus>,
    /// Repos registered on this machine.
    pub repos: Vec<RepoStatus>,
    /// Mesh repos not registered here, joinable by name.
    pub available: Vec<String>,
    /// Repo names contested by peers announcing a different repo.
    pub conflicts: Vec<ConflictStatus>,
}

/// A repo name contested by a peer: it announces a different repo (by id)
/// under a name registered here. Sync with that peer is suspended for the
/// repo until one side renames or removes it.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConflictStatus {
    pub repo: String,
    pub peer: EndpointId,
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
    Connecting,
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
}

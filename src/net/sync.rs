//! Sync protocol, multiplexed over the persistent peer connection.
//!
//! One-shot uni streams carry a [`UniMessage`]: op-head announcements,
//! membership gossip and daemon status reports. All are idempotent
//! latest-wins state, re-sent on every change and on peer (re)connect, so a
//! lost or dropped message is always healed by a later one. Op negotiation
//! and git object transfer run on bidirectional streams.

use color_eyre::eyre::{Result, ensure};
use iroh::endpoint::{Connection, RecvStream};
use serde::{Deserialize, Serialize};

use super::wire::{read_message, write_message};
use crate::config::{MAX_MESH_PEERS, MAX_MESH_REPOS, Membership, RepoId};

/// ALPN of the sync protocol. Bumped whenever the wire format changes, so
/// mismatched daemons refuse each other instead of mis-decoding.
pub const ALPN: &[u8] = b"jj-mesh/sync/0";

/// Cap on uni-stream messages. Sized for the largest legitimate message, a
/// full membership (peer records are ~100 bytes, repo records ~100), with
/// headroom; the peer, while authenticated, is not trusted with our
/// memory, and every byte here is decoded before we can check anything.
const MAX_UNI_SIZE: u32 = 192 * 1024;

/// A message carried by a one-shot uni stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UniMessage {
    Announce(Announce),
    Membership(Membership),
    Status(StatusReport),
}

/// Advertises the current op heads of one repo.
///
/// Repos are identified mesh-wide by their name; the id rides along so a
/// receiver can detect two different repos contesting one name (see
/// [`crate::daemon::hub`]) instead of silently merging them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Announce {
    /// Mesh-wide name of the repo.
    pub name: String,
    /// The sender's id for the repo, for conflict detection.
    pub id: RepoId,
    /// Publish sequence for this repo, monotonically increasing within the
    /// sender's daemon run. Streams have no cross-stream ordering, so the
    /// receiver uses it to discard reordered announcements; it forgets the
    /// watermark when the connection goes away.
    pub seq: u64,
    /// Current op head ids, as raw id bytes; the receiver validates them.
    pub heads: Vec<Vec<u8>>,
    /// Whether the sender's instance of the repo is colocated (has a
    /// user-visible `.git`). A mesh repo supports at most one colocated
    /// instance (see `jj-mesh join`'s docs); receivers use this to detect
    /// a second one and pause the repo instead of corrupting its history.
    pub colocated: bool,
}

/// One machine's self-reported health, shown by `jj-mesh status` on its
/// peers. Ephemeral by design: only ever held for connected peers, unlike
/// the durable, gossip-replicated membership. Compared for equality to
/// suppress re-broadcasts of an unchanged report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusReport {
    /// The sender's jj-mesh version (its `CARGO_PKG_VERSION`).
    pub daemon_version: String,
    /// The jj version on the sender's PATH, when detectable.
    pub jj_version: Option<String>,
    /// Health of every repo registered on the sender.
    pub repos: Vec<RepoHealth>,
}

/// Health of one repo in a [`StatusReport`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoHealth {
    /// Mesh-wide name of the repo.
    pub name: String,
    pub state: RepoHealthState,
}

/// Condensed repo state for peer consumption. Deliberately carries no
/// detail strings: local error messages embed filesystem paths (which
/// never leave the machine) and would make a full report outgrow the wire
/// cap; the owning machine's own `jj-mesh status` has the details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepoHealthState {
    /// Open and syncing.
    Ok,
    /// Cannot be opened or watched.
    Failed,
    /// The repo directory is gone.
    Missing,
    /// Sync suspended by a colocation conflict.
    Paused,
}

/// Sends one message on a fresh uni stream.
pub async fn send_uni(conn: &Connection, message: &UniMessage) -> Result<()> {
    let mut stream = conn.open_uni().await?;
    write_message(&mut stream, message, MAX_UNI_SIZE).await?;
    stream.finish()?;
    Ok(())
}

/// Reads the message from an accepted uni stream, rejecting memberships
/// and status reports too big to be legitimate before they reach the
/// daemon: later handling caps what is stored, but a bounded message keeps
/// the flood off the queue.
pub async fn recv_uni(stream: &mut RecvStream) -> Result<UniMessage> {
    let message: UniMessage = read_message(stream, MAX_UNI_SIZE).await?;

    match &message {
        UniMessage::Membership(membership) => {
            ensure!(
                membership.peers.len() <= MAX_MESH_PEERS
                    && membership.repos.len() <= MAX_MESH_REPOS,
                "membership too large",
            );
        }
        UniMessage::Status(report) => {
            ensure!(report.repos.len() <= MAX_MESH_REPOS, "status too large");
        }
        UniMessage::Announce(_) => {}
    }

    Ok(message)
}

// --- Fetch protocol ---
//
// A fetch runs on one bidirectional stream opened by the fetching side:
//
// 1. fetcher: [`FetchRequest`] (wanted op heads + have samples)
// 2. server:  [`OpFrame`]s, views before the ops referencing them, ops in
//    parents-first order, terminated by `OpFrame::Done`
// 3. fetcher: [`GitRequest`] (commits referenced by the new views that are
//    missing locally, plus have samples)
// 4. server:  [`GitFrame`]s carrying raw git objects, terminated by
//    `GitFrame::Done`
//
// The fetcher then applies everything locally in crash-safe order; nothing
// is written before the peer's data is fully validated per object.

/// Cap on op/view frames. Views grow with refs, not history; multi-MiB
/// views mean something is deeply wrong.
pub const MAX_OP_FRAME_SIZE: u32 = 4 << 20;

/// Git object frames (and the git request that lists them) are bounded only
/// by the wire's `u32` length prefix. A frame carries one whole git object,
/// so any artificial cap would just refuse a repo whose blobs are larger,
/// and peers are trusted with our memory. jj keeps oversized files out of
/// its default backend, so what travels here is the user's own content; the
/// receiver still bounds resident memory by streaming to disk in chunks.
pub const MAX_GIT_FRAME_SIZE: u32 = u32::MAX;

/// Cap on ids in want/have lists.
pub const MAX_WANTS: usize = 64;
pub const MAX_HAVES: usize = 256;

/// Cap on commit haves sent in the git phase (current view heads).
pub const MAX_GIT_HAVES: usize = 4096;

/// Opens a fetch: the op heads to obtain and a sample of ops the fetcher
/// already has (its heads plus exponentially spaced ancestors), bounding
/// what the server sends back.
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    /// Mesh-wide name of the repo.
    pub name: String,
    /// The fetcher's id for the repo; the server refuses a mismatch, the
    /// backstop against transferring data between unrelated repos that
    /// contest one name.
    pub id: RepoId,
    pub wants: Vec<Vec<u8>>,
    pub haves: Vec<Vec<u8>>,
}

/// Server-to-fetcher frame of the op phase.
///
/// Views and ops travel as the raw proto bytes stored in the server's op
/// store, under their stored ids. jj computes op and view ids by hashing
/// its in-memory structs at write time and never re-verifies them, so ids
/// of objects written by older jj versions do not survive a decode +
/// re-encode round trip; replicating the stored bytes verbatim is the only
/// way to keep ids identical across the mesh. The receiver validates the
/// bytes structurally (see `crate::repo::codec`) before storing them.
#[derive(Debug, Serialize, Deserialize)]
pub enum OpFrame {
    /// A view's raw proto bytes, sent before any op referencing it.
    View { id: Vec<u8>, view: Vec<u8> },
    /// An operation's raw proto bytes; its parents (when sent at all) were
    /// sent before it.
    Op { id: Vec<u8>, op: Vec<u8> },
    /// End of the op phase.
    Done,
    /// The server cannot serve this fetch.
    Error { message: String },
}

/// Commits the fetcher is missing after the op phase.
#[derive(Debug, Serialize, Deserialize)]
pub struct GitRequest {
    pub wants: Vec<Vec<u8>>,
    pub haves: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::config::{
        MAX_MESH_PEERS, MAX_MESH_REPOS, MAX_NAME_LEN, Membership, MeshRepo, Peer, RepoId,
    };

    /// A membership carrying the largest state we accept must stay inside
    /// the wire cap: if it did not, a machine at the caps could no longer
    /// gossip at all, and its sender task would die on every reconnect.
    #[test]
    fn a_full_membership_fits_on_the_wire() {
        let peers = (0..MAX_MESH_PEERS)
            .map(|_| {
                (
                    iroh::SecretKey::generate().public(),
                    Peer {
                        version: u64::from(u32::MAX),
                        status: crate::config::PeerStatus::Alive {
                            name: "n".repeat(MAX_NAME_LEN),
                        },
                    },
                )
            })
            .collect();
        let repos: BTreeMap<String, MeshRepo> = (0..MAX_MESH_REPOS)
            .map(|n| {
                (
                    format!("{n:0>64}"),
                    MeshRepo {
                        version: u64::from(u32::MAX),
                        status: crate::config::MeshRepoStatus::Present {
                            id: RepoId::generate(),
                        },
                    },
                )
            })
            .collect();

        let encoded =
            postcard::to_stdvec(&super::UniMessage::Membership(Membership { peers, repos }))
                .unwrap();
        assert!(
            encoded.len() <= super::MAX_UNI_SIZE as usize,
            "a full membership is {} bytes, over the {} byte cap",
            encoded.len(),
            super::MAX_UNI_SIZE,
        );
    }

    /// Same guarantee for the status report: a machine at the repo cap with
    /// every repo failing must still fit its report on the wire, or its
    /// sender tasks would die and stop all announcements.
    #[test]
    fn a_full_status_report_fits_on_the_wire() {
        let report = super::StatusReport {
            daemon_version: "9".repeat(MAX_NAME_LEN),
            jj_version: Some("9".repeat(MAX_NAME_LEN)),
            repos: (0..MAX_MESH_REPOS)
                .map(|n| super::RepoHealth {
                    name: format!("{n:0>64}"),
                    state: super::RepoHealthState::Failed,
                })
                .collect(),
        };

        let encoded = postcard::to_stdvec(&super::UniMessage::Status(report)).unwrap();
        assert!(
            encoded.len() <= super::MAX_UNI_SIZE as usize,
            "a full status report is {} bytes, over the {} byte cap",
            encoded.len(),
            super::MAX_UNI_SIZE,
        );
    }
}

/// Server-to-fetcher frame of the git phase.
#[derive(Debug, Serialize, Deserialize)]
pub enum GitFrame {
    /// One raw git object; `id` is verified against the data on receipt.
    Object {
        id: Vec<u8>,
        kind: WireObjectKind,
        data: Vec<u8>,
    },
    /// End of the git phase.
    Done,
    /// The server cannot serve the git phase.
    Error { message: String },
}

/// Git object kinds on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireObjectKind {
    Commit,
    Tree,
    Blob,
    Tag,
}

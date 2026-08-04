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
    /// Publish sequence, sender-wide: monotonic across all repos for the
    /// sender's daemon run. Streams have no cross-stream ordering, so the
    /// receiver uses it to discard reordered announcements.
    pub seq: u64,
    /// Current op head ids, as raw id bytes; the receiver validates them.
    pub heads: Vec<Vec<u8>>,
    /// Whether the sender's instance of the repo is colocated (has a
    /// user-visible `.git`). A mesh repo supports at most one colocated
    /// instance (see `jj-mesh repo clone`'s docs); receivers use this to detect
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
// 2. server:  `OpFrame::Begin` with the phase's exact frame counts (for
//    progress display; the delta is collected before streaming anyway),
//    then [`OpFrame`]s, views before the ops referencing them, ops in
//    parents-first order, terminated by `OpFrame::Done`
// 3. fetcher: [`GitRequest`] (commits referenced by the new views that are
//    missing locally, plus have samples, and the transfer format)
// 4. server:  [`GitFrame`]s terminated by `GitFrame::Done`: raw git objects
//    in the loose format, or chunks of one git packfile in the pack format
//
// The fetcher then applies everything locally in crash-safe order; nothing
// is written before the peer's data is fully validated per object.
//
// Op, view and loose git object payloads travel zstd-compressed (see
// [`compress_payload`]); QUIC does not compress, and proto bytes and git
// objects shrink well. Pack chunks are already zlib-compressed and travel
// as-is.

/// Cap on op/view frames. Views grow with refs, not history; multi-MiB
/// views mean something is deeply wrong.
pub const MAX_OP_FRAME_SIZE: u32 = 4 << 20;

/// Git object frames (and the git request that lists them) are bounded only
/// by the wire's `u32` length prefix. A frame carries one whole git object,
/// so any artificial cap would just refuse a repo whose blobs are larger,
/// and peers are trusted with our memory *in proportion to what they send*.
/// jj keeps oversized files out of its default backend, so what travels
/// here is the user's own content; the receiver still bounds resident
/// memory by streaming to disk in chunks, and caps decompression
/// separately (see [`MAX_GIT_OBJECT_SIZE`]), as compression breaks that
/// proportionality.
pub const MAX_GIT_FRAME_SIZE: u32 = u32::MAX;

/// Cap on op ids in a fetch's want list.
pub const MAX_WANTS: usize = 64;

/// Cap on op ids in a fetch's have sample.
pub const MAX_HAVES: usize = 256;

/// Cap on commit haves sent in the git phase (current view heads).
pub const MAX_GIT_HAVES: usize = 4096;

/// Cap on a single git object once decompressed.
///
/// The frame cap bounds only the bytes a peer spends: zstd expands highly
/// repetitive data by orders of magnitude, so without an own bound a
/// hundred-kilobyte frame could force a multi-gigabyte allocation. Far
/// above any object jj's default backend produces, and objects are
/// decompressed one at a time.
pub const MAX_GIT_OBJECT_SIZE: u64 = 512 << 20;

/// Compresses a wire payload (op, view or loose git object bytes).
pub fn compress_payload(bytes: &[u8]) -> Result<Vec<u8>> {
    // One-shot rather than streaming: the payload length is known, so zstd
    // sizes its context to the input instead of allocating a full
    // streaming workspace per call (payloads are mostly a few hundred
    // bytes, and a clone compresses one per op and view). Level 0 selects
    // zstd's default (3), a good size/speed balance.
    Ok(zstd::bulk::compress(bytes, 0)?)
}

/// Decompresses a wire payload, rejecting expansions past `max_size` (see
/// [`MAX_GIT_OBJECT_SIZE`]). Grows as it reads rather than allocating the
/// cap up front, so the bound costs nothing on ordinary payloads.
pub fn decompress_payload(bytes: &[u8], max_size: u64) -> Result<Vec<u8>> {
    use std::io::Read as _;

    let decoder = zstd::stream::read::Decoder::new(bytes)?;
    let mut out = Vec::new();
    decoder.take(max_size + 1).read_to_end(&mut out)?;
    ensure!(out.len() as u64 <= max_size, "payload too large");
    Ok(out)
}

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
/// store, under their stored ids (see the sync docs for why re-encoding
/// them is impossible). The receiver validates the bytes structurally
/// (`repo`'s codec) before storing them.
///
/// The proto bytes travel zstd-compressed; compression is a wire concern
/// and the stored bytes stay verbatim.
#[derive(Debug, Serialize, Deserialize)]
pub enum OpFrame {
    /// Announces the exact number of op and view frames of the phase,
    /// sent before any of them. For progress display only.
    Begin { ops: u64, views: u64 },
    /// A view's raw proto bytes (compressed), sent before any op
    /// referencing it.
    View { id: Vec<u8>, view: Vec<u8> },
    /// An operation's raw proto bytes (compressed); its parents (when sent
    /// at all) were sent before it.
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
    /// How the server should send the objects.
    pub format: GitTransferFormat,
}

/// Git object transfer formats.
///
/// Loose sends one hash-verified object per frame and suits incremental
/// syncs, whose few objects exist only loose on the server anyway. Pack
/// streams one packfile, letting the server reuse the deltas of its
/// on-disk packs and the fetcher index the objects into a single pack
/// instead of thousands of loose files; clones request it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitTransferFormat {
    Loose,
    Pack,
}

/// Server-to-fetcher frame of the git phase.
#[derive(Debug, Serialize, Deserialize)]
pub enum GitFrame {
    /// One raw git object, zstd-compressed; `id` is verified against the
    /// decompressed data on receipt. Only sent in the loose format.
    Object {
        id: Vec<u8>,
        kind: WireObjectKind,
        data: Vec<u8>,
    },
    /// One chunk of the packfile stream. Only sent in the pack format; the
    /// pack's own trailer checksum and the per-object hashing done while
    /// indexing verify the content.
    Pack { chunk: Vec<u8> },
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::config::{
        MAX_MESH_PEERS, MAX_MESH_REPOS, MAX_NAME_LEN, Membership, MeshRepo, Peer, RepoId,
    };

    #[test]
    fn payloads_roundtrip_and_bombs_are_rejected() {
        let payload = b"some sync payload".repeat(64);
        let compressed = super::compress_payload(&payload).unwrap();
        let out = super::decompress_payload(&compressed, payload.len() as u64).unwrap();
        assert_eq!(out, payload);

        // A frame far within the size cap must not decompress past it: a
        // highly repetitive payload compresses by orders of magnitude, so
        // the bound has to come from the decompressed side.
        let bomb = super::compress_payload(&vec![0u8; 8 << 20]).unwrap();
        assert!(bomb.len() < 64 << 10, "bomb unexpectedly incompressible");
        let err = super::decompress_payload(&bomb, 4 << 20).unwrap_err();
        assert!(err.to_string().contains("too large"), "{err:#}");
    }

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

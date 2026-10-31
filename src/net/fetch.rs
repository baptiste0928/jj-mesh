//! The fetch protocol, run on one bidirectional stream opened by the
//! fetching side:
//!
//! ```text
//! fetcher                                     server
//!    |  FetchRequest { wants, haves }           |
//!    |----------------------------------------->|
//!    |  OpFrame: Begin, (View | Op)*, Done      |
//!    |<-----------------------------------------|
//!    |  GitRequest { wants, haves, format }     |
//!    |----------------------------------------->|
//!    |  GitFrame: (Object* | Pack chunk*), Done |
//!    |<-----------------------------------------|
//! ```
//!
//! Op, view and loose git object payloads travel zstd-compressed (see
//! [`compress_payload`]); QUIC does not compress, and proto bytes and git
//! objects shrink well. Pack chunks are already zlib-compressed and travel
//! as-is.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::RepoId;

/// Git object frames are bounded only by the wire's `u32` length prefix:
/// a frame carries a whole blob. Other frames use
/// [`super::wire::MAX_FRAME_SIZE`].
pub const MAX_GIT_FRAME_SIZE: u32 = u32::MAX;

/// Compresses a wire payload (op, view or loose git object bytes).
pub fn compress_payload(bytes: &[u8]) -> Result<Vec<u8>> {
    // One-shot rather than streaming: the payload length is known, so zstd
    // sizes its context to the input instead of allocating a full
    // streaming workspace per call (payloads are mostly a few hundred
    // bytes, and a clone compresses one per op and view). Level 0 selects
    // zstd's default (3), a good size/speed balance.
    Ok(zstd::bulk::compress(bytes, 0)?)
}

/// Decompresses a wire payload.
pub fn decompress_payload(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::stream::decode_all(bytes)?)
}

/// Opens a fetch: the op heads to obtain and a sample of ops the fetcher
/// already has (its heads plus exponentially spaced ancestors), bounding
/// what the server sends back.
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub id: RepoId,
    pub wants: Vec<Vec<u8>>,
    pub haves: Vec<Vec<u8>>,
}

/// Server-to-fetcher frame of the op phase.
///
/// Views and ops travel as the raw proto bytes stored in the server's op
/// store, under their stored ids (see [`crate::repo::transfer`] for why
/// re-encoding them is impossible). The receiver validates the bytes
/// structurally (`repo`'s codec) before storing them.
///
/// The proto bytes travel zstd-compressed; compression is a wire concern
/// and the stored bytes stay verbatim.
#[derive(Debug, Serialize, Deserialize)]
pub enum OpFrame {
    /// Announces the exact number of op and view frames of the phase,
    /// sent before any of them; the fetcher refuses frames past it.
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

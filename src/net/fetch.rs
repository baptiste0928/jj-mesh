//! The fetch protocol, run on one bidirectional stream opened by the
//! fetching side:
//!
//! 1. fetcher: [`FetchRequest`] (wanted op heads + have samples)
//! 2. server:  [`OpFrame::Begin`] with the phase's exact frame counts (for
//!    progress display; the delta is collected before streaming anyway),
//!    then [`OpFrame`]s, views before the ops referencing them, ops in
//!    parents-first order, terminated by [`OpFrame::Done`]
//! 3. fetcher: [`GitRequest`] (commits referenced by the new views that are
//!    missing locally, plus have samples, and the transfer format)
//! 4. server:  [`GitFrame`]s terminated by [`GitFrame::Done`]: raw git
//!    objects in the loose format, or chunks of one git packfile in the
//!    pack format
//!
//! The fetcher then applies everything locally in crash-safe order; nothing
//! is written before the peer's data is fully validated per object.
//!
//! Op, view and loose git object payloads travel zstd-compressed (see
//! [`compress_payload`]); QUIC does not compress, and proto bytes and git
//! objects shrink well. Pack chunks are already zlib-compressed and travel
//! as-is.

use color_eyre::eyre::{Result, ensure};
use serde::{Deserialize, Serialize};

use crate::config::RepoId;

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
}

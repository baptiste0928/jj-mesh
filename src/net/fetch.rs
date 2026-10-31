//! The fetch protocol, run on one bidirectional stream opened by the
//! fetching side:
//!
//! ```text
//! fetcher                                             server
//!    |  FetchRequest { wants, haves }                      |
//!    |---------------------------------------------------->|
//!    |  OpFrame: Begin, (View | Op)*, Done | Error          |
//!    |<----------------------------------------------------|
//!    |  GitRequest { wants, haves, format }                |
//!    |---------------------------------------------------->|
//!    |  GitFrame: [Begin, (Object* | Pack*)], Done | Error  |
//!    |<----------------------------------------------------|
//! ```
//!
//! Op and view payloads travel as one zstd stream per op phase (see
//! [`PhaseCompressor`]), loose git objects zstd-compressed one by one (see
//! [`compress_object`]); QUIC does not compress, and proto bytes and git
//! objects shrink well. Pack chunks are already zlib-compressed and travel
//! as-is.

use std::io::Write as _;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::config::RepoId;

/// Git object and pack frames are bounded only by the wire's `u32` length
/// prefix: a loose object frame carries a whole blob. Other frames use
/// [`super::wire::MAX_FRAME_SIZE`].
pub const MAX_GIT_FRAME_SIZE: u32 = u32::MAX;

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
/// The proto bytes travel compressed (see [`PhaseCompressor`]);
/// compression is a wire concern and the stored bytes stay verbatim.
#[derive(Debug, Serialize, Deserialize)]
pub enum OpFrame {
    /// Announces the exact number of op and view frames of the phase,
    /// sent before any of them; the fetcher refuses frames past it.
    Begin { ops: u64, views: u64 },
    /// A view's raw proto bytes, sent before any op referencing it.
    View {
        id: Vec<u8>,
        #[serde(with = "serde_bytes")]
        view: Vec<u8>,
    },
    /// An operation's raw proto bytes; its parents (when sent at all) were
    /// sent before it.
    Op {
        id: Vec<u8>,
        #[serde(with = "serde_bytes")]
        op: Vec<u8>,
    },
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
    /// The format the fetcher asks for.
    pub format: GitTransferFormat,
}

/// Git object transfer formats.
///
/// Loose sends one hash-verified object per frame and suits incremental
/// syncs, whose few objects exist only loose on the server anyway. Pack
/// streams one packfile, letting the server reuse the deltas of its
/// on-disk packs and the fetcher index the objects into a single pack
/// instead of thousands of loose files. Clones ask for it, and servers
/// switch large loose transfers to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitTransferFormat {
    Loose,
    Pack,
}

/// Server-to-fetcher frame of the git phase.
#[derive(Debug, Serialize, Deserialize)]
pub enum GitFrame {
    /// Announces the format and the exact object count of the phase, sent
    /// before any object. Omitted when there is nothing to send.
    Begin {
        format: GitTransferFormat,
        objects: u64,
    },
    /// One raw git object, zstd-compressed; `id` is verified against the
    /// decompressed data on receipt. Only sent in the loose format.
    Object {
        id: Vec<u8>,
        kind: WireObjectKind,
        #[serde(with = "serde_bytes")]
        data: Vec<u8>,
    },
    /// One chunk of the packfile stream. Only sent in the pack format; the
    /// pack's own trailer checksum and the per-object hashing done while
    /// indexing verify the content.
    Pack {
        #[serde(with = "serde_bytes")]
        chunk: Vec<u8>,
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

/// Compresses a loose git object.
pub fn compress_object(bytes: &[u8]) -> Result<Vec<u8>> {
    // One-shot rather than streaming: the payload length is known, so zstd
    // sizes its context to the input instead of allocating a full
    // streaming workspace per call. Level 0 selects zstd's default (3), a
    // good size/speed balance.
    Ok(zstd::bulk::compress(bytes, 0)?)
}

/// Decompresses a loose git object.
pub fn decompress_object(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::stream::decode_all(bytes)?)
}

/// Window of a phase's zstd stream, bounding the decoder's memory. Covers
/// dozens of views, which mostly repeat their predecessors.
const PHASE_WINDOW_LOG: u32 = 22;

/// Compresses the payloads of an op phase as a single zstd stream, each
/// flushed into its own block: later payloads reference earlier ones.
pub struct PhaseCompressor(zstd::stream::write::Encoder<'static, Vec<u8>>);

impl PhaseCompressor {
    /// A compressor for a new phase.
    pub fn new() -> Result<Self> {
        let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 0)?;
        encoder.window_log(PHASE_WINDOW_LOG)?;
        Ok(PhaseCompressor(encoder))
    }

    /// Compresses the next payload of the phase.
    pub fn compress(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.0.write_all(bytes)?;
        self.0.flush()?;
        Ok(std::mem::take(self.0.get_mut()))
    }
}

/// Decompresses the payloads of a [`PhaseCompressor`], in order.
pub struct PhaseDecompressor(zstd::stream::write::Decoder<'static, Vec<u8>>);

impl PhaseDecompressor {
    /// A decompressor for a new phase.
    pub fn new() -> Result<Self> {
        let mut decoder = zstd::stream::write::Decoder::new(Vec::new())?;
        decoder.window_log_max(PHASE_WINDOW_LOG)?;
        Ok(PhaseDecompressor(decoder))
    }

    /// Decompresses the next payload of the phase.
    pub fn decompress(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.0.write_all(bytes)?;
        self.0.flush()?;
        Ok(std::mem::take(self.0.get_mut()))
    }
}

#[cfg(test)]
mod tests {
    /// A phase stream decodes payload by payload, and compresses repeats
    /// against earlier payloads.
    #[test]
    fn phase_streams_roundtrip() {
        let mut compressor = super::PhaseCompressor::new().unwrap();
        let mut decompressor = super::PhaseDecompressor::new().unwrap();
        let view: Vec<u8> = (0..64 << 10).map(|_| rand::random()).collect();
        let first = compressor.compress(&view).unwrap();
        let repeat = compressor.compress(&view).unwrap();
        assert!(repeat.len() < first.len() / 100, "repeat not compressed");
        assert_eq!(decompressor.decompress(&first).unwrap(), view);
        assert_eq!(decompressor.decompress(&repeat).unwrap(), view);
    }
}

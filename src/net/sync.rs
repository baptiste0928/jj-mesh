//! Sync protocol, multiplexed over the persistent peer connection.
//!
//! Head announcements travel as one-shot uni streams carrying an
//! [`Announce`]. They are idempotent latest-wins state, re-sent on every
//! head change, watch start, and peer (re)connect, so a lost or dropped
//! announcement is always healed by a later one. Op negotiation and git
//! object transfer come next, on bidirectional streams.

use color_eyre::eyre::Result;
use iroh::endpoint::{Connection, RecvStream};
use serde::{Deserialize, Serialize};

use super::wire::{read_message, write_message};
use crate::config::RepoId;

/// ALPN of the sync protocol.
pub const ALPN: &[u8] = b"jj-mesh/sync/0";

/// Cap on announce messages; a legitimate one carries a handful of head
/// ids, and the peer, while authenticated, is not trusted with our memory.
const MAX_ANNOUNCE_SIZE: u32 = 64 * 1024;

/// Advertises the current op heads of one repo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Announce {
    /// Mesh-wide id of the repo.
    pub repo: RepoId,
    /// Publish sequence for this repo, monotonically increasing within the
    /// sender's daemon run. Streams have no cross-stream ordering, so the
    /// receiver uses it to discard reordered announcements; it forgets the
    /// watermark when the connection goes away.
    pub seq: u64,
    /// Current op head ids, as raw id bytes; the receiver validates them.
    pub heads: Vec<Vec<u8>>,
}

/// Sends one announcement on a fresh uni stream.
pub async fn send_announce(conn: &Connection, announce: &Announce) -> Result<()> {
    let mut stream = conn.open_uni().await?;
    write_message(&mut stream, announce, MAX_ANNOUNCE_SIZE).await?;
    stream.finish()?;
    Ok(())
}

/// Reads the announcement from an accepted uni stream.
pub async fn recv_announce(stream: &mut RecvStream) -> Result<Announce> {
    read_message(stream, MAX_ANNOUNCE_SIZE).await
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

/// Cap on git object frames: must fit the largest blob in the repo (large
/// files deserve a dedicated mechanism later).
pub const MAX_GIT_FRAME_SIZE: u32 = 64 << 20;

/// Cap on ids in want/have lists.
pub const MAX_WANTS: usize = 64;
pub const MAX_HAVES: usize = 256;

/// Cap on commit ids in a git request: commits referenced by the fetched
/// views (heads, refs, working copies), not history.
pub const MAX_GIT_WANTS: usize = 65_536;

/// Opens a fetch: the op heads to obtain and a sample of ops the fetcher
/// already has (its heads plus exponentially spaced ancestors), bounding
/// what the server sends back.
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchRequest {
    pub repo: RepoId,
    pub wants: Vec<Vec<u8>>,
    pub haves: Vec<Vec<u8>>,
}

/// Server-to-fetcher frame of the op phase.
#[derive(Debug, Serialize, Deserialize)]
pub enum OpFrame {
    /// A view, sent before any op referencing it.
    View { id: Vec<u8>, view: WireView },
    /// An operation; its parents (when sent at all) were sent before it.
    Op { id: Vec<u8>, op: WireOperation },
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

// --- Wire forms of jj's operation log data ---
//
// jj's own types are not serializable, so they are mirrored field for
// field (conversions live in `crate::repo::codec`, next to the jj
// dependency). Fidelity failures cannot corrupt a repo: writes verify the
// content-addressed id, so a drifted codec fails syncs loudly instead.

/// A commit id and the ids it was rewritten from, in wire form.
pub type WirePredecessors = Vec<(Vec<u8>, Vec<Vec<u8>>)>;

/// Mirror of [`jj_lib::op_store::Operation`] plus its metadata, flattened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireOperation {
    pub view_id: Vec<u8>,
    pub parents: Vec<Vec<u8>>,
    pub start_time: WireTimestamp,
    pub end_time: WireTimestamp,
    pub description: String,
    pub hostname: String,
    pub username: String,
    pub is_snapshot: bool,
    pub workspace_name: Option<String>,
    pub attributes: Vec<(String, String)>,
    pub commit_predecessors: Option<WirePredecessors>,
}

/// Mirror of [`jj_lib::backend::Timestamp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireTimestamp {
    pub millis: i64,
    pub tz_offset: i32,
}

/// Mirror of [`jj_lib::op_store::View`]. Maps become sorted pairs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireView {
    pub head_ids: Vec<Vec<u8>>,
    pub local_bookmarks: Vec<(String, WireRefTarget)>,
    pub local_tags: Vec<(String, WireRefTarget)>,
    pub remote_views: Vec<(String, WireRemoteView)>,
    pub git_refs: Vec<(String, WireRefTarget)>,
    pub git_head: WireRefTarget,
    pub wc_commit_ids: Vec<(String, Vec<u8>)>,
}

/// Mirror of [`jj_lib::op_store::RefTarget`]: the interleaved values of
/// its merge, which must have odd length (validated on decode).
pub type WireRefTarget = Vec<Option<Vec<u8>>>;

/// Mirror of [`jj_lib::op_store::RemoteView`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRemoteView {
    pub bookmarks: Vec<(String, WireRemoteRef)>,
    pub tags: Vec<(String, WireRemoteRef)>,
}

/// Mirror of [`jj_lib::op_store::RemoteRef`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireRemoteRef {
    pub target: WireRefTarget,
    pub tracked: bool,
}

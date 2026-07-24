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

//! Sync protocol, multiplexed over the persistent peer connection.
//!
//! The protocol is pull-based: announcements advertise op heads, and a
//! machine missing some fetches them. The receiver thus controls exactly
//! what enters its repo (validation lives on one side), and delivery needs
//! no guarantees: the latest announcement always suffices.
//!
//! One-shot uni streams carry a [`UniMessage`]: op-head announcements,
//! membership gossip and daemon status reports. All are idempotent
//! latest-wins state, re-sent on every change and on peer (re)connect, so a
//! lost or dropped message is always healed by a later one. Op negotiation
//! and git object transfer run on bidirectional streams (see
//! [`super::fetch`]).

use anyhow::Result;
use iroh::endpoint::{Connection, RecvStream};
use serde::{Deserialize, Serialize};

use super::wire::{MAX_FRAME_SIZE, read_message, write_message};
use crate::config::{Membership, RepoId};

/// ALPN of the sync protocol, opening with a [`super::hello`].
pub const ALPN: &[u8] = b"jj-mesh/sync/3";

/// A message carried by a one-shot uni stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UniMessage {
    Announce(Announce),
    Membership(Membership),
    Status(StatusReport),
}

/// Advertises the current op heads of one repo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Announce {
    pub id: RepoId,
    /// Publish sequence, sender-wide: monotonic across all repos for the
    /// sender's daemon run. Streams have no cross-stream ordering, so the
    /// receiver uses it to discard reordered announcements.
    pub seq: u64,
    /// Current op head ids, as raw id bytes; the receiver validates them.
    pub heads: Vec<Vec<u8>>,
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
    pub id: RepoId,
    pub state: RepoHealthState,
}

/// Condensed repo state for peer consumption. Deliberately carries no
/// detail strings: local error messages embed filesystem paths, which
/// never leave the machine; its own `jj-mesh status` has the details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepoHealthState {
    /// Open and syncing.
    Ok,
    /// Cannot be opened or watched.
    Failed,
    /// The repo directory is gone.
    Missing,
}

/// Sends one message on a fresh uni stream.
pub async fn send_uni(conn: &Connection, message: &UniMessage) -> Result<()> {
    let mut stream = conn.open_uni().await?;
    write_message(&mut stream, message, MAX_FRAME_SIZE).await?;
    stream.finish()?;
    Ok(())
}

/// Reads the message from an accepted uni stream.
pub async fn recv_uni(stream: &mut RecvStream) -> Result<UniMessage> {
    read_message(stream, MAX_FRAME_SIZE).await
}

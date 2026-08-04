//! Sync protocol, multiplexed over the persistent peer connection.
//!
//! One-shot uni streams carry a [`UniMessage`]: op-head announcements,
//! membership gossip and daemon status reports. All are idempotent
//! latest-wins state, re-sent on every change and on peer (re)connect, so a
//! lost or dropped message is always healed by a later one. Op negotiation
//! and git object transfer run on bidirectional streams (see
//! [`super::fetch`]).

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

//! Gossip-replicated membership records.
//!
//! [`Peer`] and [`MeshRepo`] are versioned registers: the higher version
//! wins, and ties resolve to the retired state (removed), then to the
//! smaller payload, so every machine converges on the same record without
//! clocks. Tombstones are records too and must be remembered, so a machine
//! that missed a removal cannot undo it.

use std::{borrow::Borrow, cmp, collections::BTreeMap, fmt};

use color_eyre::eyre::{Result, ensure};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use super::RepoId;

/// Cap on machines tracked in the mesh state, tombstones included. A
/// personal mesh is a handful of machines; the cap keeps a peer from
/// growing the state file (and the membership we must be able to gossip)
/// without bound.
pub const MAX_MESH_PEERS: usize = 256;

/// Cap on repos tracked in the mesh-wide repo list, same reasoning.
pub const MAX_MESH_REPOS: usize = 1024;

/// Cap on a record's version. Versions only ever advance by one per local
/// change, so a record beyond this is corrupted or hostile: without the
/// cap, a record parked at `u64::MAX` could never be superseded again,
/// permanently freezing a machine out of the mesh.
pub(super) const MAX_RECORD_VERSION: u64 = 1 << 32;

/// The membership one machine gossips: its whole view of the mesh. Sent as
/// idempotent latest-wins snapshots (on connect, on every change, and
/// periodically), so a lost message is healed by a later one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub peers: BTreeMap<EndpointId, Peer>,
    /// The mesh-wide repo list, tombstones included.
    pub repos: BTreeMap<String, MeshRepo>,
}

/// A gossip-replicated versioned register. [`Peer`] and [`MeshRepo`] are
/// the two implementors.
pub(super) trait Register: Clone {
    /// Bumped on every local change, so the change outranks other machines.
    fn version(&self) -> u64;
    /// Whether this record outranks `other` and should replace it on merge.
    fn outranks(&self, other: &Self) -> bool;
}

/// The version a local change to the record under `key` must carry to
/// outrank the stored one. Refuses to pass [`MAX_RECORD_VERSION`]: a
/// saturating bump would make the record unchangeable, so one parked at the
/// ceiling is reported instead of silently freezing its subject out.
pub(super) fn bumped_version<K, Q, V>(map: &BTreeMap<K, V>, key: &Q) -> Result<u64>
where
    K: Ord + Borrow<Q>,
    Q: Ord + fmt::Display + ?Sized,
    V: Register,
{
    let version = map.get(key).map_or(0, V::version);
    ensure!(
        version < MAX_RECORD_VERSION,
        "the mesh record of `{key}` is corrupted (version {version}); \
         remove it from mesh.json on every machine",
    );

    Ok(version + 1)
}

/// Whether a gossiped `record` should be adopted into `map` under `key`: it
/// must outrank what we hold, and a key we do not know is only added while
/// the map is below `cap`, so a peer cannot grow our state without bound.
pub(super) fn should_adopt<K: Ord, V: Register>(
    map: &BTreeMap<K, V>,
    key: &K,
    record: &V,
    cap: usize,
) -> bool {
    match map.get(key) {
        Some(ours) => record.outranks(ours),
        None => map.len() < cap,
    }
}

/// A machine's record in the mesh.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    /// Bumped on every local change to the record, so the change outranks
    /// the state every other machine holds.
    pub version: u64,
    pub status: PeerStatus,
}

impl Peer {
    /// The machine's name while it is part of the mesh; `None` for a
    /// tombstone.
    pub fn name(&self) -> Option<&str> {
        match &self.status {
            PeerStatus::Alive { name } => Some(name),
            PeerStatus::Removed => None,
        }
    }
}

impl Register for Peer {
    fn version(&self) -> u64 {
        self.version
    }

    /// Higher version first, removal beating alive on ties, then the smaller
    /// name (so equal-version renames converge instead of ping-ponging).
    fn outranks(&self, other: &Self) -> bool {
        fn rank(peer: &Peer) -> (u64, bool, cmp::Reverse<&str>) {
            (
                peer.version,
                peer.name().is_none(),
                cmp::Reverse(peer.name().unwrap_or("")),
            )
        }
        rank(self) > rank(other)
    }
}

/// Whether a machine is part of the mesh. `Removed` is a tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Alive { name: String },
    Removed,
}

/// A repo's record in the mesh-wide list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshRepo {
    /// Bumped on every local change to the record.
    pub version: u64,
    pub status: MeshRepoStatus,
}

impl MeshRepo {
    /// The repo's mesh id while the name is in use; `None` for a tombstone.
    pub fn id(&self) -> Option<&RepoId> {
        match &self.status {
            MeshRepoStatus::Present { id } => Some(id),
            MeshRepoStatus::Removed => None,
        }
    }
}

impl Register for MeshRepo {
    fn version(&self) -> u64 {
        self.version
    }

    /// Higher version first, removal beating presence on ties, then the
    /// smaller id.
    fn outranks(&self, other: &Self) -> bool {
        fn rank(repo: &MeshRepo) -> (u64, bool, cmp::Reverse<Option<&RepoId>>) {
            (repo.version, repo.id().is_none(), cmp::Reverse(repo.id()))
        }
        rank(self) > rank(other)
    }
}

/// Whether a repo name is in use on the mesh. `Removed` is a tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeshRepoStatus {
    Present { id: RepoId },
    Removed,
}

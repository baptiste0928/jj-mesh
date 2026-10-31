//! Gossip-replicated membership records.
//!
//! Membership records are versioned registers ([`Register`]): the higher
//! version wins, and each record type breaks ties the same way everywhere,
//! so every machine converges on the same record without clocks.
//! Tombstones are records too and must be remembered, so a machine that
//! missed a removal cannot undo it.

use std::{borrow::Borrow, cmp, collections::BTreeMap, fmt};

use anyhow::{Context as _, Result};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use super::{ObjectHash, RepoId, WorkspaceClaims};
use crate::config::validate_name;

/// The membership one machine gossips: its whole view of the mesh. Sent as
/// idempotent latest-wins snapshots (on connect, on every change, and
/// periodically), so a lost message is healed by a later one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub peers: BTreeMap<EndpointId, Peer>,
    /// The mesh-wide repo list, tombstones included.
    pub repos: BTreeMap<RepoId, MeshRepo>,
    /// The workspace claims of every alive machine.
    pub claims: BTreeMap<EndpointId, WorkspaceClaims>,
}

/// A gossip-replicated versioned register.
pub(super) trait Register: Clone {
    /// Bumped on every local change, so the change outranks other machines.
    fn version(&self) -> u64;
    /// Whether this record outranks `other` and should replace it on merge.
    fn outranks(&self, other: &Self) -> bool;
}

/// The version a local change to the record under `key` must carry to
/// outrank the stored one.
pub(super) fn bumped_version<K, Q, V>(map: &BTreeMap<K, V>, key: &Q) -> Result<u64>
where
    K: Ord + Borrow<Q>,
    Q: Ord + fmt::Display + ?Sized,
    V: Register,
{
    next_version(map.get(key).map_or(0, V::version), key)
}

/// The version after `version` for the record of `subject`.
pub(super) fn next_version(version: u64, subject: impl fmt::Display) -> Result<u64> {
    version.checked_add(1).with_context(|| {
        format!(
            "the mesh record of {subject} is corrupted (version {version}); \
             remove it from mesh.json on every machine"
        )
    })
}

/// Whether a gossiped `record` should be adopted into `map` under `key`:
/// it must outrank what we hold, if anything.
pub(super) fn should_adopt<K: Ord, V: Register>(map: &BTreeMap<K, V>, key: &K, record: &V) -> bool {
    map.get(key).is_none_or(|ours| record.outranks(ours))
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
            PeerStatus::Alive { name, .. } => Some(name),
            PeerStatus::Removed => None,
        }
    }

    /// Whether the machine is alive and runs in server mode.
    pub fn is_server(&self) -> bool {
        matches!(self.status, PeerStatus::Alive { server: true, .. })
    }
}

impl Register for Peer {
    fn version(&self) -> u64 {
        self.version
    }

    /// Higher version first, removal beating alive on ties, then the smaller
    /// name and the server role (so equal-version changes converge instead
    /// of ping-ponging).
    fn outranks(&self, other: &Self) -> bool {
        fn rank(peer: &Peer) -> (u64, bool, cmp::Reverse<&str>, bool) {
            (
                peer.version,
                peer.name().is_none(),
                cmp::Reverse(peer.name().unwrap_or("")),
                peer.is_server(),
            )
        }
        rank(self) > rank(other)
    }
}

/// Whether a machine is part of the mesh. `Removed` is a tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Alive {
        name: String,
        /// Whether it runs in server mode.
        #[serde(default)]
        server: bool,
    },
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
    /// The repo's name and git object hash while it is in the mesh; `None`
    /// for a tombstone.
    pub fn present(&self) -> Option<(&str, ObjectHash)> {
        match &self.status {
            MeshRepoStatus::Present { name, object_hash } => Some((name, *object_hash)),
            MeshRepoStatus::Removed => None,
        }
    }

    /// The repo's name while it is in the mesh; `None` for a tombstone.
    pub fn name(&self) -> Option<&str> {
        self.present().map(|(name, _)| name)
    }

    /// Whether the record carries a usable name (or is a tombstone).
    pub(super) fn is_valid(&self) -> bool {
        self.name()
            .is_none_or(|name| validate_name("repo", name).is_ok())
    }
}

impl Register for MeshRepo {
    fn version(&self) -> u64 {
        self.version
    }

    /// Removal first, whatever the version (repo ids are never reused, so
    /// nothing may bring a removed repo back), then the higher version,
    /// then the smaller name and object hash.
    fn outranks(&self, other: &Self) -> bool {
        fn rank(repo: &MeshRepo) -> (bool, u64, cmp::Reverse<Option<(&str, ObjectHash)>>) {
            (
                repo.name().is_none(),
                repo.version,
                cmp::Reverse(repo.present()),
            )
        }
        rank(self) > rank(other)
    }
}

/// Whether a repo is in the mesh. `Removed` is a tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeshRepoStatus {
    Present {
        name: String,
        object_hash: ObjectHash,
    },
    Removed,
}

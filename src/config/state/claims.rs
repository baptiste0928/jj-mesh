//! Workspace claims: which machine keeps which workspace of a repo fresh.
//!
//! [`WorkspaceClaims`] is the register each machine gossips, and
//! [`RepoClaims`] one repo's claims as its watch task sees them.

use std::{
    cmp,
    collections::{BTreeMap, BTreeSet},
};

use color_eyre::eyre::{Result, ensure};
use serde::{Deserialize, Serialize};

use super::{
    RepoId,
    membership::{MAX_RECORD_VERSION, Register, next_version},
};
use crate::config::validate_name;

/// Cap on the workspaces one machine claims, all repos together.
pub const MAX_MACHINE_WORKSPACES: usize = 64;

/// Cap on the workspaces claimed across the mesh, bounding the gossiped
/// membership like the peer and repo caps.
pub const MAX_MESH_WORKSPACES: usize = 1024;

/// Cap on the other machines' claims, leaving room for our own.
pub(super) const MAX_OTHER_WORKSPACES: usize = MAX_MESH_WORKSPACES - MAX_MACHINE_WORKSPACES;

/// The workspaces one machine claims, by repo.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceClaims {
    pub version: u64,
    pub repos: BTreeMap<RepoId, BTreeSet<String>>,
}

/// The claims on one repo's workspaces, as its watch task sees them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RepoClaims {
    /// The names this machine claims.
    pub ours: BTreeSet<String>,
    /// The names alive peers claim, with the claiming machines' names.
    pub others: BTreeMap<String, Vec<String>>,
    /// How many names this machine may claim on the repo, within
    /// [`MAX_MACHINE_WORKSPACES`].
    pub limit: usize,
}

impl WorkspaceClaims {
    /// Number of claimed workspaces.
    pub fn count(&self) -> usize {
        self.repos.values().map(BTreeSet::len).sum()
    }

    /// Whether the record only holds non-empty claims on valid names,
    /// within the per-machine cap. Empty claims would cost nothing against
    /// the cap while growing the gossiped membership.
    pub(super) fn is_valid(&self) -> bool {
        self.count() <= MAX_MACHINE_WORKSPACES
            && self.repos.values().all(|names| {
                !names.is_empty()
                    && names
                        .iter()
                        .all(|name| validate_name("workspace", name).is_ok())
            })
    }

    /// Replaces the claims on one repo, bumping the version on change.
    pub(super) fn set(&mut self, repo: &RepoId, names: BTreeSet<String>) -> Result<()> {
        if self
            .repos
            .get(repo)
            .map_or(names.is_empty(), |held| *held == names)
        {
            return Ok(());
        }
        let mut next = self.clone();
        if names.is_empty() {
            next.repos.remove(repo);
        } else {
            next.repos.insert(repo.clone(), names);
        }
        ensure!(
            next.is_valid(),
            "workspace claims must be valid names, at most {MAX_MACHINE_WORKSPACES}",
        );
        next.version = next_version(self.version, "this machine's workspace claims")?;
        *self = next;
        Ok(())
    }

    /// Absorbs a copy of our own claims gossiped back by a peer: our
    /// version moves past any stale copy, so the mesh converges on ours.
    /// Clamped below [`MAX_RECORD_VERSION`], like [`super::Machine`].
    pub(super) fn observe(&mut self, copy: &Self) {
        let outranking = if copy.repos == self.repos {
            copy.version
        } else {
            copy.version + 1
        };
        self.version = self.version.max(outranking).min(MAX_RECORD_VERSION - 1);
    }
}

impl Register for WorkspaceClaims {
    fn version(&self) -> u64 {
        self.version
    }

    /// Higher version first, then the smaller claims: only the owner
    /// writes, but relayed copies must still converge on one record.
    fn outranks(&self, other: &Self) -> bool {
        (self.version, cmp::Reverse(&self.repos)) > (other.version, cmp::Reverse(&other.repos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn set_bumps_only_on_change() {
        let repo = RepoId::generate();
        let mut claims = WorkspaceClaims::default();

        claims.set(&repo, names(&["laptop"])).unwrap();
        assert_eq!(claims.version, 1);
        claims.set(&repo, names(&["laptop"])).unwrap();
        assert_eq!(claims.version, 1);

        claims.set(&repo, BTreeSet::new()).unwrap();
        assert!(claims.repos.is_empty());
        assert_eq!(claims.version, 2);
    }

    #[test]
    fn set_refuses_invalid_claims() {
        let repo = RepoId::generate();
        let mut claims = WorkspaceClaims::default();

        assert!(claims.set(&repo, names(&["bad\u{202E}"])).is_err());
        let many: BTreeSet<String> = (0..=MAX_MACHINE_WORKSPACES)
            .map(|i| format!("w{i}"))
            .collect();
        assert!(claims.set(&repo, many).is_err());
        assert_eq!(claims, WorkspaceClaims::default());
    }

    #[test]
    fn empty_claims_are_invalid() {
        let claims = WorkspaceClaims {
            version: 1,
            repos: BTreeMap::from([(RepoId::generate(), BTreeSet::new())]),
        };
        assert!(!claims.is_valid());
    }

    #[test]
    fn observe_outranks_stale_copies() {
        let repo = RepoId::generate();
        let mut ours = WorkspaceClaims::default();
        ours.set(&repo, names(&["laptop"])).unwrap();

        // A copy from before a local reset: same content keeps pace, other
        // content is outranked.
        let mut copy = ours.clone();
        copy.version = 7;
        ours.observe(&copy);
        assert_eq!(ours.version, 7);
        copy.repos.clear();
        ours.observe(&copy);
        assert_eq!(ours.version, 8);
    }
}

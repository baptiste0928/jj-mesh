//! Orphan announcements: the latest announcement each peer made for repo
//! names not registered here.
//!
//! Peers replay all their repos on connect, so the [`HubState::orphans`]
//! map is how `clone` learns a repo's id and heads and who serves it;
//! [`SyncHub::clone_sources`] resolves that into [`CloneSource`]s. The map
//! is bounded per peer and pruned on disconnect.

use std::collections::BTreeMap;

use color_eyre::eyre::{Result, bail, ensure};
use iroh::EndpointId;

use super::{HubState, SyncHub};
use crate::config::RepoId;

/// Cap on unregistered repo names tracked *per peer*, so one peer (hostile
/// or simply repo-rich) cannot evict the names other peers announce. A
/// peer's entries are pruned when it disconnects.
pub(super) const MAX_ORPHAN_REPOS_PER_PEER: usize = 64;

/// A peer that can serve a `clone` of an unregistered repo, with the op
/// heads it claims.
#[derive(Debug)]
pub struct CloneSource {
    pub peer: EndpointId,
    pub heads: Vec<Vec<u8>>,
}

/// The latest announcement one peer made for an unregistered repo name.
#[derive(Debug, Clone)]
pub(super) struct OrphanAnnounce {
    pub(super) id: RepoId,
    pub(super) heads: Vec<Vec<u8>>,
}

impl HubState {
    /// Records `announce` as `peer`'s latest for the unregistered `name`.
    /// Refuses (returning `false`) a new name that would put the peer over
    /// [`MAX_ORPHAN_REPOS_PER_PEER`]; a known name always updates.
    pub(super) fn remember_orphan(
        &mut self,
        peer: EndpointId,
        name: String,
        announce: OrphanAnnounce,
    ) -> bool {
        let known = self
            .orphans
            .get(&name)
            .is_some_and(|peers| peers.contains_key(&peer));
        if !known && self.orphans_held_by(&peer) >= MAX_ORPHAN_REPOS_PER_PEER {
            return false;
        }
        self.orphans.entry(name).or_default().insert(peer, announce);
        true
    }

    /// Drops `peer`'s entry for `name` (it retracted the repo).
    pub(super) fn forget_orphan(&mut self, name: &str, peer: &EndpointId) {
        if let Some(peers) = self.orphans.get_mut(name) {
            peers.remove(peer);
            if peers.is_empty() {
                self.orphans.remove(name);
            }
        }
    }

    /// Drops every entry `peer` holds (its connection is gone).
    pub(super) fn forget_orphan_peer(&mut self, peer: &EndpointId) {
        self.orphans.retain(|_, peers| {
            peers.remove(peer);
            !peers.is_empty()
        });
    }

    /// Takes every announcement for `name`, which is being registered.
    pub(super) fn take_orphans(&mut self, name: &str) -> BTreeMap<EndpointId, OrphanAnnounce> {
        self.orphans.remove(name).unwrap_or_default()
    }

    /// How many orphan names hold an entry for `peer`, against
    /// [`MAX_ORPHAN_REPOS_PER_PEER`].
    fn orphans_held_by(&self, peer: &EndpointId) -> usize {
        self.orphans
            .values()
            .filter(|peers| peers.contains_key(peer))
            .count()
    }
}

impl SyncHub {
    /// Resolves a `clone` of the repo named `name`: the id every connected
    /// announcing peer agrees on, and each peer's claimed heads. Errors
    /// when nobody announces the name, or when peers disagree on the id
    /// (unrelated repos contesting one name, which only the user can
    /// resolve).
    pub fn clone_sources(&self, name: &str) -> Result<(RepoId, Vec<CloneSource>)> {
        let state = self.state.lock().unwrap();
        let sources: Vec<(EndpointId, OrphanAnnounce)> = state
            .orphans
            .get(name)
            .map(|peers| {
                peers
                    .iter()
                    .filter(|(peer, _)| state.peers.contains_key(*peer))
                    .map(|(peer, announce)| (*peer, announce.clone()))
                    .collect()
            })
            .unwrap_or_default();

        let Some(id) = sources.first().map(|(_, announce)| announce.id.clone()) else {
            bail!(
                "no connected peer announces a repo named `{name}`; check \
                 that the other machine's daemon is running and the name is \
                 correct"
            );
        };
        ensure!(
            sources.iter().all(|(_, announce)| announce.id == id),
            "peers announce different repos under the name `{name}`; \
             resolve the conflict before cloning",
        );

        Ok((
            id,
            sources
                .into_iter()
                .map(|(peer, announce)| CloneSource {
                    peer,
                    heads: announce.heads,
                })
                .collect(),
        ))
    }
}

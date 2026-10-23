//! Orphan announcements: the latest announcement each peer made for repos
//! not registered here.
//!
//! Peers replay all their repos on connect, so the [`HubState::orphans`]
//! map is how `clone` and server mode learn a repo's heads and who serves
//! it; [`SyncHub::clone_sources`] resolves that into
//! [`CloneSource`]s to pull from (see [`super::pull`]). The map is bounded
//! per peer and pruned on disconnect.

use std::collections::BTreeMap;

use color_eyre::eyre::{Result, ensure};
use iroh::EndpointId;

use super::{CloneSource, HubState, SyncHub};
use crate::config::{MAX_MESH_REPOS, RepoId};

/// The latest announcement one peer made for an unregistered repo.
#[derive(Debug, Clone)]
pub(super) struct OrphanAnnounce {
    pub(super) seq: u64,
    pub(super) heads: Vec<Vec<u8>>,
}

impl HubState {
    /// Records `announce` as `peer`'s latest for the unregistered repo
    /// `id`. Refuses (returning `false`) a new repo that would put the peer
    /// over [`MAX_MESH_REPOS`]; a known one always updates.
    pub(super) fn remember_orphan(
        &mut self,
        peer: EndpointId,
        id: RepoId,
        announce: OrphanAnnounce,
    ) -> bool {
        let known = self
            .orphans
            .get(&id)
            .is_some_and(|peers| peers.contains_key(&peer));
        if !known && self.orphans_held_by(&peer) >= MAX_MESH_REPOS {
            return false;
        }
        self.orphans.entry(id).or_default().insert(peer, announce);
        true
    }

    /// Drops `peer`'s entry for `id` (it retracted the repo).
    pub(super) fn forget_orphan(&mut self, id: &RepoId, peer: &EndpointId) {
        if let Some(peers) = self.orphans.get_mut(id) {
            peers.remove(peer);
            if peers.is_empty() {
                self.orphans.remove(id);
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

    /// Takes every announcement for `id`, which is being registered.
    pub(super) fn take_orphans(&mut self, id: &RepoId) -> BTreeMap<EndpointId, OrphanAnnounce> {
        self.orphans.remove(id).unwrap_or_default()
    }

    /// How many orphan repos hold an entry for `peer`, against
    /// [`MAX_MESH_REPOS`].
    fn orphans_held_by(&self, peer: &EndpointId) -> usize {
        self.orphans
            .values()
            .filter(|peers| peers.contains_key(peer))
            .count()
    }
}

impl SyncHub {
    /// Resolves when an orphan announcement may have been remembered since
    /// the last call: a repo may have become clonable. Wakeups coalesce.
    pub async fn orphans_changed(&self) {
        self.orphaned.notified().await;
    }

    /// Resolves a `clone` of the repo `id`, named `name`: each connected
    /// announcing peer's claimed heads. Errors when nobody announces it.
    pub fn clone_sources(&self, id: &RepoId, name: &str) -> Result<Vec<CloneSource>> {
        let state = self.state.lock().unwrap();
        let sources: Vec<CloneSource> = state
            .orphans
            .get(id)
            .into_iter()
            .flatten()
            .filter(|(peer, _)| state.peers.contains_key(*peer))
            .map(|(peer, announce)| CloneSource {
                peer: *peer,
                heads: announce.heads.clone(),
            })
            .collect();
        ensure!(
            !sources.is_empty(),
            "no connected peer holds `{name}`. Check that the other machine's daemon is running"
        );
        Ok(sources)
    }
}

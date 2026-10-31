//! Orphan announcements: the latest announcement each peer made for repos
//! not registered here.
//!
//! Peers replay all their repos on connect, so the [`HubState::orphans`]
//! map is how `clone` and server mode learn a repo's heads and who serves
//! it; [`SyncHub::clone_sources`] resolves that into
//! [`CloneSource`]s to pull from (see [`super::fetch`]). The map is pruned
//! on disconnect.

use std::collections::BTreeMap;

use anyhow::{Result, ensure};
use iroh::EndpointId;
use jj_lib::op_store::OperationId;

use super::{CloneSource, HubState, SyncHub};
use crate::{config::RepoId, hint::hinted, net::sync::Heads};

impl HubState {
    /// Records `heads` as `peer`'s latest for the unregistered repo `id`.
    pub(super) fn remember_orphan(&mut self, peer: EndpointId, id: RepoId, heads: Heads) {
        self.orphans.entry(id).or_default().insert(peer, heads);
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
    pub(super) fn take_orphans(&mut self, id: &RepoId) -> BTreeMap<EndpointId, Heads> {
        self.orphans.remove(id).unwrap_or_default()
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
            .map(|(peer, heads)| CloneSource {
                peer: *peer,
                heads: heads.ids.iter().cloned().map(OperationId::new).collect(),
            })
            .collect();
        ensure!(
            !sources.is_empty(),
            hinted(
                format!("no connected peer holds {name}"),
                "Check that the other machine's daemon is running"
            )
        );
        Ok(sources)
    }
}

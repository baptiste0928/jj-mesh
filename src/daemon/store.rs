//! Owner of the daemon's authoritative mesh state.
//!
//! Every mutation, whether driven by the control socket or by membership
//! gossip, funnels through [`MeshStore::update`]: persist to `mesh.json`,
//! commit in memory, align the live peer and repo sets, and broadcast the
//! membership when it changed. Owning the sets and the hub is what makes
//! that guarantee structural rather than a convention on callers.

use std::sync::{Arc, Mutex};

use color_eyre::eyre::Result;
use iroh::EndpointId;

use super::{hub::SyncHub, peers::PeerSet, repos::RepoSet};
use crate::{
    config::{ConfigDir, Membership, MeshState},
    net::pair,
};

/// The persisted mesh state and everything a change to it must reach.
#[derive(Debug)]
pub struct MeshStore {
    dir: ConfigDir,
    /// This machine's endpoint, under which its own record is gossiped.
    local: EndpointId,
    state: Mutex<MeshState>,
    peers: Arc<PeerSet>,
    repos: Arc<RepoSet>,
    hub: Arc<SyncHub>,
}

impl MeshStore {
    /// Adopts the loaded state: seeds the membership the hub gossips, then
    /// aligns the peer and repo sets with it. Publishing first means a peer
    /// task cannot connect and replay an empty membership.
    pub fn new(
        dir: ConfigDir,
        local: EndpointId,
        state: MeshState,
        peers: Arc<PeerSet>,
        repos: Arc<RepoSet>,
        hub: Arc<SyncHub>,
    ) -> Self {
        hub.publish_membership(state.membership(local));
        peers.sync(&state);
        repos.sync(&state);

        MeshStore {
            dir,
            local,
            state: Mutex::new(state),
            peers,
            repos,
            hub,
        }
    }

    /// Clones the current state.
    pub fn snapshot(&self) -> MeshState {
        self.state.lock().unwrap().clone()
    }

    /// Re-publishes the membership unchanged. Gossip has no
    /// acknowledgements and a membership is only sent when it changes, so a
    /// snapshot dropped under load (a shed stream, a full queue, a failed
    /// save) would otherwise be lost until the next unrelated change: this
    /// is the anti-entropy that heals it.
    pub fn republish_membership(&self) {
        let state = self.state.lock().unwrap();
        self.hub.publish_membership(state.membership(self.local));
    }

    /// Merges a peer's membership; a merge that changes anything is
    /// persisted and re-broadcast, which carries membership across machines
    /// that are not directly exchanging, while one that changes nothing is
    /// silent, which stops the echo.
    pub fn merge_membership(&self, remote: &Membership) -> Result<()> {
        self.update(|state| {
            state.merge_membership(remote, &self.local);
            Ok(())
        })
    }

    /// Registers a paired peer; a no-op when the endpoint is already alive
    /// (idempotent re-pair). The peer set starts connecting and the gossip
    /// introduces the peer to the rest of the mesh as part of the update.
    pub fn add_paired_peer(&self, peer: &pair::PairedPeer) -> Result<()> {
        self.update(|state| {
            if state.peer_name(&peer.endpoint).is_some() {
                return Ok(());
            }
            state.add_peer(peer.endpoint, peer.name.clone())
        })
    }

    /// Mutates the state: persists the change to `mesh.json`, aligns the
    /// peer and repo sets with it, and broadcasts the membership if it
    /// changed. Nothing is committed (in memory or on disk) when the
    /// mutation or the save fails, and a mutation that changes nothing
    /// writes nothing, which is what stops gossip from echoing forever.
    /// The lock is deliberately held across the syncs, so concurrent
    /// mutations apply theirs in commit order.
    pub fn update<T>(&self, mutate: impl FnOnce(&mut MeshState) -> Result<T>) -> Result<T> {
        let mut state = self.state.lock().unwrap();

        let mut next = state.clone();
        let value = mutate(&mut next)?;
        if next == *state {
            return Ok(value);
        }

        next.save(&self.dir)?;
        let membership_changed = next.membership_differs(&state);
        *state = next;

        self.peers.sync(&state);
        self.repos.sync(&state);
        if membership_changed {
            self.hub.publish_membership(state.membership(self.local));
        }

        Ok(value)
    }
}

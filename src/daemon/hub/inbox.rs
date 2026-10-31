//! Per-repo inbound announcement slots.

use std::{collections::BTreeMap, sync::Mutex};

use iroh::EndpointId;
use tokio::sync::Notify;

use crate::net::sync::Heads;

/// An announcement received from a peer, drained by a repo task.
#[derive(Debug)]
pub struct PeerAnnounce {
    pub peer: EndpointId,
    pub heads: Heads,
}

/// Inbound announcements for one repo: the latest per peer, drained by
/// the repo task.
#[derive(Debug, Default)]
pub struct Inbox {
    slots: Mutex<BTreeMap<EndpointId, Slot>>,
    notify: Notify,
}

/// One peer's slot: the announcement with the highest sequence seen. The
/// heads survive draining (marked `drained`), so the peer's last known
/// state can be demoted to an orphan announcement when the repo is
/// unregistered, and the watermark keeps a requeued fetch from replacing
/// a newer announcement.
#[derive(Debug)]
struct Slot {
    heads: Heads,
    drained: bool,
}

impl Inbox {
    /// Stores an announcement unless a newer one was already seen. A
    /// retraction advances the watermark and clears the heads, so a
    /// reordered pre-retraction announcement stays rejected while a later
    /// re-registration's announcements come through.
    pub(super) fn offer(&self, peer: EndpointId, heads: Heads) {
        let drained = heads.is_retraction();
        let mut slots = self.slots.lock().unwrap();
        if slots
            .get(&peer)
            .is_some_and(|held| held.heads.seq >= heads.seq)
        {
            return;
        }
        slots.insert(peer, Slot { heads, drained });
        drop(slots);
        if !drained {
            self.notify.notify_one();
        }
    }

    /// Drops a peer's slot (its connection is gone).
    pub(super) fn forget(&self, peer: &EndpointId) {
        self.slots.lock().unwrap().remove(peer);
    }

    /// The last announcement of every peer, drained or not, for demotion
    /// to orphan announcements on unregistration. Peers that retracted the
    /// repo are not included.
    pub(super) fn snapshot(&self) -> Vec<PeerAnnounce> {
        let slots = self.slots.lock().unwrap();
        slots
            .iter()
            .filter(|(_, slot)| !slot.heads.is_retraction())
            .map(|(peer, slot)| PeerAnnounce {
                peer: *peer,
                heads: slot.heads.clone(),
            })
            .collect()
    }

    /// Resolves when an announcement may be waiting. Consumers should
    /// still [`Self::drain`] on every wake from any source, so a missed
    /// notification is healed by the next one.
    pub async fn changed(&self) {
        self.notify.notified().await;
    }

    /// Takes all undrained announcements, keeping the per-peer sequence
    /// watermarks.
    pub fn drain(&self) -> Vec<PeerAnnounce> {
        let mut slots = self.slots.lock().unwrap();
        slots
            .iter_mut()
            .filter(|(_, slot)| !slot.drained)
            .map(|(peer, slot)| {
                slot.drained = true;
                PeerAnnounce {
                    peer: *peer,
                    heads: slot.heads.clone(),
                }
            })
            .collect()
    }

    /// Restores heads a fetch failed to apply, so the next drain retries
    /// them. A newer announcement arriving meanwhile supersedes them (it
    /// bumps the slot past their sequence), and a fresh connection drops
    /// the slot entirely, so the retry never resurrects stale or revoked
    /// state. Does not notify: the retry is driven by the repo task's own
    /// timer, which avoids hot-looping against a peer that keeps failing.
    pub fn requeue(&self, announce: &PeerAnnounce) {
        let mut slots = self.slots.lock().unwrap();
        if let Some(slot) = slots.get_mut(&announce.peer)
            && slot.heads.seq == announce.heads.seq
        {
            slot.drained = false;
        }
    }
}

//! Per-repo inbound announcement slots.

use std::{collections::BTreeMap, sync::Mutex};

use iroh::EndpointId;
use tokio::sync::Notify;

/// An announcement received from a peer, drained by a repo task.
#[derive(Debug)]
pub struct PeerAnnounce {
    pub peer: EndpointId,
    /// Sequence it was drained at, so a failed fetch can [requeue] the
    /// heads without clobbering a newer announcement.
    ///
    /// [requeue]: Inbox::requeue
    pub seq: u64,
    pub heads: Vec<Vec<u8>>,
}

/// Inbound announcements for one repo: the latest per peer, drained by
/// the repo task.
#[derive(Debug, Default)]
pub struct Inbox {
    slots: Mutex<BTreeMap<EndpointId, Slot>>,
    notify: Notify,
}

/// One peer's slot: the highest announcement sequence seen and its heads.
/// The heads survive draining (marked `drained`), so the peer's last known
/// state can be demoted to an orphan announcement when the repo is
/// unregistered, and the watermark fends off reordered stale
/// announcements arriving after a drain.
#[derive(Debug)]
struct Slot {
    seq: u64,
    heads: Vec<Vec<u8>>,
    drained: bool,
}

impl Inbox {
    /// Stores an announcement unless a newer one was already seen.
    pub(super) fn offer(&self, peer: EndpointId, seq: u64, heads: Vec<Vec<u8>>) {
        {
            let mut slots = self.slots.lock().unwrap();
            if slots.get(&peer).is_some_and(|slot| slot.seq >= seq) {
                return;
            }
            slots.insert(
                peer,
                Slot {
                    seq,
                    heads,
                    drained: false,
                },
            );
        }
        self.notify.notify_one();
    }

    /// Drops a peer's slot (its connection is gone).
    pub(super) fn forget(&self, peer: &EndpointId) {
        self.slots.lock().unwrap().remove(peer);
    }

    /// Records a retraction: the watermark advances and the heads clear,
    /// so a reordered pre-retraction announcement stays rejected while a
    /// later re-registration's announcements come through.
    pub(super) fn retract(&self, peer: EndpointId, seq: u64) {
        let mut slots = self.slots.lock().unwrap();
        if slots.get(&peer).is_some_and(|slot| slot.seq >= seq) {
            return;
        }
        slots.insert(
            peer,
            Slot {
                seq,
                heads: Vec::new(),
                drained: true,
            },
        );
    }

    /// The last announced heads of every peer, drained or not, for
    /// demotion to orphan announcements on unregistration. Peers that
    /// retracted the repo (no heads) are not included.
    pub(super) fn snapshot(&self) -> Vec<(EndpointId, Vec<Vec<u8>>)> {
        let slots = self.slots.lock().unwrap();
        slots
            .iter()
            .filter(|(_, slot)| !slot.heads.is_empty())
            .map(|(peer, slot)| (*peer, slot.heads.clone()))
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
                    seq: slot.seq,
                    heads: slot.heads.clone(),
                }
            })
            .collect()
    }

    /// Restores heads a fetch failed to apply, so the next drain retries
    /// them. A newer announcement arriving meanwhile supersedes them (it
    /// bumps the slot past `seq`), and a fresh connection drops the slot
    /// entirely, so the retry never resurrects stale or revoked state. Does
    /// not notify: the retry is driven by the repo task's own timer, which
    /// avoids hot-looping against a peer that keeps failing.
    pub fn requeue(&self, peer: EndpointId, seq: u64, heads: Vec<Vec<u8>>) {
        let mut slots = self.slots.lock().unwrap();
        if let Some(slot) = slots.get_mut(&peer)
            && slot.seq == seq
            && slot.drained
        {
            slot.heads = heads;
            slot.drained = false;
        }
    }
}

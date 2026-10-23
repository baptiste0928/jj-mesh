//! Routing between peer connections and repo tasks.
//!
//! Peer tasks and repo tasks do not know each other; the hub sits between
//! them and makes announcements genuinely latest-wins in both directions:
//! a per-peer coalescing [`Outbox`] outbound, and per-peer slots on the
//! repo's [`Inbox`], ordered by sequence number, inbound.
//!
//! ```text
//! peer task --route()---> Inbox (per repo) ---drain()--> repo task
//! peer task <--sender---- Outbox (per peer) <-publish()- repo task
//! ```
//!
//! Owning the outboxes, the hub also carries the membership and status
//! reports to peers, remembers announcements for unregistered repos (see
//! [`orphans`]), pulls whole repos from them (see [`pull`]), serves inbound
//! fetches (see [`serve`]), and holds the peers' paired names (see
//! [`SyncHub::peer_name`]).

mod inbox;
mod orphans;
mod outbox;
mod pull;
mod serve;
#[cfg(test)]
mod tests;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use iroh::{EndpointId, endpoint::Connection};
use tokio::sync::Notify;
use tracing::debug;

pub use self::inbox::{Inbox, PeerAnnounce};
pub use self::pull::CloneSource;
use self::{
    orphans::OrphanAnnounce,
    outbox::{Outbox, run_sender},
    serve::Serving,
};
use crate::{
    config::{Membership, RepoId, sanitize},
    net::sync::{Announce, StatusReport},
    repo::OpenRepo,
};

/// Cap on head ids accepted in one announcement; legitimate divergence is
/// a few heads, anything more is a hostile or broken peer.
const MAX_ANNOUNCED_HEADS: usize = 64;

/// Cap on the length of an announced head id: jj's op ids are 64 bytes.
/// Repo tasks check the exact length against their store.
const MAX_HEAD_LEN: usize = 64;

/// The router between peer connections and repo tasks.
#[derive(Debug, Default)]
pub struct SyncHub {
    state: Mutex<HubState>,
    /// Pinged when an orphan announcement is remembered (see
    /// [`SyncHub::orphans_changed`]).
    orphaned: Notify,
}

#[derive(Debug, Default)]
struct HubState {
    repos: BTreeMap<RepoId, RepoEntry>,
    peers: BTreeMap<EndpointId, PeerSender>,
    /// Announcement sequence, monotonic across every repo for the whole
    /// daemon run. Shared rather than per repo so a repo forgotten and
    /// cloned again keeps outranking its old announcements: receivers only
    /// reset their per-repo watermark on a new connection.
    announce_seq: u64,
    /// See [`orphans`].
    orphans: BTreeMap<RepoId, BTreeMap<EndpointId, OrphanAnnounce>>,
    /// This machine's latest membership, replayed to connecting peers and
    /// broadcast on every change (see [`SyncHub::publish_membership`]).
    membership: Membership,
    /// This machine's latest status report, replayed to connecting peers
    /// and broadcast on every change (see [`SyncHub::publish_status`]).
    status: Option<StatusReport>,
    /// The latest (sanitized) status report of each connected peer.
    /// Dropped on disconnect: stale health is worse than none.
    reports: BTreeMap<EndpointId, StatusReport>,
    /// The paired name of every managed peer, set by the peer set.
    names: BTreeMap<EndpointId, String>,
}

/// See [`SyncHub::peer_name`]. Takes the map alone so it can be read
/// while a repo entry is borrowed.
fn peer_name(names: &BTreeMap<EndpointId, String>, peer: &EndpointId) -> String {
    names
        .get(peer)
        .map_or_else(|| peer.fmt_short().to_string(), String::clone)
}

impl HubState {
    /// Queues a message to every connected peer's outbox.
    fn broadcast(&self, push: impl Fn(&Outbox)) {
        for sender in self.peers.values() {
            push(&sender.outbox);
        }
    }

    /// Drops everything learned from `peer` over its previous connection.
    /// Sequence tracking is per connection (a restarted peer daemon starts
    /// over from 1), and orphan announcements and reports come back with
    /// the replay if still real.
    fn forget_peer(&mut self, peer: &EndpointId) {
        for entry in self.repos.values_mut() {
            entry.inbox.forget(peer);
        }
        self.forget_orphan_peer(peer);
        self.reports.remove(peer);
    }
}

/// Hub-side state of one registered repo.
#[derive(Debug)]
struct RepoEntry {
    /// Sequence of the latest publish (drawn from [`HubState::announce_seq`]),
    /// stamped into announcements so receivers can discard reordered ones.
    seq: u64,
    /// Latest published op heads, replayed to connecting peers. `None`
    /// until the repo task first publishes (repo not opened yet).
    published: Option<Vec<Vec<u8>>>,
    inbox: Arc<Inbox>,
    /// Serve handle, present while the repo task has the repo open (see
    /// [`serve`] for why the hub dispatches fetches itself).
    serving: Option<Serving>,
}

/// Hub-side state of one connected peer.
#[derive(Debug)]
struct PeerSender {
    conn: Connection,
    outbox: Arc<Outbox>,
    task: tokio::task::JoinHandle<()>,
}

impl SyncHub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a repo, returning the inbox its task drains inbound
    /// announcements from. Replaces any previous registration of the repo.
    /// Orphan announcements held for it seed the inbox, so they move back
    /// to the orphans on unregistration; the peers that sent them would
    /// otherwise never re-warn an idle repo.
    pub fn register_repo(&self, id: RepoId) -> Arc<Inbox> {
        let inbox = Arc::new(Inbox::default());
        let mut state = self.state.lock().unwrap();
        for (peer, announce) in state.take_orphans(&id) {
            inbox.offer(peer, announce.seq, announce.heads);
        }

        state.repos.insert(
            id,
            RepoEntry {
                seq: 0,
                published: None,
                inbox: inbox.clone(),
                serving: None,
            },
        );
        inbox
    }

    /// Makes an opened repo servable. Called by the repo task once its
    /// stores are open; replaces the handle from a previous open.
    pub fn repo_opened(&self, id: &RepoId, repo: Arc<OpenRepo>) {
        if let Some(entry) = self.state.lock().unwrap().repos.get_mut(id) {
            entry.serving = Some(Serving::new(repo));
        }
    }

    /// Stops serving a repo (its watch failed; the stores may be stale).
    pub fn repo_closed(&self, id: &RepoId) {
        if let Some(entry) = self.state.lock().unwrap().repos.get_mut(id) {
            entry.serving = None;
        }
    }

    /// The live connection to a peer, for opening fetch streams.
    pub fn connection(&self, peer: &EndpointId) -> Option<Connection> {
        let state = self.state.lock().unwrap();
        state.peers.get(peer).map(|sender| sender.conn.clone())
    }

    /// Removes a repo registration (a local forget, a mesh-wide removal,
    /// or a restart of its task).
    ///
    /// The peers' last announcements move back to the orphan store, so the
    /// repo stays immediately clonable here: peers only re-announce on a
    /// head change or a reconnect, which could otherwise be arbitrarily
    /// far away. A retraction (an announcement with no heads) also goes
    /// out, so peers stop counting this machine as a source.
    pub fn unregister_repo(&self, id: &RepoId) {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.remove(id) else {
            return;
        };

        for PeerAnnounce { peer, seq, heads } in entry.inbox.snapshot() {
            state.remember_orphan(peer, id.clone(), OrphanAnnounce { seq, heads });
        }
        self.orphaned.notify_one();

        if entry.published.is_some() {
            state.announce_seq += 1;
            let announce = Announce {
                id: id.clone(),
                seq: state.announce_seq,
                heads: Vec::new(),
            };
            state.broadcast(|outbox| outbox.push_announce(announce.clone()));
        }
    }

    /// Publishes a repo's current op heads: cached for peers that connect
    /// later and queued to every connected peer's outbox, coalescing with
    /// any announcement still pending there.
    pub fn publish(&self, id: &RepoId, heads: Vec<Vec<u8>>) {
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        let Some(entry) = state.repos.get_mut(id) else {
            return;
        };
        state.announce_seq += 1;
        entry.seq = state.announce_seq;
        entry.published = Some(heads.clone());

        let announce = Announce {
            id: id.clone(),
            seq: entry.seq,
            heads,
        };
        state.broadcast(|outbox| outbox.push_announce(announce.clone()));
    }

    /// Publishes this machine's status report: cached for peers that
    /// connect later and queued to every connected peer, coalescing with
    /// any report still pending there.
    pub fn publish_status(&self, report: StatusReport) {
        let mut state = self.state.lock().unwrap();
        state.broadcast(|outbox| outbox.push_status(report.clone()));
        state.status = Some(report);
    }

    /// Publishes this machine's membership: cached for peers that connect
    /// later and queued to every connected peer, coalescing with any
    /// membership still pending there. Called on every membership change,
    /// including changes learned from gossip, which is what makes the
    /// propagation transitive.
    pub fn publish_membership(&self, membership: Membership) {
        let mut state = self.state.lock().unwrap();
        state.broadcast(|outbox| outbox.push_membership(membership.clone()));
        state.membership = membership;
    }

    /// Replaces the paired names of the managed peers.
    pub fn set_peer_names(&self, names: BTreeMap<EndpointId, String>) {
        self.state.lock().unwrap().names = names;
    }

    /// The paired name of a peer, or its short endpoint id when it is not
    /// (or no longer) managed.
    pub fn peer_name(&self, peer: &EndpointId) -> String {
        peer_name(&self.state.lock().unwrap().names, peer)
    }

    /// Marks a peer connected: spawns its sender task and seeds the outbox
    /// with the membership, the status report and every published repo, so a (re)connecting
    /// peer learns state it missed while away. Resets per-peer state (see
    /// [`HubState::forget_peer`]): the connection may replace one whose
    /// loss went unnoticed.
    pub fn peer_connected(&self, peer: EndpointId, conn: &Connection) {
        let outbox = Arc::new(Outbox::default());
        let task = tokio::spawn(run_sender(conn.clone(), outbox.clone()));

        let mut state = self.state.lock().unwrap();
        state.forget_peer(&peer);
        outbox.push_membership(state.membership.clone());
        if let Some(report) = &state.status {
            outbox.push_status(report.clone());
        }
        for (id, entry) in &state.repos {
            if let Some(heads) = &entry.published {
                outbox.push_announce(Announce {
                    id: id.clone(),
                    seq: entry.seq,
                    heads: heads.clone(),
                });
            }
        }
        let previous = state.peers.insert(
            peer,
            PeerSender {
                conn: conn.clone(),
                outbox,
                task,
            },
        );
        if let Some(previous) = previous {
            previous.task.abort();
        }
    }

    /// Marks a peer disconnected, closing its connection: the caller may
    /// be revoking the peer, and a revoked peer must stop receiving
    /// announcements even if the removal raced connection setup.
    pub fn peer_disconnected(&self, peer: &EndpointId) {
        let removed = {
            let mut state = self.state.lock().unwrap();
            // Per-peer state is dropped even when no sender was registered
            // (the peer may be mid-setup). Pruning orphans here also keeps
            // a revoked peer's entries from lingering forever.
            state.forget_peer(peer);
            state.peers.remove(peer)
        };

        if let Some(sender) = removed {
            sender.task.abort();
            sender.conn.close(0u32.into(), b"peer removed");
        }
    }

    /// Routes an inbound announcement to its repo's inbox; announcements
    /// for unregistered repos are remembered for `clone`.
    ///
    /// An announcement without heads is a retraction (the peer forgot its
    /// instance; a held repo always has at least one op head): it releases
    /// everything attributed to the peer for the repo, like a disconnect
    /// scoped to one repo.
    pub fn route(&self, peer: EndpointId, announce: Announce) {
        // Bounds what an announcement holds, kept as an orphan or in an
        // inbox.
        if announce.heads.len() > MAX_ANNOUNCED_HEADS
            || announce.heads.iter().any(|head| head.len() > MAX_HEAD_LEN)
        {
            let peer = self.peer_name(&peer);
            debug!(%peer, "dropping malformed announcement");
            return;
        }
        let retraction = announce.heads.is_empty();

        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        let Some(entry) = state.repos.get_mut(&announce.id) else {
            if retraction {
                state.forget_orphan(&announce.id, &peer);
                return;
            }
            let orphan = OrphanAnnounce {
                seq: announce.seq,
                heads: announce.heads,
            };
            if state.remember_orphan(peer, announce.id, orphan) {
                self.orphaned.notify_one();
            } else {
                let peer = peer_name(&state.names, &peer);
                debug!(%peer, "dropping announcement: too many unregistered repos");
            }
            return;
        };

        if retraction {
            entry.inbox.retract(peer, announce.seq);
        } else {
            entry.inbox.offer(peer, announce.seq, announce.heads);
        }
    }

    /// Stores a peer's status report for `jj-mesh status`. The peer is
    /// authenticated but its strings reach the user's terminal, so the
    /// free-form fields are sanitized.
    pub fn route_status(&self, peer: EndpointId, mut report: StatusReport) {
        report.daemon_version = sanitize(&report.daemon_version);
        report.jj_version = report.jj_version.as_deref().map(sanitize);

        let mut state = self.state.lock().unwrap();
        // Only connected peers may hold a slot, or a report racing its
        // disconnect would be retained forever.
        if state.peers.contains_key(&peer) {
            state.reports.insert(peer, report);
        }
    }

    /// The latest status report of every connected peer.
    pub fn peer_reports(&self) -> Vec<(EndpointId, StatusReport)> {
        let state = self.state.lock().unwrap();
        state
            .reports
            .iter()
            .map(|(peer, report)| (*peer, report.clone()))
            .collect()
    }
}

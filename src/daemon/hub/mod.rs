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
//! [`orphans`]), fetches from peers (see [`fetch`]), serves their fetches
//! (see [`serve`]), and holds the peers' paired names (see
//! [`SyncHub::peer_name`]).

mod fetch;
mod inbox;
mod orphans;
mod outbox;
mod serve;
#[cfg(test)]
mod tests;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use iroh::{EndpointId, endpoint::Connection};
use tokio::sync::Notify;

pub use self::fetch::CloneSource;
pub use self::inbox::{Inbox, PeerAnnounce};
use self::{
    outbox::{Outbox, run_sender},
    serve::Serving,
};
use crate::{
    config::{Membership, RepoId, sanitize},
    net::sync::{Announce, Heads, StatusReport, UniMessage},
    repo::OpenRepo,
};

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
    orphans: BTreeMap<RepoId, BTreeMap<EndpointId, Heads>>,
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
    fn broadcast(&self, message: &UniMessage) {
        for sender in self.peers.values() {
            sender.outbox.push(message.clone());
        }
    }

    /// Stamps a repo's op heads with the next publish sequence and queues
    /// them to every peer.
    fn announce(&mut self, id: &RepoId, ids: Vec<Vec<u8>>) -> Heads {
        self.announce_seq += 1;
        let heads = Heads {
            seq: self.announce_seq,
            ids,
        };
        self.broadcast(&UniMessage::Announce(Announce {
            id: id.clone(),
            heads: heads.clone(),
        }));
        heads
    }

    /// Announces a registered repo's op heads, recording them for replay.
    fn publish(&mut self, id: &RepoId, ids: Vec<Vec<u8>>) {
        if !self.repos.contains_key(id) {
            return;
        }
        let heads = self.announce(id, ids);
        if let Some(entry) = self.repos.get_mut(id) {
            entry.published = Some(heads);
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
    /// Latest published op heads, replayed to connecting peers. `None`
    /// until the repo task first publishes (repo not opened yet).
    published: Option<Heads>,
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
        for (peer, heads) in state.take_orphans(&id) {
            inbox.offer(peer, heads);
        }

        state.repos.insert(
            id,
            RepoEntry {
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
        if let Some(entry) = self.state.lock().unwrap().repos.get_mut(id)
            && let Some(old) = entry.serving.replace(Serving::new(repo))
        {
            old.close();
        }
    }

    /// Stops serving a repo (its watch failed; the stores may be stale).
    pub fn repo_closed(&self, id: &RepoId) {
        if let Some(entry) = self.state.lock().unwrap().repos.get_mut(id)
            && let Some(serving) = entry.serving.take()
        {
            serving.close();
        }
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
        if let Some(serving) = &entry.serving {
            serving.close();
        }

        for PeerAnnounce { peer, heads } in entry.inbox.snapshot() {
            state.remember_orphan(peer, id.clone(), heads);
        }
        self.orphaned.notify_one();

        if entry.published.is_some() {
            state.announce(id, Vec::new());
        }
    }

    /// Publishes a repo's current op heads: cached for peers that connect
    /// later and queued to every connected peer's outbox, coalescing with
    /// any announcement still pending there.
    pub fn publish(&self, id: &RepoId, ids: Vec<Vec<u8>>) {
        self.state.lock().unwrap().publish(id, ids);
    }

    /// Publishes this machine's status report: cached for peers that
    /// connect later and queued to every connected peer, coalescing with
    /// any report still pending there.
    pub fn publish_status(&self, report: StatusReport) {
        let mut state = self.state.lock().unwrap();
        state.broadcast(&UniMessage::Status(report.clone()));
        state.status = Some(report);
    }

    /// Publishes this machine's membership: cached for peers that connect
    /// later and queued to every connected peer, coalescing with any
    /// membership still pending there. Called on every membership change,
    /// including changes learned from gossip, which is what makes the
    /// propagation transitive.
    pub fn publish_membership(&self, membership: Membership) {
        let mut state = self.state.lock().unwrap();
        state.broadcast(&UniMessage::Membership(membership.clone()));
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
    /// with the membership, the status report and every published repo, so
    /// a (re)connecting peer learns state it missed while away. Resets
    /// per-peer state (see [`HubState::forget_peer`]): the connection may
    /// replace one whose loss went unnoticed.
    pub fn peer_connected(&self, peer: EndpointId, conn: &Connection) {
        let outbox = Arc::new(Outbox::default());
        let task = tokio::spawn(run_sender(conn.clone(), outbox.clone()));

        let mut state = self.state.lock().unwrap();
        state.forget_peer(&peer);
        outbox.push(UniMessage::Membership(state.membership.clone()));
        if let Some(report) = &state.status {
            outbox.push(UniMessage::Status(report.clone()));
        }
        for (id, entry) in &state.repos {
            if let Some(heads) = &entry.published {
                outbox.push(UniMessage::Announce(Announce {
                    id: id.clone(),
                    heads: heads.clone(),
                }));
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
    /// A retraction (see [`Heads`]) releases everything attributed to the
    /// peer for the repo, like a disconnect scoped to one repo.
    pub fn route(&self, peer: EndpointId, announce: Announce) {
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.repos.get(&announce.id) {
            return entry.inbox.offer(peer, announce.heads);
        }
        if announce.heads.is_retraction() {
            state.forget_orphan(&announce.id, &peer);
        } else {
            state.remember_orphan(peer, announce.id, announce.heads);
            self.orphaned.notify_one();
        }
    }

    /// Stores a peer's status report for `jj-mesh status`. Its strings
    /// reach the user's terminal, so the free-form fields are sanitized.
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

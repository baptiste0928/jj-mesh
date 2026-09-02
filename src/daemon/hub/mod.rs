//! Routing between peer connections and repo tasks.
//!
//! Peer tasks and repo tasks do not know each other; the hub sits between
//! them and makes announcements genuinely latest-wins in both directions:
//!
//! - Outbound, each connected peer has one sender task draining a per-repo
//!   coalescing [`Outbox`]: publishes overwrite the pending entry for their
//!   repo, sends are sequential per peer, and a (re)connecting peer's
//!   outbox is seeded with every published repo (anti-entropy replay).
//! - Inbound, announcements land in a per-peer slot on the repo's
//!   [`Inbox`], newer sequence numbers overwriting older ones, so reordered
//!   streams and slow repo tasks can never make stale state win.
//!
//! ```text
//! peer task --route()---> Inbox (per repo) ---drain()--> repo task
//! peer task <--sender---- Outbox (per peer) <-publish()- repo task
//! ```
//!
//! Repos are identified by name. An announcement whose name matches a
//! registered repo but whose id differs means two unrelated repos contest
//! the name: it is never synced (that would merge unrelated histories) and
//! the conflict is surfaced through the status. Announcements for names not
//! registered here are remembered for `clone` (see [`orphans`]).
//! Disconnecting a peer closes its connection through the hub: revocation
//! must sever announcements even when it races connection setup.
//!
//! The hub also carries the machine's latest membership and status report,
//! since it owns the outboxes: both are published here on every change and
//! replayed (before any announcement) to every connecting peer. It holds
//! the latest report of each connected peer as well. And it serves inbound
//! fetches for the repos it has open (see [`serve`]).

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
use tracing::{debug, warn};

pub use self::inbox::{Inbox, PeerAnnounce};
pub use self::orphans::CloneSource;
use self::{
    orphans::OrphanAnnounce,
    outbox::{Outbox, run_sender},
    serve::Serving,
};
use crate::{
    config::{Membership, RepoId, sanitize, validate_name},
    net::sync::{Announce, StatusReport},
    repo::OpenRepo,
};

/// The router between peer connections and repo tasks.
#[derive(Debug, Default)]
pub struct SyncHub {
    state: Mutex<HubState>,
}

#[derive(Debug, Default)]
struct HubState {
    repos: BTreeMap<String, RepoEntry>,
    peers: BTreeMap<EndpointId, PeerSender>,
    /// Announcement sequence, monotonic across every repo for the whole
    /// daemon run. Shared rather than per repo so a repo forgotten and
    /// cloned again keeps outranking its old announcements: receivers only
    /// reset their per-repo watermark when the connection drops.
    announce_seq: u64,
    /// See [`orphans`].
    orphans: BTreeMap<String, BTreeMap<EndpointId, OrphanAnnounce>>,
    /// This machine's latest membership, replayed to connecting peers and
    /// broadcast on every change (see [`SyncHub::publish_membership`]).
    membership: Membership,
    /// This machine's latest status report, replayed to connecting peers
    /// and broadcast on every change (see [`SyncHub::publish_status`]).
    status: Option<StatusReport>,
    /// The latest (sanitized) status report of each connected peer.
    /// Dropped on disconnect: stale health is worse than none.
    reports: BTreeMap<EndpointId, StatusReport>,
}

impl HubState {
    /// Queues a message to every connected peer's outbox.
    fn broadcast(&self, push: impl Fn(&Outbox)) {
        for sender in self.peers.values() {
            push(&sender.outbox);
        }
    }
}

/// Hub-side state of one registered repo.
#[derive(Debug)]
struct RepoEntry {
    /// The local id of the repo, checked against announcements and fetch
    /// requests: a name match with a different id is a conflict, never a
    /// sync.
    id: RepoId,
    /// Sequence of the latest publish (drawn from [`HubState::announce_seq`]),
    /// stamped into announcements so receivers can discard reordered ones.
    seq: u64,
    /// Latest published op heads, replayed to connecting peers. `None`
    /// until the repo task first publishes (repo not opened yet).
    published: Option<Vec<Vec<u8>>>,
    inbox: Arc<Inbox>,
    /// Peers whose last announcement for this name carried a different id.
    /// An entry clears when its peer announces the matching id again or
    /// disconnects.
    conflicts: BTreeMap<EndpointId, RepoId>,
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
    /// announcements from. Replaces any previous registration for the name.
    /// Orphan announcements held for the name become conflicts when their
    /// id disagrees; the peers that sent them would otherwise never re-warn
    /// an idle repo.
    pub fn register_repo(&self, name: String, id: RepoId) -> Arc<Inbox> {
        let inbox = Arc::new(Inbox::default());
        let mut state = self.state.lock().unwrap();

        let mut conflicts = BTreeMap::new();
        for (peer, announce) in state.take_orphans(&name) {
            if announce.id != id {
                warn!(
                    repo = %name, peer = %peer,
                    "peer announces a different repo under this name; not syncing with it",
                );
                conflicts.insert(peer, announce.id);
            }
        }

        state.repos.insert(
            name,
            RepoEntry {
                id,
                seq: 0,
                published: None,
                inbox: inbox.clone(),
                conflicts,
                serving: None,
            },
        );
        inbox
    }

    /// Makes an opened repo servable. Called by the repo task once its
    /// stores are open; replaces the handle from a previous open. The id
    /// guards against a stale task of a replaced same-name repo installing
    /// the wrong stores (aborts only land at the task's next await point).
    pub fn repo_opened(&self, name: &str, id: &RepoId, repo: Arc<OpenRepo>) {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap()
            .repos
            .get_mut(name)
            .filter(|entry| &entry.id == id)
        {
            entry.serving = Some(Serving::new(repo));
        }
    }

    /// Stops serving a repo (its watch failed; the stores may be stale).
    pub fn repo_closed(&self, name: &str, id: &RepoId) {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap()
            .repos
            .get_mut(name)
            .filter(|entry| &entry.id == id)
        {
            entry.serving = None;
        }
    }

    /// The live connection to a peer, for opening fetch streams.
    pub fn connection(&self, peer: &EndpointId) -> Option<Connection> {
        let state = self.state.lock().unwrap();
        state.peers.get(peer).map(|sender| sender.conn.clone())
    }

    /// Removes a repo registration (a local forget, a mesh-wide removal,
    /// or a same-name replacement).
    ///
    /// The peers' last announcements move back to the orphan store, so the
    /// repo stays immediately clonable here: peers only re-announce on a
    /// head change or a reconnect, which could otherwise be arbitrarily
    /// far away. A retraction (an announcement with no heads) also goes
    /// out, so peers holding a name conflict against this instance release
    /// it instead of staying stuck until this machine disconnects.
    pub fn unregister_repo(&self, name: &str) {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.remove(name) else {
            return;
        };

        for (peer, heads) in entry.inbox.snapshot() {
            state.remember_orphan(
                peer,
                name.to_owned(),
                OrphanAnnounce {
                    id: entry.id.clone(),
                    heads,
                },
            );
        }

        if entry.published.is_some() {
            state.announce_seq += 1;
            let announce = Announce {
                name: name.to_owned(),
                id: entry.id,
                seq: state.announce_seq,
                heads: Vec::new(),
            };
            state.broadcast(|outbox| outbox.push_announce(announce.clone()));
        }
    }

    /// Publishes a repo's current op heads: cached for peers that connect
    /// later and queued to every connected peer's outbox, coalescing with
    /// any announcement still pending there. The id guards against a stale
    /// task of a replaced same-name repo announcing the wrong heads.
    pub fn publish(&self, name: &str, id: &RepoId, heads: Vec<Vec<u8>>) {
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        let Some(entry) = state.repos.get_mut(name).filter(|entry| &entry.id == id) else {
            return;
        };
        state.announce_seq += 1;
        entry.seq = state.announce_seq;
        entry.published = Some(heads.clone());

        let announce = Announce {
            name: name.to_owned(),
            id: entry.id.clone(),
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

    /// Marks a peer connected: spawns its sender task and seeds the outbox
    /// with the membership and every published repo, so a (re)connecting
    /// peer learns state it missed while away.
    pub fn peer_connected(&self, peer: EndpointId, conn: &Connection) {
        let outbox = Arc::new(Outbox::default());
        let task = tokio::spawn(run_sender(conn.clone(), outbox.clone()));

        let mut state = self.state.lock().unwrap();
        outbox.push_membership(state.membership.clone());
        if let Some(report) = &state.status {
            outbox.push_status(report.clone());
        }
        for (name, entry) in &state.repos {
            if let Some(heads) = &entry.published {
                outbox.push_announce(Announce {
                    name: name.clone(),
                    id: entry.id.clone(),
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
            // (the peer may be mid-setup): sequence tracking is per
            // connection (a restarted peer daemon starts over from 1), and
            // conflicts and orphan announcements attributed to the peer
            // come back with the replay if still real. Pruning orphans here
            // also keeps a revoked peer's entries from lingering forever.
            for entry in state.repos.values_mut() {
                entry.inbox.forget(peer);
                entry.conflicts.remove(peer);
            }
            state.forget_orphan_peer(peer);
            state.reports.remove(peer);
            state.peers.remove(peer)
        };

        if let Some(sender) = removed {
            sender.task.abort();
            sender.conn.close(0u32.into(), b"peer removed");
        }
    }

    /// Routes an inbound announcement to its repo's inbox; announcements
    /// for unregistered names are remembered for `clone`, and a registered
    /// name announced with a different id is recorded as a conflict.
    ///
    /// An announcement without heads is a retraction (the peer forgot its
    /// instance; a held repo always has at least one op head): it releases
    /// everything attributed to the peer for the name, like a disconnect
    /// scoped to one repo.
    pub fn route(&self, peer: EndpointId, announce: Announce) {
        // Repo names arriving from peers are validated before any use:
        // they end up in logs and as map keys, and must never carry
        // unbounded length or confusables.
        if validate_name("repo", &announce.name).is_err() {
            debug!(peer = %peer, "dropping announcement with an invalid repo name");
            return;
        }
        let retraction = announce.heads.is_empty();

        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get_mut(&announce.name) else {
            if retraction {
                state.forget_orphan(&announce.name, &peer);
                return;
            }
            let remembered = state.remember_orphan(
                peer,
                announce.name,
                OrphanAnnounce {
                    id: announce.id,
                    heads: announce.heads,
                },
            );
            if !remembered {
                debug!(peer = %peer, "dropping announcement: too many unregistered repos");
            }
            return;
        };

        if retraction {
            entry.conflicts.remove(&peer);
            entry.inbox.retract(peer, announce.seq);
            return;
        }

        if entry.id != announce.id {
            if !entry.conflicts.contains_key(&peer) {
                warn!(
                    repo = %announce.name, peer = %peer,
                    "peer announces a different repo under this name; not syncing with it",
                );
            }
            entry.conflicts.insert(peer, announce.id);
            return;
        }
        entry.conflicts.remove(&peer);
        entry.inbox.offer(peer, announce.seq, announce.heads);
    }

    /// The current name conflicts, one entry per contesting peer.
    pub fn conflicts(&self) -> Vec<(String, EndpointId)> {
        let state = self.state.lock().unwrap();
        state
            .repos
            .iter()
            .flat_map(|(name, entry)| entry.conflicts.keys().map(|peer| (name.clone(), *peer)))
            .collect()
    }

    /// Stores a peer's status report for `jj-mesh status`. The peer is
    /// authenticated but its strings reach the user's terminal, so the
    /// free-form fields are sanitized and invalid repo names dropped.
    pub fn route_status(&self, peer: EndpointId, mut report: StatusReport) {
        report.daemon_version = sanitize(&report.daemon_version);
        report.jj_version = report.jj_version.as_deref().map(sanitize);
        report
            .repos
            .retain(|repo| validate_name("repo", &repo.name).is_ok());

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

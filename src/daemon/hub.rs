//! Routing between peer connections and repo tasks.
//!
//! Peer tasks and repo tasks do not know each other; the hub sits between
//! them and makes announcements genuinely latest-wins in both directions:
//!
//! - Outbound, each connected peer has one sender task draining a per-repo
//!   coalescing outbox: publishes overwrite the pending entry for their
//!   repo, sends are sequential per peer, and a (re)connecting peer's
//!   outbox is seeded with every published repo (anti-entropy replay).
//! - Inbound, announcements land in a per-peer slot on the repo's inbox,
//!   newer sequence numbers overwriting older ones, so reordered streams
//!   and slow repo tasks can never make stale state win.
//!
//! Unknown repo ids are ignored (the repo is not registered here), and
//! disconnecting a peer closes its connection through the hub: revocation
//! must sever announcements even when it races connection setup.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use tokio::sync::Notify;
use tracing::debug;

use crate::{
    config::RepoId,
    net::{
        sync::{self, Announce, FetchRequest, MAX_OP_FRAME_SIZE, OpFrame},
        wire,
    },
    repo::{MeshRepo, transfer},
};

/// Budget for sending one announcement; a stalled peer connection kills
/// its sender task (the reconnect replay recovers the state).
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(10);

/// An announcement received from a peer, drained by a repo task.
#[derive(Debug)]
pub struct PeerAnnounce {
    pub peer: EndpointId,
    pub heads: Vec<Vec<u8>>,
}

/// Fetches served concurrently per repo (read-only on the repo).
const MAX_SERVES: usize = 2;

/// Hard budget on serving one fetch; QUIC flow control means a stalled
/// fetcher could otherwise pin a serve task and its permit forever.
const SERVE_TIMEOUT: Duration = Duration::from_mins(30);

/// The router between peer connections and repo tasks.
#[derive(Debug, Default)]
pub struct SyncHub {
    state: Mutex<HubState>,
}

#[derive(Debug, Default)]
struct HubState {
    repos: BTreeMap<RepoId, RepoEntry>,
    peers: BTreeMap<EndpointId, PeerSender>,
    /// Latest announcement per peer for repos not registered here. Peers
    /// replay all their repos on connect, so this is how `join` learns a
    /// repo's heads and who serves it. Bounded; stale entries are healed
    /// like any announcement.
    orphans: BTreeMap<RepoId, BTreeMap<EndpointId, Vec<Vec<u8>>>>,
}

/// Cap on tracked unregistered repos; beyond it new ones are dropped
/// (join for them recovers on the next announcement after space frees).
const MAX_ORPHAN_REPOS: usize = 64;

/// Hub-side state of one registered repo.
#[derive(Debug)]
struct RepoEntry {
    /// Sequence of the latest publish, stamped into announcements so
    /// receivers can discard reordered ones. Monotonic per daemon run.
    seq: u64,
    /// Latest published op heads, replayed to connecting peers. `None`
    /// until the repo task first publishes (repo not opened yet).
    published: Option<Vec<Vec<u8>>>,
    inbox: Arc<Inbox>,
    /// Serve handle, present while the repo task has the repo open.
    /// Serving is read-only and dispatched straight from the hub: it must
    /// never depend on the repo task's loop, which may itself be blocked
    /// fetching from the very peer whose fetch we are serving.
    serving: Option<Serving>,
}

/// What the hub needs to serve fetches for an open repo.
#[derive(Debug, Clone)]
struct Serving {
    repo: Arc<MeshRepo>,
    permits: Arc<tokio::sync::Semaphore>,
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
    /// announcements from. Replaces any previous registration for the id.
    pub fn register_repo(&self, id: RepoId) -> Arc<Inbox> {
        let inbox = Arc::new(Inbox::default());
        let mut state = self.state.lock().unwrap();
        state.orphans.remove(&id);
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
    pub fn repo_opened(&self, id: &RepoId, repo: Arc<MeshRepo>) {
        if let Some(entry) = self.state.lock().unwrap().repos.get_mut(id) {
            entry.serving = Some(Serving {
                repo,
                permits: Arc::new(tokio::sync::Semaphore::new(MAX_SERVES)),
            });
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

    /// Serves an inbound fetch on a detached task, or refuses it when the
    /// repo is not open here or too busy.
    pub fn serve_fetch(
        &self,
        peer: EndpointId,
        request: FetchRequest,
        mut send: SendStream,
        mut recv: RecvStream,
    ) {
        let serving = {
            let state = self.state.lock().unwrap();
            state
                .repos
                .get(&request.repo)
                .and_then(|entry| entry.serving.clone())
        };

        let message = match serving {
            None => {
                debug!(repo = %request.repo, "refusing fetch: repo not open here");
                "repo not available"
            }
            Some(serving) => match serving.permits.clone().try_acquire_owned() {
                Err(_) => {
                    debug!(repo = %request.repo, "refusing fetch: too many being served");
                    "busy, retry later"
                }
                Ok(permit) => {
                    tokio::spawn(async move {
                        let _permit = permit;
                        let serve = transfer::serve(&serving.repo, request, &mut send, &mut recv);
                        match tokio::time::timeout(SERVE_TIMEOUT, serve).await {
                            Ok(Ok(())) => {
                                let _ = send.finish();
                                debug!(peer = %peer, "served fetch");
                            }
                            Ok(Err(err)) => debug!(peer = %peer, "serve failed: {err:#}"),
                            Err(_) => debug!(peer = %peer, "serve timed out"),
                        }
                    });
                    return;
                }
            },
        };

        tokio::spawn(async move {
            let frame = OpFrame::Error {
                message: message.to_owned(),
            };
            let _ = wire::write_message(&mut send, &frame, MAX_OP_FRAME_SIZE).await;
            let _ = send.finish();
        });
    }

    /// Removes a repo registration.
    pub fn unregister_repo(&self, id: &RepoId) {
        self.state.lock().unwrap().repos.remove(id);
    }

    /// Publishes a repo's current op heads: cached for peers that connect
    /// later and queued to every connected peer's outbox, coalescing with
    /// any announcement still pending there.
    pub fn publish(&self, id: &RepoId, heads: Vec<Vec<u8>>) {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get_mut(id) else {
            return;
        };
        entry.seq += 1;
        entry.published = Some(heads.clone());

        let announce = Announce {
            repo: id.clone(),
            seq: entry.seq,
            heads,
        };
        for sender in state.peers.values() {
            sender.outbox.push(announce.clone());
        }
    }

    /// Marks a peer connected: spawns its sender task and seeds the outbox
    /// with every published repo, so a (re)connecting peer learns state it
    /// missed while away.
    pub fn peer_connected(&self, peer: EndpointId, conn: &Connection) {
        let outbox = Arc::new(Outbox::default());
        let task = tokio::spawn(run_sender(conn.clone(), outbox.clone()));

        let mut state = self.state.lock().unwrap();
        for (id, entry) in &state.repos {
            if let Some(heads) = &entry.published {
                outbox.push(Announce {
                    repo: id.clone(),
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
            let removed = state.peers.remove(peer);
            if removed.is_some() {
                // Sequence tracking is per connection: a restarted peer
                // daemon starts over from 1.
                for entry in state.repos.values() {
                    entry.inbox.forget(peer);
                }
            }
            removed
        };

        if let Some(sender) = removed {
            sender.task.abort();
            sender.conn.close(0u32.into(), b"peer removed");
        }
    }

    /// Routes an inbound announcement to its repo's inbox; announcements
    /// for unregistered repos are remembered for `join`.
    pub fn route(&self, peer: EndpointId, announce: Announce) {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get(&announce.repo) else {
            if state.orphans.len() >= MAX_ORPHAN_REPOS
                && !state.orphans.contains_key(&announce.repo)
            {
                debug!(repo = %announce.repo, "dropping announcement for unregistered repo");
                return;
            }
            state
                .orphans
                .entry(announce.repo)
                .or_default()
                .insert(peer, announce.heads);
            return;
        };
        entry.inbox.offer(peer, announce.seq, announce.heads);
    }

    /// Peers announcing an unregistered repo, with the heads they claim.
    /// Only currently connected peers are returned.
    pub fn announced_by(&self, id: &RepoId) -> Vec<(EndpointId, Vec<Vec<u8>>)> {
        let state = self.state.lock().unwrap();
        state
            .orphans
            .get(id)
            .map(|peers| {
                peers
                    .iter()
                    .filter(|(peer, _)| state.peers.contains_key(*peer))
                    .map(|(peer, heads)| (*peer, heads.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Inbound announcements for one repo: the latest per peer, drained by
/// the repo task.
#[derive(Debug, Default)]
pub struct Inbox {
    slots: Mutex<BTreeMap<EndpointId, Slot>>,
    notify: Notify,
}

/// One peer's slot: the highest announcement sequence seen, and the heads
/// not yet drained (`None` once consumed; the watermark stays to fend off
/// reordered stale announcements arriving after a drain).
#[derive(Debug)]
struct Slot {
    seq: u64,
    heads: Option<Vec<Vec<u8>>>,
}

impl Inbox {
    /// Stores an announcement unless a newer one was already seen.
    fn offer(&self, peer: EndpointId, seq: u64, heads: Vec<Vec<u8>>) {
        {
            let mut slots = self.slots.lock().unwrap();
            if slots.get(&peer).is_some_and(|slot| slot.seq >= seq) {
                return;
            }
            slots.insert(
                peer,
                Slot {
                    seq,
                    heads: Some(heads),
                },
            );
        }
        self.notify.notify_one();
    }

    /// Drops a peer's slot (its connection is gone).
    fn forget(&self, peer: &EndpointId) {
        self.slots.lock().unwrap().remove(peer);
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
            .filter_map(|(peer, slot)| {
                slot.heads
                    .take()
                    .map(|heads| PeerAnnounce { peer: *peer, heads })
            })
            .collect()
    }
}

/// Pending announcements for one peer, coalesced per repo: only the
/// latest head set of each repo is kept until the sender task takes it.
#[derive(Debug, Default)]
struct Outbox {
    pending: Mutex<BTreeMap<RepoId, Announce>>,
    notify: Notify,
}

impl Outbox {
    fn push(&self, announce: Announce) {
        self.pending
            .lock()
            .unwrap()
            .insert(announce.repo.clone(), announce);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<Announce> {
        self.pending
            .lock()
            .unwrap()
            .pop_first()
            .map(|(_, announce)| announce)
    }
}

/// Sends a peer's outbox until its connection fails; announcements lost
/// with the connection are recovered by the reconnect replay.
async fn run_sender(conn: Connection, outbox: Arc<Outbox>) {
    loop {
        let Some(announce) = outbox.pop() else {
            outbox.notify.notified().await;
            continue;
        };
        match tokio::time::timeout(ANNOUNCE_TIMEOUT, sync::send_announce(&conn, &announce)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return debug!("announcement failed: {err:#}"),
            Err(_) => return debug!("announcement timed out"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn announce(repo: &RepoId, seq: u64, heads: Vec<Vec<u8>>) -> Announce {
        Announce {
            repo: repo.clone(),
            seq,
            heads,
        }
    }

    #[tokio::test]
    async fn routes_to_registered_repo() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo(id.clone());

        let peer = iroh::SecretKey::generate().public();
        hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));

        let drained = inbox.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].peer, peer);
        assert_eq!(drained[0].heads, vec![vec![1; 64]]);
        assert!(inbox.drain().is_empty());
    }

    #[tokio::test]
    async fn discards_reordered_announcements() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo(id.clone());
        let peer = iroh::SecretKey::generate().public();

        hub.route(peer, announce(&id, 2, vec![vec![2; 64]]));
        hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));

        let drained = inbox.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].heads, vec![vec![2; 64]]);

        // The watermark survives draining: the stale announcement stays
        // rejected even when it arrives afterwards.
        hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
        assert!(inbox.drain().is_empty());
    }

    #[tokio::test]
    async fn ignores_unregistered_repo() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo(id.clone());
        hub.unregister_repo(&id);

        let peer = iroh::SecretKey::generate().public();
        hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
        assert!(inbox.drain().is_empty());
    }

    #[tokio::test]
    async fn remembers_unregistered_announcements_for_join() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let peer = iroh::SecretKey::generate().public();

        // Not returned while the peer is not connected.
        hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
        assert!(hub.announced_by(&id).is_empty());

        // Registering the repo claims the orphan entry.
        let inbox = hub.register_repo(id.clone());
        assert!(hub.announced_by(&id).is_empty());
        hub.route(peer, announce(&id, 2, vec![vec![2; 64]]));
        assert_eq!(inbox.drain().len(), 1);
    }

    #[test]
    fn outbox_coalesces_per_repo() {
        let outbox = Outbox::default();
        let id = RepoId::generate();
        let other = RepoId::generate();

        outbox.push(announce(&id, 1, vec![vec![1; 64]]));
        outbox.push(announce(&other, 1, vec![vec![3; 64]]));
        outbox.push(announce(&id, 2, vec![vec![2; 64]]));

        let sent: Vec<Announce> = std::iter::from_fn(|| outbox.pop()).collect();
        assert_eq!(sent.len(), 2);
        assert!(sent.iter().any(|a| a.repo == id && a.seq == 2));
        assert!(sent.iter().any(|a| a.repo == other && a.seq == 1));
    }
}

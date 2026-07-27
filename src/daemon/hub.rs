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
//! Repos are identified by name. An announcement whose name matches a
//! registered repo but whose id differs means two unrelated repos contest
//! the name: it is never synced (that would merge unrelated histories) and
//! the conflict is surfaced through the status. Announcements for names not
//! registered here are remembered for `join`. Disconnecting a peer closes
//! its connection through the hub: revocation must sever announcements even
//! when it races connection setup.
//!
//! The hub also carries the machine's latest membership, since it owns the
//! outboxes: the mesh store publishes it here on every change, and it is
//! replayed (before any announcement) to every connecting peer.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use color_eyre::eyre::{Result, bail, ensure};
use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::{
    config::{Membership, RepoId, validate_name},
    net::{
        sync::{self, Announce, FetchRequest, MAX_OP_FRAME_SIZE, OpFrame, UniMessage},
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
    /// Sequence it was drained at, so a failed fetch can [requeue] the
    /// heads without clobbering a newer announcement.
    ///
    /// [requeue]: Inbox::requeue
    pub seq: u64,
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
    repos: BTreeMap<String, RepoEntry>,
    peers: BTreeMap<EndpointId, PeerSender>,
    /// Latest announcement per peer for repo names not registered here.
    /// Peers replay all their repos on connect, so this is how `join`
    /// learns a repo's id and heads and who serves it. Bounded; stale
    /// entries are healed like any announcement.
    orphans: BTreeMap<String, BTreeMap<EndpointId, OrphanAnnounce>>,
    /// This machine's latest membership, replayed to connecting peers and
    /// broadcast on every change (see [`SyncHub::publish_membership`]).
    membership: Membership,
}

/// Cap on unregistered repo names tracked *per peer*, so one peer (hostile
/// or simply repo-rich) cannot evict the names other peers announce. A
/// peer's entries are pruned when it disconnects.
const MAX_ORPHAN_REPOS_PER_PEER: usize = 64;

/// The latest announcement one peer made for an unregistered repo name.
#[derive(Debug, Clone)]
struct OrphanAnnounce {
    id: RepoId,
    heads: Vec<Vec<u8>>,
}

/// A peer that can serve a `join` of an unregistered repo, with the op
/// heads it claims.
pub type JoinSource = (EndpointId, Vec<Vec<u8>>);

/// Hub-side state of one registered repo.
#[derive(Debug)]
struct RepoEntry {
    /// The local id of the repo, checked against announcements and fetch
    /// requests: a name match with a different id is a conflict, never a
    /// sync.
    id: RepoId,
    /// Sequence of the latest publish, stamped into announcements so
    /// receivers can discard reordered ones. Monotonic per daemon run.
    seq: u64,
    /// Latest published op heads, replayed to connecting peers. `None`
    /// until the repo task first publishes (repo not opened yet).
    published: Option<Vec<Vec<u8>>>,
    inbox: Arc<Inbox>,
    /// Peers whose last announcement for this name carried a different id.
    /// An entry clears when its peer announces the matching id again or
    /// disconnects.
    conflicts: BTreeMap<EndpointId, RepoId>,
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
    /// announcements from. Replaces any previous registration for the name.
    /// Orphan announcements held for the name become conflicts when their
    /// id disagrees; the peers that sent them would otherwise never re-warn
    /// an idle repo.
    pub fn register_repo(&self, name: String, id: RepoId) -> Arc<Inbox> {
        let inbox = Arc::new(Inbox::default());
        let mut state = self.state.lock().unwrap();

        let mut conflicts = BTreeMap::new();
        for (peer, announce) in state.orphans.remove(&name).unwrap_or_default() {
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
    pub fn repo_opened(&self, name: &str, id: &RepoId, repo: Arc<MeshRepo>) {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap()
            .repos
            .get_mut(name)
            .filter(|entry| &entry.id == id)
        {
            entry.serving = Some(Serving {
                repo,
                permits: Arc::new(tokio::sync::Semaphore::new(MAX_SERVES)),
            });
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

    /// Serves an inbound fetch on a detached task, or refuses it when the
    /// repo is not open here or too busy.
    pub fn serve_fetch(
        &self,
        peer: EndpointId,
        request: FetchRequest,
        mut send: SendStream,
        mut recv: RecvStream,
    ) {
        let message = match self.lookup_serving(&request) {
            None => "repo not available",
            Some(serving) => match serving.permits.clone().try_acquire_owned() {
                Err(_) => {
                    debug!(repo = %request.name, "refusing fetch: too many being served");
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

    /// Resolves the serve handle for a fetch request, logging every kind of
    /// refusal distinctly. The name is validated before any use: it ends up
    /// in logs and as a map key, and must never carry unbounded length or
    /// confusables.
    fn lookup_serving(&self, request: &FetchRequest) -> Option<Serving> {
        if validate_name("repo", &request.name).is_err() {
            debug!("refusing fetch: invalid repo name");
            return None;
        }

        let state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get(&request.name) else {
            debug!(repo = %request.name, "refusing fetch: repo not registered here");
            return None;
        };
        // An id mismatch means the fetcher's repo is not ours, whatever the
        // name says: refuse rather than mix unrelated histories.
        if entry.id != request.id {
            debug!(repo = %request.name, "refusing fetch: repo id mismatch");
            return None;
        }

        let serving = entry.serving.clone();
        if serving.is_none() {
            debug!(repo = %request.name, "refusing fetch: repo not open");
        }
        serving
    }

    /// Removes a repo registration.
    pub fn unregister_repo(&self, name: &str) {
        self.state.lock().unwrap().repos.remove(name);
    }

    /// Publishes a repo's current op heads: cached for peers that connect
    /// later and queued to every connected peer's outbox, coalescing with
    /// any announcement still pending there. The id guards against a stale
    /// task of a replaced same-name repo announcing the wrong heads.
    pub fn publish(&self, name: &str, id: &RepoId, heads: Vec<Vec<u8>>) {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get_mut(name).filter(|entry| &entry.id == id) else {
            return;
        };
        entry.seq += 1;
        entry.published = Some(heads.clone());

        let announce = Announce {
            name: name.to_owned(),
            id: entry.id.clone(),
            seq: entry.seq,
            heads,
        };
        for sender in state.peers.values() {
            sender.outbox.push_announce(announce.clone());
        }
    }

    /// Publishes this machine's membership: cached for peers that connect
    /// later and queued to every connected peer, coalescing with any
    /// membership still pending there. Called on every membership change,
    /// including changes learned from gossip, which is what makes the
    /// propagation transitive.
    pub fn publish_membership(&self, membership: Membership) {
        let mut state = self.state.lock().unwrap();
        state.membership = membership;
        for sender in state.peers.values() {
            sender.outbox.push_membership(state.membership.clone());
        }
    }

    /// Marks a peer connected: spawns its sender task and seeds the outbox
    /// with the membership and every published repo, so a (re)connecting
    /// peer learns state it missed while away.
    pub fn peer_connected(&self, peer: EndpointId, conn: &Connection) {
        let outbox = Arc::new(Outbox::default());
        let task = tokio::spawn(run_sender(conn.clone(), outbox.clone()));

        let mut state = self.state.lock().unwrap();
        outbox.push_membership(state.membership.clone());
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
            state.orphans.retain(|_, peers| {
                peers.remove(peer);
                !peers.is_empty()
            });
            state.peers.remove(peer)
        };

        if let Some(sender) = removed {
            sender.task.abort();
            sender.conn.close(0u32.into(), b"peer removed");
        }
    }

    /// Routes an inbound announcement to its repo's inbox; announcements
    /// for unregistered names are remembered for `join`, and a registered
    /// name announced with a different id is recorded as a conflict.
    pub fn route(&self, peer: EndpointId, announce: Announce) {
        // Repo names arriving from peers are validated before any use:
        // they end up in logs and as map keys, and must never carry
        // unbounded length or confusables.
        if validate_name("repo", &announce.name).is_err() {
            debug!(peer = %peer, "dropping announcement with an invalid repo name");
            return;
        }

        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get_mut(&announce.name) else {
            let known = state
                .orphans
                .get(&announce.name)
                .is_some_and(|peers| peers.contains_key(&peer));
            let held = state
                .orphans
                .values()
                .filter(|peers| peers.contains_key(&peer))
                .count();
            if !known && held >= MAX_ORPHAN_REPOS_PER_PEER {
                debug!(peer = %peer, "dropping announcement: too many unregistered repos");
                return;
            }
            state.orphans.entry(announce.name).or_default().insert(
                peer,
                OrphanAnnounce {
                    id: announce.id,
                    heads: announce.heads,
                },
            );
            return;
        };

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

    /// Resolves a `join` of the repo named `name`: the id every connected
    /// announcing peer agrees on, and each peer's claimed heads. Errors
    /// when nobody announces the name, or when peers disagree on the id
    /// (unrelated repos contesting one name, which only the user can
    /// resolve).
    pub fn join_sources(&self, name: &str) -> Result<(RepoId, Vec<JoinSource>)> {
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
             resolve the conflict before joining",
        );

        Ok((
            id,
            sources
                .into_iter()
                .map(|(peer, announce)| (peer, announce.heads))
                .collect(),
        ))
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
                slot.heads.take().map(|heads| PeerAnnounce {
                    peer: *peer,
                    seq: slot.seq,
                    heads,
                })
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
            && slot.heads.is_none()
        {
            slot.heads = Some(heads);
        }
    }
}

/// Pending messages for one peer, coalesced latest-wins: one slot for the
/// membership and one per repo for announcements, each kept until the
/// sender task takes it.
#[derive(Debug, Default)]
struct Outbox {
    pending: Mutex<OutboxState>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct OutboxState {
    membership: Option<Membership>,
    announces: BTreeMap<String, Announce>,
}

impl Outbox {
    fn push_announce(&self, announce: Announce) {
        self.pending
            .lock()
            .unwrap()
            .announces
            .insert(announce.name.clone(), announce);
        self.notify.notify_one();
    }

    fn push_membership(&self, membership: Membership) {
        self.pending.lock().unwrap().membership = Some(membership);
        self.notify.notify_one();
    }

    /// Takes the next message, membership first: a new peer should learn
    /// the mesh before the repo heads.
    fn pop(&self) -> Option<UniMessage> {
        let mut pending = self.pending.lock().unwrap();
        if let Some(membership) = pending.membership.take() {
            return Some(UniMessage::Membership(membership));
        }
        pending
            .announces
            .pop_first()
            .map(|(_, announce)| UniMessage::Announce(announce))
    }
}

/// Sends a peer's outbox until its connection fails; messages lost with
/// the connection are recovered by the reconnect replay.
async fn run_sender(conn: Connection, outbox: Arc<Outbox>) {
    loop {
        let Some(message) = outbox.pop() else {
            outbox.notify.notified().await;
            continue;
        };
        match tokio::time::timeout(ANNOUNCE_TIMEOUT, sync::send_uni(&conn, &message)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return debug!("send failed: {err:#}"),
            Err(_) => return debug!("send timed out"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn announce(name: &str, id: &RepoId, seq: u64, heads: Vec<Vec<u8>>) -> Announce {
        Announce {
            name: name.to_owned(),
            id: id.clone(),
            seq,
            heads,
        }
    }

    #[tokio::test]
    async fn routes_to_registered_repo() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo("a".to_owned(), id.clone());

        let peer = iroh::SecretKey::generate().public();
        hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));

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
        let inbox = hub.register_repo("a".to_owned(), id.clone());
        let peer = iroh::SecretKey::generate().public();

        hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
        hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));

        let drained = inbox.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].heads, vec![vec![2; 64]]);

        // The watermark survives draining: the stale announcement stays
        // rejected even when it arrives afterwards.
        hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
        assert!(inbox.drain().is_empty());
    }

    #[tokio::test]
    async fn requeue_retries_failed_heads_until_superseded() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo("a".to_owned(), id.clone());
        let peer = iroh::SecretKey::generate().public();

        hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
        let drained = inbox.drain();
        assert_eq!(drained.len(), 1);

        // A failed fetch requeues the drained heads; the next drain retries.
        inbox.requeue(peer, drained[0].seq, drained[0].heads.clone());
        let retried = inbox.drain();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].heads, vec![vec![1; 64]]);

        // A newer announcement supersedes a stale requeue.
        hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
        inbox.requeue(peer, 1, vec![vec![1; 64]]);
        let latest = inbox.drain();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].heads, vec![vec![2; 64]]);
    }

    #[tokio::test]
    async fn ignores_unregistered_repo() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo("a".to_owned(), id.clone());
        hub.unregister_repo("a");

        let peer = iroh::SecretKey::generate().public();
        hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
        assert!(inbox.drain().is_empty());
    }

    #[tokio::test]
    async fn remembers_unregistered_announcements_for_join() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let peer = iroh::SecretKey::generate().public();

        // Not offered as a join source while the peer is not connected.
        hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
        assert!(hub.join_sources("a").is_err());

        // Registering the repo claims the orphan entry.
        let inbox = hub.register_repo("a".to_owned(), id.clone());
        assert!(hub.join_sources("a").is_err());
        hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
        assert_eq!(inbox.drain().len(), 1);
    }

    #[tokio::test]
    async fn conflicting_id_is_surfaced_and_never_synced() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo("a".to_owned(), id.clone());
        let peer = iroh::SecretKey::generate().public();

        let foreign = RepoId::generate();
        hub.route(peer, announce("a", &foreign, 1, vec![vec![1; 64]]));

        assert!(inbox.drain().is_empty(), "conflicts must not be synced");
        assert_eq!(hub.conflicts(), vec![("a".to_owned(), peer)]);

        // A matching announcement resumes sync and resolves the conflict.
        hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
        assert_eq!(inbox.drain().len(), 1);
        assert!(hub.conflicts().is_empty());
    }

    #[tokio::test]
    async fn tracks_conflicts_per_peer() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        hub.register_repo("a".to_owned(), id.clone());
        let (b, c) = (
            iroh::SecretKey::generate().public(),
            iroh::SecretKey::generate().public(),
        );

        hub.route(b, announce("a", &RepoId::generate(), 1, vec![vec![1; 64]]));
        hub.route(c, announce("a", &RepoId::generate(), 1, vec![vec![2; 64]]));
        assert_eq!(hub.conflicts().len(), 2);

        // One peer leaving must not hide the other's live conflict.
        hub.peer_disconnected(&c);
        assert_eq!(hub.conflicts(), vec![("a".to_owned(), b)]);
    }

    #[tokio::test]
    async fn register_seeds_conflicts_from_orphans() {
        let hub = SyncHub::new();
        let peer = iroh::SecretKey::generate().public();

        // The peer announced first; adding a different local repo under
        // the same name must surface the conflict without waiting for the
        // (idle) peer to announce again.
        hub.route(
            peer,
            announce("a", &RepoId::generate(), 1, vec![vec![1; 64]]),
        );
        hub.register_repo("a".to_owned(), RepoId::generate());
        assert_eq!(hub.conflicts(), vec![("a".to_owned(), peer)]);
    }

    #[tokio::test]
    async fn orphans_are_bounded_per_peer() {
        let hub = SyncHub::new();
        let flooder = iroh::SecretKey::generate().public();
        let honest = iroh::SecretKey::generate().public();

        for n in 0..MAX_ORPHAN_REPOS_PER_PEER {
            hub.route(
                flooder,
                announce(
                    &format!("junk{n}"),
                    &RepoId::generate(),
                    1,
                    vec![vec![1; 64]],
                ),
            );
        }
        hub.route(
            honest,
            announce("work", &RepoId::generate(), 1, vec![vec![2; 64]]),
        );

        // The honest orphan survived the flood: registering `work` with a
        // different id seeds its conflict, proving the entry was kept.
        hub.register_repo("work".to_owned(), RepoId::generate());
        assert_eq!(hub.conflicts(), vec![("work".to_owned(), honest)]);

        // Disconnecting the flooder frees its slots for new names.
        hub.peer_disconnected(&flooder);
        hub.route(
            flooder,
            announce("fresh", &RepoId::generate(), 1, vec![vec![3; 64]]),
        );
        hub.register_repo("fresh".to_owned(), RepoId::generate());
        assert!(hub.conflicts().iter().any(|(name, _)| name == "fresh"));
    }

    #[tokio::test]
    async fn rejects_invalid_announced_names() {
        let hub = SyncHub::new();
        let id = RepoId::generate();
        let inbox = hub.register_repo("a\u{202E}b".to_owned(), id.clone());
        let peer = iroh::SecretKey::generate().public();

        hub.route(peer, announce("a\u{202E}b", &id, 1, vec![vec![1; 64]]));
        assert!(inbox.drain().is_empty());
        hub.route(peer, announce("", &id, 1, vec![vec![1; 64]]));
        assert!(hub.join_sources("").is_err());
    }

    #[test]
    fn outbox_coalesces_per_repo_with_membership_first() {
        let outbox = Outbox::default();
        let id = RepoId::generate();
        let other = RepoId::generate();

        outbox.push_announce(announce("a", &id, 1, vec![vec![1; 64]]));
        outbox.push_membership(Membership::default());
        outbox.push_announce(announce("b", &other, 1, vec![vec![3; 64]]));
        outbox.push_announce(announce("a", &id, 2, vec![vec![2; 64]]));
        outbox.push_membership(Membership::default());

        let sent: Vec<UniMessage> = std::iter::from_fn(|| outbox.pop()).collect();
        assert_eq!(sent.len(), 3);
        assert!(matches!(sent[0], UniMessage::Membership(_)));
        let seqs: Vec<(&str, u64)> = sent
            .iter()
            .filter_map(|message| match message {
                UniMessage::Announce(a) => Some((a.name.as_str(), a.seq)),
                UniMessage::Membership(_) => None,
            })
            .collect();
        assert!(seqs.contains(&("a", 2)) && seqs.contains(&("b", 1)));
    }
}

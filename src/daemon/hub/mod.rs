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
//! registered here are remembered for `clone`. Disconnecting a peer closes
//! its connection through the hub: revocation must sever announcements even
//! when it races connection setup.
//!
//! The hub also carries the machine's latest membership and status report,
//! since it owns the outboxes: both are published here on every change and
//! replayed (before any announcement) to every connecting peer. It holds
//! the latest report of each connected peer as well.

mod inbox;
mod outbox;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use color_eyre::eyre::{Result, bail, ensure};
use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
};
use tracing::{debug, warn};

pub use self::inbox::{Inbox, PeerAnnounce};
use self::outbox::{Outbox, run_sender};
use crate::{
    config::{Membership, RepoId, sanitize, validate_name},
    net::{
        sync::{Announce, FetchRequest, MAX_OP_FRAME_SIZE, OpFrame, StatusReport},
        wire,
    },
    repo::{OpenRepo, transfer},
};

/// Fetches served concurrently per repo (read-only on the repo).
const MAX_SERVES: usize = 2;

/// Hard budget on serving one fetch; QUIC flow control means a stalled
/// fetcher could otherwise pin a serve task and its permit forever.
const SERVE_TIMEOUT: Duration = Duration::from_mins(30);

/// Cap on unregistered repo names tracked *per peer*, so one peer (hostile
/// or simply repo-rich) cannot evict the names other peers announce. A
/// peer's entries are pruned when it disconnects.
const MAX_ORPHAN_REPOS_PER_PEER: usize = 64;

/// A peer that can serve a `clone` of an unregistered repo, with the op
/// heads it claims.
#[derive(Debug)]
pub struct CloneSource {
    pub peer: EndpointId,
    pub heads: Vec<Vec<u8>>,
}

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
    /// Latest announcement per peer for repo names not registered here.
    /// Peers replay all their repos on connect, so this is how `clone`
    /// learns a repo's id and heads and who serves it. Bounded; stale
    /// entries are healed like any announcement.
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
    /// How many orphan names hold an entry for `peer`, against
    /// [`MAX_ORPHAN_REPOS_PER_PEER`].
    fn orphans_held_by(&self, peer: &EndpointId) -> usize {
        self.orphans
            .values()
            .filter(|peers| peers.contains_key(peer))
            .count()
    }

    /// Queues a message to every connected peer's outbox.
    fn broadcast(&self, push: impl Fn(&Outbox)) {
        for sender in self.peers.values() {
            push(&sender.outbox);
        }
    }
}

/// The latest announcement one peer made for an unregistered repo name.
#[derive(Debug, Clone)]
struct OrphanAnnounce {
    id: RepoId,
    heads: Vec<Vec<u8>>,
    colocated: bool,
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
    /// Whether the local instance of the repo is colocated, learned when
    /// the repo task opens it.
    local_colocated: bool,
    /// Peers whose last announcement claimed a colocated instance. An
    /// entry clears when its peer announces non-colocated or disconnects.
    colocated_peers: BTreeSet<EndpointId>,
    /// Serve handle, present while the repo task has the repo open.
    /// Serving is read-only and dispatched straight from the hub: it must
    /// never depend on the repo task's loop, which may itself be blocked
    /// fetching from the very peer whose fetch we are serving.
    serving: Option<Serving>,
}

impl RepoEntry {
    /// Whether sync is suspended for this repo: the local instance and at
    /// least one peer's are both colocated (see the sync docs for why two
    /// colocated instances must not exchange ops). While paused the repo
    /// fetches from nobody; announcing and serving stay on. Detection
    /// needs the colocated instances directly connected; in a relay-only
    /// topology the conflict goes undetected.
    fn paused(&self) -> bool {
        self.local_colocated && !self.colocated_peers.is_empty()
    }
}

/// What the hub needs to serve fetches for an open repo.
#[derive(Debug, Clone)]
struct Serving {
    repo: Arc<OpenRepo>,
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
        let mut colocated_peers = BTreeSet::new();
        for (peer, announce) in state.orphans.remove(&name).unwrap_or_default() {
            if announce.id != id {
                warn!(
                    repo = %name, peer = %peer,
                    "peer announces a different repo under this name; not syncing with it",
                );
                conflicts.insert(peer, announce.id);
            } else if announce.colocated {
                colocated_peers.insert(peer);
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
                local_colocated: false,
                colocated_peers,
                serving: None,
            },
        );
        inbox
    }

    /// Makes an opened repo servable and records its colocation state.
    /// Called by the repo task once its stores are open; replaces the
    /// handle from a previous open. The id guards against a stale task of
    /// a replaced same-name repo installing the wrong stores (aborts only
    /// land at the task's next await point).
    pub fn repo_opened(&self, name: &str, id: &RepoId, repo: Arc<OpenRepo>) {
        if let Some(entry) = self
            .state
            .lock()
            .unwrap()
            .repos
            .get_mut(name)
            .filter(|entry| &entry.id == id)
        {
            entry.local_colocated = repo.is_colocated();
            entry.serving = Some(Serving {
                repo,
                permits: Arc::new(tokio::sync::Semaphore::new(MAX_SERVES)),
            });
            if entry.paused() {
                warn!(
                    repo = %name,
                    "this instance and a peer's are both colocated; sync is paused",
                );
            }
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
        let Some(serving) = self.lookup_serving(&peer, &request) else {
            return refuse_fetch(send, "repo not available");
        };
        let Ok(permit) = serving.permits.clone().try_acquire_owned() else {
            debug!(repo = %request.name, "refusing fetch: too many being served");
            return refuse_fetch(send, "busy, retry later");
        };

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
    }

    /// Resolves the serve handle for a fetch request, logging every kind of
    /// refusal distinctly. The name needs no validation here: only names that
    /// passed it at registration are ever in the map, so an invalid one
    /// simply fails to match.
    fn lookup_serving(&self, peer: &EndpointId, request: &FetchRequest) -> Option<Serving> {
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
        // Refused only for the conflicting peer, not repo-wide: it should
        // not be fetching while paused itself, but an older or hostile
        // build must still be denied the ops that feed the colocation
        // ping-pong. Everyone else is served normally.
        if entry.local_colocated && entry.colocated_peers.contains(peer) {
            debug!(repo = %request.name, "refusing fetch: colocation conflict with this peer");
            return None;
        }

        let serving = entry.serving.clone();
        if serving.is_none() {
            debug!(repo = %request.name, "refusing fetch: repo not open");
        }
        serving
    }

    /// Removes a repo registration (a local forget, a mesh-wide removal,
    /// or a same-name replacement).
    ///
    /// The peers' last announcements move back to the orphan store, so the
    /// repo stays immediately clonable here: peers only re-announce on a
    /// head change or a reconnect, which could otherwise be arbitrarily
    /// far away. A retraction (an announcement with no heads) also goes
    /// out, so peers holding a colocation pause or a name conflict against
    /// this instance release it instead of staying stuck until this
    /// machine disconnects.
    pub fn unregister_repo(&self, name: &str) {
        let mut state = self.state.lock().unwrap();
        let Some(entry) = state.repos.remove(name) else {
            return;
        };

        for (peer, heads) in entry.inbox.snapshot() {
            if state.orphans_held_by(&peer) >= MAX_ORPHAN_REPOS_PER_PEER {
                continue;
            }
            state.orphans.entry(name.to_owned()).or_default().insert(
                peer,
                OrphanAnnounce {
                    id: entry.id.clone(),
                    heads,
                    colocated: entry.colocated_peers.contains(&peer),
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
                colocated: false,
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
            colocated: entry.local_colocated,
        };
        state.broadcast(|outbox| outbox.push_announce(announce.clone()));
    }

    /// Publishes this machine's status report: cached for peers that
    /// connect later and queued to every connected peer, coalescing with
    /// any report still pending there.
    pub fn publish_status(&self, report: StatusReport) {
        let mut state = self.state.lock().unwrap();
        state.status = Some(report.clone());
        state.broadcast(move |outbox| outbox.push_status(report.clone()));
    }

    /// Publishes this machine's membership: cached for peers that connect
    /// later and queued to every connected peer, coalescing with any
    /// membership still pending there. Called on every membership change,
    /// including changes learned from gossip, which is what makes the
    /// propagation transitive.
    pub fn publish_membership(&self, membership: Membership) {
        let mut state = self.state.lock().unwrap();
        state.membership = membership;
        state.broadcast(|outbox| outbox.push_membership(state.membership.clone()));
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
                    colocated: entry.local_colocated,
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
                entry.colocated_peers.remove(peer);
            }
            state.orphans.retain(|_, peers| {
                peers.remove(peer);
                !peers.is_empty()
            });
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
                if let Some(peers) = state.orphans.get_mut(&announce.name) {
                    peers.remove(&peer);
                    if peers.is_empty() {
                        state.orphans.remove(&announce.name);
                    }
                }
                return;
            }
            let known = state
                .orphans
                .get(&announce.name)
                .is_some_and(|peers| peers.contains_key(&peer));
            if !known && state.orphans_held_by(&peer) >= MAX_ORPHAN_REPOS_PER_PEER {
                debug!(peer = %peer, "dropping announcement: too many unregistered repos");
                return;
            }
            state.orphans.entry(announce.name).or_default().insert(
                peer,
                OrphanAnnounce {
                    id: announce.id,
                    heads: announce.heads,
                    colocated: announce.colocated,
                },
            );
            return;
        };

        if retraction {
            entry.conflicts.remove(&peer);
            entry.colocated_peers.remove(&peer);
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

        let was_paused = entry.paused();
        if announce.colocated {
            entry.colocated_peers.insert(peer);
        } else {
            entry.colocated_peers.remove(&peer);
        }
        if entry.paused() && !was_paused {
            warn!(
                repo = %announce.name, peer = %peer,
                "this instance and the peer's are both colocated; pausing sync \
                 (de-colocate one side to resume)",
            );
        }
        // Offered even while paused: the repo task requeues instead of
        // fetching, so the heads are fetched once the pause lifts rather
        // than lost until the peer's next change.
        entry.inbox.offer(peer, announce.seq, announce.heads);
    }

    /// Resolves a `clone` of the repo named `name`: the id every connected
    /// announcing peer agrees on, and each peer's claimed heads. Errors
    /// when nobody announces the name, or when peers disagree on the id
    /// (unrelated repos contesting one name, which only the user can
    /// resolve).
    pub fn clone_sources(&self, name: &str) -> Result<(RepoId, Vec<CloneSource>)> {
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
             resolve the conflict before cloning",
        );

        Ok((
            id,
            sources
                .into_iter()
                .map(|(peer, announce)| CloneSource {
                    peer,
                    heads: announce.heads,
                })
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

    /// The repos whose sync is paused by a colocation conflict, keyed by
    /// name, with the peers whose colocated instances cause it.
    pub fn paused_repos(&self) -> BTreeMap<String, Vec<EndpointId>> {
        let state = self.state.lock().unwrap();
        state
            .repos
            .iter()
            .filter(|(_, entry)| entry.paused())
            .map(|(name, entry)| {
                (
                    name.clone(),
                    entry.colocated_peers.iter().copied().collect(),
                )
            })
            .collect()
    }

    /// Whether a repo's sync is paused (see [`RepoEntry::paused`]). Repo
    /// tasks check this on every announcement they drain and requeue
    /// instead of fetching, which is what enforces the fetch side of the
    /// pause.
    pub fn is_paused(&self, name: &str) -> bool {
        let state = self.state.lock().unwrap();
        state.repos.get(name).is_some_and(RepoEntry::paused)
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

/// Refuses a fetch on a detached task, telling the peer why before closing.
fn refuse_fetch(mut send: SendStream, message: &'static str) {
    tokio::spawn(async move {
        let frame = OpFrame::Error {
            message: message.to_owned(),
        };
        let _ = wire::write_message(&mut send, &frame, MAX_OP_FRAME_SIZE).await;
        let _ = send.finish();
    });
}

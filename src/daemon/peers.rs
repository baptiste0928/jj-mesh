//! Peer connection management.
//!
//! One task per configured peer maintains a persistent connection: dial,
//! hold, reconnect with jittered exponential backoff. Both sides dial each
//! other; duplicate connections are resolved deterministically by keeping the
//! one whose *dialer* has the lower endpoint id, so the pair converges on a
//! single connection without flapping.

use std::{
    collections::BTreeMap,
    mem,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Result, anyhow};
use futures::{Stream, StreamExt as _, stream};
use iroh::{
    Endpoint, EndpointId, TransportAddr,
    endpoint::{Connection, ConnectionError, RecvStream, SendStream},
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::{backoff::Backoff, control, hub::SyncHub};
use crate::{
    config::{Membership, MeshState, Peer},
    net::{
        fetch,
        hello::{self, Incompatible},
        sync, wire,
    },
};

/// Budget for reading one uni stream or fetch request, so a stalled
/// stream cannot hold its slot, and the messages read after it,
/// indefinitely.
const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Budget for one dial. iroh only gives up on its own after its handshake
/// idle timeout, and a peer with a stale discovery record hangs until then.
const DIAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Reconnect delay after a first failure; doubles up to [`BACKOFF_MAX`].
const BACKOFF_MIN: Duration = Duration::from_secs(1);

/// Ceiling of the reconnect delay.
const BACKOFF_MAX: Duration = Duration::from_mins(1);

/// A connection living at least this long resets the backoff. Connections
/// dying younger (e.g. closed as duplicate by the peer, or a flapping peer)
/// advance it instead, so establish-then-close cycles cannot redial hot.
const STABLE_UPTIME: Duration = Duration::from_secs(10);

/// The set of managed peers, synced from the mesh state.
///
/// Also acts as the connection allowlist: inbound connections are routed to
/// the matching peer task and refused when the endpoint is not a peer.
#[derive(Debug)]
pub struct PeerSet {
    endpoint: Endpoint,
    local_id: EndpointId,
    hub: Arc<SyncHub>,
    /// Inbound memberships with their sender's name, drained by the
    /// daemon's membership loop.
    gossip: mpsc::Sender<(String, Membership)>,
    peers: Mutex<BTreeMap<EndpointId, PeerHandle>>,
}

/// Book-keeping for one peer task. Its name lives in the hub.
#[derive(Debug)]
struct PeerHandle {
    state: Arc<Mutex<PeerState>>,
    inbound: mpsc::Sender<Connection>,
    task: tokio::task::JoinHandle<()>,
}

/// State of one peer, shared between its task and status snapshots.
#[derive(Debug)]
struct PeerState {
    link: Link,
    /// When the last connection ended, if one did.
    last_seen: Option<SystemTime>,
}

/// The connection to one peer.
#[derive(Debug)]
enum Link {
    /// Dialing, after `failures` consecutive failed attempts.
    Connecting {
        failures: u32,
    },
    Connected {
        conn: Connection,
        since: SystemTime,
    },
    Backoff {
        failure: Failure,
    },
}

/// Why a connection attempt failed.
#[derive(Debug)]
enum Failure {
    Error(String),
    Incompatible(Incompatible),
}

impl PeerSet {
    pub fn new(
        endpoint: Endpoint,
        local_id: EndpointId,
        hub: Arc<SyncHub>,
        gossip: mpsc::Sender<(String, Membership)>,
    ) -> Self {
        PeerSet {
            endpoint,
            local_id,
            hub,
            gossip,
            peers: Mutex::new(BTreeMap::new()),
        }
    }

    /// Aligns the managed peers with the mesh state: spawns tasks for new
    /// (alive) peers, shuts down removed ones, and publishes the names to
    /// the hub.
    pub fn sync(&self, state: &MeshState) {
        let desired: BTreeMap<EndpointId, String> = state
            .alive_peers()
            .map(|(endpoint, name)| (*endpoint, name.to_owned()))
            .collect();

        let mut peers = self.peers.lock().unwrap();

        let removed: Vec<EndpointId> = peers
            .keys()
            .filter(|id| !desired.contains_key(*id))
            .copied()
            .collect();
        for id in removed {
            let handle = peers.remove(&id).expect("id was collected from the map");
            info!(peer = %self.hub.peer_name(&id), "removing peer");
            tokio::spawn(handle.stop(id, self.hub.clone()));
        }

        for id in peers.keys() {
            let (old, new) = (self.hub.peer_name(id), &desired[id]);
            if &old != new {
                info!(peer = %new, previous = %old, "renaming peer");
            }
        }
        // Before spawning, so new tasks log under their name. Running tasks
        // follow renames without dropping their connection.
        self.hub.set_peer_names(desired.clone());

        for (id, name) in desired {
            peers.entry(id).or_insert_with(|| {
                debug!(peer = %name, "managing peer");
                self.spawn_peer(id)
            });
        }
    }

    /// Stops every peer task and drops their hub registrations, closing
    /// their connections.
    pub async fn shutdown(&self) {
        let peers = mem::take(&mut *self.peers.lock().unwrap());
        for (id, handle) in peers {
            handle.stop(id, self.hub.clone()).await;
        }
    }

    /// Whether `id` is a managed peer, allowed to connect.
    pub fn allows(&self, id: &EndpointId) -> bool {
        self.peers.lock().unwrap().contains_key(id)
    }

    /// Hands an accepted connection, past its hello, to the matching peer
    /// task, refusing endpoints that are not paired (or no longer: the
    /// accept loop checked before the hello).
    pub fn route_inbound(&self, conn: Connection) {
        let id = conn.remote_id();
        let peers = self.peers.lock().unwrap();
        let Some(handle) = peers.get(&id) else {
            // Kept at debug: reachable by any endpoint that learns our id,
            // so logging louder would allow log flooding.
            debug!("refusing connection from unpaired endpoint {id}");
            conn.close(0u32.into(), b"unauthorized");
            return;
        };

        if let Err(err) = handle.inbound.try_send(conn) {
            debug!(peer = %self.hub.peer_name(&id), "dropping surplus inbound connection");
            err.into_inner().close(0u32.into(), b"busy");
        }
    }

    /// Snapshots the state of every peer for the control socket, with
    /// their roles from `state`.
    pub fn statuses(&self, state: &MeshState) -> Vec<control::PeerStatus> {
        let peers = self.peers.lock().unwrap();

        peers
            .iter()
            .map(|(id, handle)| {
                let peer_state = handle.state.lock().unwrap();
                let connection = match &peer_state.link {
                    Link::Connecting { failures } => control::ConnectionStatus::Connecting {
                        failures: *failures,
                    },
                    Link::Backoff { failure } => match failure {
                        Failure::Error(error) => control::ConnectionStatus::Backoff {
                            error: error.clone(),
                        },
                        Failure::Incompatible(peer) => {
                            control::ConnectionStatus::Incompatible { peer: peer.clone() }
                        }
                    },
                    Link::Connected { conn, since } => control::ConnectionStatus::Connected {
                        path: selected_path(conn),
                        since_secs: since.elapsed().unwrap_or_default().as_secs(),
                    },
                };
                let last_seen_secs = peer_state
                    .last_seen
                    .map(|at| at.elapsed().unwrap_or_default().as_secs());
                drop(peer_state);

                control::PeerStatus {
                    name: self.hub.peer_name(id),
                    endpoint: *id,
                    server: state.peers.get(id).is_some_and(Peer::is_server),
                    last_seen_secs,
                    connection,
                }
            })
            .collect()
    }

    fn spawn_peer(&self, peer_id: EndpointId) -> PeerHandle {
        let state = Arc::new(Mutex::new(PeerState {
            link: Link::Connecting { failures: 0 },
            last_seen: None,
        }));
        let (tx, rx) = mpsc::channel(4);

        let task = tokio::spawn(run_peer(PeerTask {
            endpoint: self.endpoint.clone(),
            local_id: self.local_id,
            peer_id,
            state: state.clone(),
            hub: self.hub.clone(),
            gossip: self.gossip.clone(),
            inbound: rx,
        }));

        PeerHandle {
            state,
            inbound: tx,
            task,
        }
    }
}

impl PeerHandle {
    /// Stops the task of peer `id`, then drops its hub registration.
    async fn stop(self, id: EndpointId, hub: Arc<SyncHub>) {
        self.task.abort();
        if let Link::Connected { conn, .. } = &self.state.lock().unwrap().link {
            conn.close(0u32.into(), b"peer removed");
        }
        let _ = self.task.await;
        hub.peer_disconnected(&id);
    }
}

/// Describes the selected network path of a connection, if any.
fn selected_path(conn: &Connection) -> Option<control::PathInfo> {
    let paths = conn.paths();
    let path = paths.iter().find(iroh::endpoint::Path::is_selected)?;

    let route = match path.remote_addr() {
        TransportAddr::Ip(addr) => control::Route::Direct {
            addr: addr.to_string(),
        },
        TransportAddr::Relay(url) => control::Route::Relay {
            url: url.to_string(),
        },
        other => control::Route::Direct {
            addr: format!("{other:?}"),
        },
    };

    Some(control::PathInfo {
        route,
        rtt_ms: u64::try_from(path.rtt().as_millis()).unwrap_or(u64::MAX),
    })
}

/// Everything a peer task owns.
struct PeerTask {
    endpoint: Endpoint,
    local_id: EndpointId,
    peer_id: EndpointId,
    state: Arc<Mutex<PeerState>>,
    hub: Arc<SyncHub>,
    gossip: mpsc::Sender<(String, Membership)>,
    inbound: mpsc::Receiver<Connection>,
}

/// Maintains the connection to one peer forever.
async fn run_peer(mut task: PeerTask) {
    let mut backoff = Backoff::new(BACKOFF_MIN, BACKOFF_MAX);
    // Consecutive attempts that failed to yield a stable connection.
    let mut failures = 0u32;
    // Why the last attempt failed, to be waited out before the next one.
    let mut failure: Option<Failure> = None;
    // The kind of failure logged in the current outage: a sleeping or
    // offline peer fails every retry.
    let mut logged = None;

    loop {
        // Wait out the backoff, adopting an inbound connection if one
        // arrives in the meantime.
        let mut adopted = None;
        if let Some(failure) = failure.take() {
            failures += 1;
            let delay = backoff.next_delay();
            task.set_link(Link::Backoff { failure });
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                Some(conn) = task.inbound.recv() => adopted = Some((conn, false)),
            }
        }

        let established = match adopted {
            Some(adopted) => Ok(adopted),
            None => task.establish(failures).await,
        };
        let (conn, outbound) = match established {
            Ok(established) => established,
            Err(error) => {
                let kind = mem::discriminant(&error);
                if logged != Some(kind) {
                    match &error {
                        Failure::Error(error) => {
                            info!(peer = %task.name(), %error, "cannot reach peer, retrying");
                        }
                        Failure::Incompatible(error) => {
                            warn!(peer = %task.name(), %error, "incompatible peer, retrying");
                        }
                    }
                    logged = Some(kind);
                }
                failure = Some(error);
                continue;
            }
        };

        logged = None;
        let held = Instant::now();
        info!(peer = %task.name(), outbound, "peer connected");
        let reason = task.connected(conn, outbound).await;
        let uptime = held.elapsed();
        info!(
            peer = %task.name(), %reason, uptime = %format_args!("{}s", uptime.as_secs()),
            "peer disconnected",
        );

        if uptime >= STABLE_UPTIME {
            backoff.reset();
            failures = 0;
        } else {
            failure = Some(Failure::Error(
                "connection dropped right after connecting".to_owned(),
            ));
        }
    }
}

impl PeerTask {
    /// Dials the peer, returning why it failed otherwise.
    ///
    /// The lower endpoint id prefers completing its own dial, leaving
    /// inbound connections queued for the duplicate tie-break; the higher id
    /// adopts whichever lands first. First-wins on both sides could adopt
    /// mirrored connections that the other side just abandoned, redialing
    /// in a loop.
    async fn establish(&mut self, failures: u32) -> Result<(Connection, bool), Failure> {
        self.set_link(Link::Connecting { failures });

        if self.local_id < self.peer_id {
            dial(&self.endpoint, self.peer_id)
                .await
                .map(|conn| (conn, true))
        } else {
            tokio::select! {
                dialed = dial(&self.endpoint, self.peer_id) => {
                    dialed.map(|conn| (conn, true))
                }
                Some(conn) = self.inbound.recv() => Ok((conn, false)),
            }
        }
    }

    /// Holds an established connection until it closes, resolving duplicate
    /// connections, routing inbound messages and fetches, and keeping the
    /// hub's registration current. Returns why the connection ended.
    async fn connected(&mut self, mut conn: Connection, mut outbound: bool) -> ConnectionError {
        let mut since = SystemTime::now();
        let mut messages = Box::pin(uni_messages(conn.clone()));

        self.hub.peer_connected(self.peer_id, &conn);
        let reason = loop {
            self.set_link(Link::Connected {
                conn: conn.clone(),
                since,
            });

            tokio::select! {
                reason = conn.closed() => break reason,
                Some(new) = self.inbound.recv() => {
                    // Keep the connection dialed by the lower endpoint id:
                    // both sides pick the same one, so the duplicate dies
                    // without killing the surviving connection.
                    if outbound && self.local_id < self.peer_id {
                        new.close(0u32.into(), b"duplicate");
                    } else {
                        conn.close(0u32.into(), b"duplicate");
                        info!(peer = %self.name(), "peer reconnected");
                        conn = new;
                        messages = Box::pin(uni_messages(conn.clone()));
                        outbound = false;
                        since = SystemTime::now();
                        self.hub.peer_connected(self.peer_id, &conn);
                    }
                }
                Some(message) = messages.next() => match message {
                    Ok(Ok(message)) => self.route_uni(message),
                    Ok(Err(err)) => {
                        // The sender counts the message as delivered: only a
                        // reconnect, which replays the latest state, heals.
                        debug!(peer = %self.name(), "lost message: {err:#}");
                        conn.close(0u32.into(), b"lost message");
                    }
                    Err(err) => break err,
                },
                stream = conn.accept_bi() => {
                    let (send, recv) = match stream {
                        Ok(stream) => stream,
                        Err(err) => break err,
                    };
                    self.accept_fetch(send, recv);
                }
            }
        };
        self.hub.peer_disconnected(&self.peer_id);
        self.state.lock().unwrap().last_seen = Some(SystemTime::now());
        reason
    }

    /// Reads a fetch request from a fresh bi stream and hands it to the hub
    /// (see [`SyncHub::serve_fetch`]). QUIC stream credit bounds how many
    /// are read at once.
    fn accept_fetch(&self, send: SendStream, mut recv: RecvStream) {
        let hub = self.hub.clone();
        let peer = self.peer_id;
        tokio::spawn(async move {
            let request = tokio::time::timeout(
                STREAM_READ_TIMEOUT,
                wire::read_message(&mut recv, wire::MAX_FRAME_SIZE),
            )
            .await;
            let request: fetch::FetchRequest = match request {
                Ok(Ok(request)) => request,
                Ok(Err(err)) => {
                    return debug!(peer = %hub.peer_name(&peer), "bad fetch request: {err:#}");
                }
                Err(_) => return debug!(peer = %hub.peer_name(&peer), "fetch request timed out"),
            };

            hub.serve_fetch(peer, request, send, recv);
        });
    }

    /// Routes one uni-stream message.
    fn route_uni(&self, message: sync::UniMessage) {
        match message {
            sync::UniMessage::Announce(announce) => self.hub.route(self.peer_id, announce),
            sync::UniMessage::Status(report) => self.hub.route_status(self.peer_id, report),
            sync::UniMessage::Membership(membership) => {
                // A full queue means a membership flood; dropping is
                // safe, the next change or reconnect re-sends.
                if let Err(err) = self.gossip.try_send((self.name(), membership)) {
                    let (name, _) = err.into_inner();
                    debug!(peer = %name, "dropping membership: gossip queue full");
                }
            }
        }
    }

    /// The peer's current name, following renames.
    fn name(&self) -> String {
        self.hub.peer_name(&self.peer_id)
    }

    fn set_link(&self, link: Link) {
        self.state.lock().unwrap().link = link;
    }
}

/// The messages of `conn`'s uni streams, read up to
/// [`sync::MAX_UNI_STREAMS`] at once and yielded in stream order. Streams
/// past those stay unaccepted, holding back the sender. Yields the
/// connection's error when it ends.
fn uni_messages(
    conn: Connection,
) -> impl Stream<Item = Result<Result<sync::UniMessage>, ConnectionError>> {
    stream::unfold(conn, |conn| async {
        let accepted = conn.accept_uni().await;
        Some((accepted, conn))
    })
    .map(|accepted| async move {
        let mut recv = accepted?;
        let read = tokio::time::timeout(STREAM_READ_TIMEOUT, sync::recv_uni(&mut recv));
        Ok(read.await.unwrap_or_else(|_| Err(anyhow!("timed out"))))
    })
    .buffered(sync::MAX_UNI_STREAMS as usize)
}

/// Dials the peer on the sync ALPN and runs the hello within
/// [`DIAL_TIMEOUT`], returning why it failed otherwise.
async fn dial(endpoint: &Endpoint, peer: EndpointId) -> Result<Connection, Failure> {
    let dialed = tokio::time::timeout(DIAL_TIMEOUT, async {
        let conn = endpoint.connect(peer, sync::ALPN).await?;
        Ok::<_, anyhow::Error>(hello::dial(&conn).await?.map(|()| conn))
    });
    match dialed.await {
        Ok(Ok(Ok(conn))) => Ok(conn),
        Ok(Ok(Err(incompatible))) => Err(Failure::Incompatible(incompatible)),
        Ok(Err(err)) => Err(Failure::Error(format!("{err:#}"))),
        Err(_) => Err(Failure::Error(format!(
            "no answer within {}s",
            DIAL_TIMEOUT.as_secs()
        ))),
    }
}

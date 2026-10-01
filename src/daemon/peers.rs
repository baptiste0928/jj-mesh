//! Peer connection management.
//!
//! One task per configured peer maintains a persistent connection: dial,
//! hold, reconnect with jittered exponential backoff. Both sides dial each
//! other; duplicate connections are resolved deterministically by keeping the
//! one whose *dialer* has the lower endpoint id, so the pair converges on a
//! single connection without flapping.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use iroh::{
    Endpoint, EndpointId, TransportAddr,
    endpoint::{Connection, ConnectionError, RecvStream, SendStream},
};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info};

use super::{backoff::Backoff, control, hub::SyncHub};
use crate::{
    config::{Membership, MeshState},
    net::{fetch, sync, wire},
};

/// Maximum uni streams (announcements, status, membership) handled
/// concurrently per peer connection.
const MAX_UNI_STREAMS: usize = 16;

/// Budget for reading one uni stream or fetch request, so a stalled
/// stream cannot hold its permit indefinitely.
const STREAM_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum fetch streams accepted concurrently per peer connection,
/// pending their routing to repo tasks.
const MAX_FETCH_STREAMS: usize = 4;

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

/// Live state of one peer connection, shared between its task and status
/// snapshots.
#[derive(Debug)]
enum PeerState {
    /// Dialing, after `failures` consecutive failed attempts.
    Connecting {
        failures: u32,
    },
    Connected {
        conn: Connection,
        since: SystemTime,
    },
    Backoff {
        until: Instant,
        error: String,
    },
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
            handle.shutdown();
            // The aborted task cannot run its own hub cleanup, and it may
            // even register its connection *after* the abort (it only stops
            // at its next await point): clean up once the task is truly
            // gone, so revocation never leaves the peer registered in the
            // hub (which also closes the connection). A peer re-added in
            // that window may see its fresh connection closed once; its
            // task then simply reconnects.
            let hub = self.hub.clone();
            tokio::spawn(async move {
                let _ = handle.task.await;
                hub.peer_disconnected(&id);
            });
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

    /// Hands an accepted connection to the matching peer task, refusing
    /// endpoints that are not paired.
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

    /// Snapshots the state of every peer for the control socket.
    pub fn statuses(&self) -> Vec<control::PeerStatus> {
        let peers = self.peers.lock().unwrap();

        peers
            .iter()
            .map(|(id, handle)| {
                let connection = match &*handle.state.lock().unwrap() {
                    PeerState::Connecting { failures } => control::ConnectionStatus::Connecting {
                        failures: *failures,
                    },
                    PeerState::Backoff { until, error } => control::ConnectionStatus::Backoff {
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                        error: error.clone(),
                    },
                    PeerState::Connected { conn, since } => control::ConnectionStatus::Connected {
                        path: selected_path(conn),
                        since_secs: since.elapsed().unwrap_or_default().as_secs(),
                    },
                };

                control::PeerStatus {
                    name: self.hub.peer_name(id),
                    endpoint: *id,
                    connection,
                }
            })
            .collect()
    }

    fn spawn_peer(&self, peer_id: EndpointId) -> PeerHandle {
        let state = Arc::new(Mutex::new(PeerState::Connecting { failures: 0 }));
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
    fn shutdown(&self) {
        self.task.abort();
        if let PeerState::Connected { conn, .. } = &*self.state.lock().unwrap() {
            conn.close(0u32.into(), b"peer removed");
        }
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
    let mut failure: Option<String> = None;
    // Whether the current outage was logged: a sleeping or offline peer
    // fails every retry.
    let mut unreachable = false;

    loop {
        // Wait out the backoff, adopting an inbound connection if one
        // arrives in the meantime.
        let mut adopted = None;
        if let Some(error) = failure.take() {
            failures += 1;
            let delay = backoff.next_delay();
            task.set_state(PeerState::Backoff {
                until: Instant::now() + delay,
                error,
            });
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
                if !unreachable {
                    info!(peer = %task.name(), %error, "cannot reach peer, retrying");
                    unreachable = true;
                }
                failure = Some(error);
                continue;
            }
        };

        unreachable = false;
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
            failure = Some("connection dropped right after connecting".to_owned());
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
    async fn establish(&mut self, failures: u32) -> Result<(Connection, bool), String> {
        self.set_state(PeerState::Connecting { failures });

        if self.local_id < self.peer_id {
            dial(&self.endpoint, self.peer_id, &self.hub)
                .await
                .map(|conn| (conn, true))
        } else {
            tokio::select! {
                dialed = dial(&self.endpoint, self.peer_id, &self.hub) => {
                    dialed.map(|conn| (conn, true))
                }
                Some(conn) = self.inbound.recv() => Ok((conn, false)),
            }
        }
    }

    /// Holds an established connection until it closes, resolving duplicate
    /// connections, serving inbound announcement streams, and keeping the
    /// hub's registration current. Returns why the connection ended.
    async fn connected(&mut self, mut conn: Connection, mut outbound: bool) -> ConnectionError {
        let mut since = SystemTime::now();
        // The peer is authenticated but must not spawn unbounded work.
        let announce_permits = Arc::new(Semaphore::new(MAX_UNI_STREAMS));
        let fetch_permits = Arc::new(Semaphore::new(MAX_FETCH_STREAMS));

        self.hub.peer_connected(self.peer_id, &conn);
        let reason = loop {
            self.set_state(PeerState::Connected {
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
                        outbound = false;
                        since = SystemTime::now();
                        self.hub.peer_connected(self.peer_id, &conn);
                    }
                }
                stream = conn.accept_uni() => {
                    let stream = match stream {
                        Ok(stream) => stream,
                        Err(err) => break err,
                    };
                    self.serve_uni(stream, &announce_permits);
                }
                stream = conn.accept_bi() => {
                    let (send, recv) = match stream {
                        Ok(stream) => stream,
                        Err(err) => break err,
                    };
                    self.accept_fetch(send, recv, &fetch_permits);
                }
            }
        };
        self.hub.peer_disconnected(&self.peer_id);
        reason
    }

    /// Reads a fetch request from a fresh bi stream and routes it to the
    /// owning repo task; refused fetches get an error frame back.
    fn accept_fetch(&self, send: SendStream, mut recv: RecvStream, permits: &Arc<Semaphore>) {
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            debug!(peer = %self.name(), "dropping fetch: too many open streams");
            return;
        };

        let hub = self.hub.clone();
        let peer = self.peer_id;
        tokio::spawn(async move {
            let _permit = permit;
            let request = tokio::time::timeout(
                STREAM_READ_TIMEOUT,
                wire::read_message(&mut recv, fetch::MAX_OP_FRAME_SIZE),
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

    /// Reads one uni stream (a [`sync::UniMessage`]) in its own task and
    /// routes it.
    fn serve_uni(&self, mut stream: RecvStream, permits: &Arc<Semaphore>) {
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            debug!(peer = %self.name(), "dropping message: too many open streams");
            return;
        };

        let hub = self.hub.clone();
        let gossip = self.gossip.clone();
        let peer = self.peer_id;
        tokio::spawn(async move {
            let _permit = permit;
            match tokio::time::timeout(STREAM_READ_TIMEOUT, sync::recv_uni(&mut stream)).await {
                Ok(Ok(sync::UniMessage::Announce(announce))) => hub.route(peer, announce),
                Ok(Ok(sync::UniMessage::Status(report))) => hub.route_status(peer, report),
                Ok(Ok(sync::UniMessage::Membership(membership))) => {
                    // A full queue means a membership flood; dropping is
                    // safe, the next change or reconnect re-sends.
                    let name = hub.peer_name(&peer);
                    if let Err(err) = gossip.try_send((name, membership)) {
                        let (name, _) = err.into_inner();
                        debug!(peer = %name, "dropping membership: gossip queue full");
                    }
                }
                Ok(Err(err)) => debug!(peer = %hub.peer_name(&peer), "bad message: {err:#}"),
                Err(_) => debug!(peer = %hub.peer_name(&peer), "message timed out"),
            }
        });
    }

    /// The peer's current name, following renames.
    fn name(&self) -> String {
        self.hub.peer_name(&self.peer_id)
    }

    fn set_state(&self, state: PeerState) {
        *self.state.lock().unwrap() = state;
    }
}

/// Dials the peer on the sync ALPN within [`DIAL_TIMEOUT`], returning why it
/// failed otherwise.
async fn dial(endpoint: &Endpoint, peer: EndpointId, hub: &SyncHub) -> Result<Connection, String> {
    let error = match tokio::time::timeout(DIAL_TIMEOUT, endpoint.connect(peer, sync::ALPN)).await {
        Ok(Ok(conn)) => return Ok(conn),
        Ok(Err(err)) => format!("{err:#}"),
        Err(_) => format!("no answer within {}s", DIAL_TIMEOUT.as_secs()),
    };
    debug!(peer = %hub.peer_name(&peer), "dial failed: {error}");
    Err(error)
}

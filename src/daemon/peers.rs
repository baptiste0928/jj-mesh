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
    time::{Duration, Instant},
};

use iroh::{Endpoint, EndpointId, TransportAddr, endpoint::Connection};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info};

use super::{control, hub::SyncHub};
use crate::{config::Config, net::sync};

/// Maximum announcement streams handled concurrently per peer connection.
const MAX_ANNOUNCE_STREAMS: usize = 16;

/// Budget for reading one announcement stream, so a stalled stream cannot
/// hold its permit indefinitely.
const ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(10);

/// Reconnect delay after a first failure; doubles up to [`BACKOFF_MAX`].
const BACKOFF_MIN: Duration = Duration::from_secs(1);

/// Ceiling of the reconnect delay.
const BACKOFF_MAX: Duration = Duration::from_mins(1);

/// A connection living at least this long resets the backoff. Connections
/// dying younger (e.g. closed as duplicate by the peer, or a flapping peer)
/// advance it instead, so establish-then-close cycles cannot redial hot.
const STABLE_UPTIME: Duration = Duration::from_secs(10);

/// The set of managed peers, synced from the configuration.
///
/// Also acts as the connection allowlist: inbound connections are routed to
/// the matching peer task and refused when the endpoint is not a peer.
#[derive(Debug)]
pub struct PeerSet {
    endpoint: Endpoint,
    local_id: EndpointId,
    hub: Arc<SyncHub>,
    peers: Mutex<BTreeMap<EndpointId, PeerHandle>>,
}

/// Book-keeping for one peer task.
#[derive(Debug)]
struct PeerHandle {
    name: String,
    state: Arc<Mutex<PeerState>>,
    inbound: mpsc::Sender<Connection>,
    task: tokio::task::JoinHandle<()>,
}

/// Live state of one peer connection, shared between its task and status
/// snapshots.
#[derive(Debug)]
enum PeerState {
    Connecting,
    Connected { conn: Connection, since: Instant },
    Backoff { until: Instant },
}

impl PeerSet {
    pub fn new(endpoint: Endpoint, local_id: EndpointId, hub: Arc<SyncHub>) -> Self {
        PeerSet {
            endpoint,
            local_id,
            hub,
            peers: Mutex::new(BTreeMap::new()),
        }
    }

    /// Aligns the managed peers with the configuration: spawns tasks for new
    /// peers and shuts down removed ones. A renamed peer is respawned.
    pub fn sync(&self, config: &Config) {
        let desired: BTreeMap<EndpointId, &str> = config
            .peers
            .iter()
            .map(|(name, peer)| (peer.endpoint, name.as_str()))
            .collect();

        let mut peers = self.peers.lock().unwrap();

        peers.retain(|id, handle| {
            let Some(name) = desired.get(id) else {
                info!(peer = %handle.name, "removing peer");
                handle.shutdown();
                // The task cannot run its own hub cleanup once aborted, and
                // revocation must not leave the peer receiving
                // announcements (the hub also closes the connection).
                self.hub.peer_disconnected(id);
                return false;
            };

            if handle.name != *name {
                // Renaming must not drop the live connection; the task
                // keeps logging under its spawn-time name.
                info!(old = %handle.name, new = %name, "renaming peer");
                (*name).clone_into(&mut handle.name);
            }
            true
        });

        for (id, name) in desired {
            peers.entry(id).or_insert_with(|| {
                info!(peer = %name, "managing peer");
                self.spawn_peer(id, name.to_owned())
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
            debug!(peer = %handle.name, "dropping surplus inbound connection");
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
                    PeerState::Connecting => control::ConnectionStatus::Connecting,
                    PeerState::Backoff { until } => control::ConnectionStatus::Backoff {
                        retry_in_secs: until.saturating_duration_since(Instant::now()).as_secs(),
                    },
                    PeerState::Connected { conn, since } => control::ConnectionStatus::Connected {
                        path: selected_path(conn),
                        since_secs: since.elapsed().as_secs(),
                    },
                };

                control::PeerStatus {
                    name: handle.name.clone(),
                    endpoint: *id,
                    connection,
                }
            })
            .collect()
    }

    fn spawn_peer(&self, peer_id: EndpointId, name: String) -> PeerHandle {
        let state = Arc::new(Mutex::new(PeerState::Connecting));
        let (tx, rx) = mpsc::channel(4);

        let task = tokio::spawn(run_peer(PeerTask {
            endpoint: self.endpoint.clone(),
            local_id: self.local_id,
            peer_id,
            name: name.clone(),
            state: state.clone(),
            hub: self.hub.clone(),
            inbound: rx,
        }));

        PeerHandle {
            name,
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
    name: String,
    state: Arc<Mutex<PeerState>>,
    hub: Arc<SyncHub>,
    inbound: mpsc::Receiver<Connection>,
}

/// Maintains the connection to one peer forever.
async fn run_peer(mut task: PeerTask) {
    let mut backoff = BACKOFF_MIN;
    // Delay to respect before the next attempt, from a previous failure.
    let mut delay = None;

    loop {
        // Wait out the backoff, adopting an inbound connection if one
        // arrives in the meantime.
        let mut adopted = None;
        if let Some(delay) = delay.take() {
            task.set_state(PeerState::Backoff {
                until: Instant::now() + delay,
            });
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                Some(conn) = task.inbound.recv() => adopted = Some((conn, false)),
            }
        }

        let established = match adopted {
            Some(adopted) => Some(adopted),
            None => task.establish().await,
        };
        let Some((conn, outbound)) = established else {
            delay = Some(next_backoff(&mut backoff));
            continue;
        };

        let held = Instant::now();
        info!(peer = %task.name, outbound, "peer connected");
        task.connected(conn, outbound).await;
        info!(peer = %task.name, "peer disconnected");

        if held.elapsed() >= STABLE_UPTIME {
            backoff = BACKOFF_MIN;
        } else {
            delay = Some(next_backoff(&mut backoff));
        }
    }
}

impl PeerTask {
    /// Dials the peer, returning `None` on failure.
    ///
    /// The lower endpoint id prefers completing its own dial, leaving
    /// inbound connections queued for the duplicate tie-break; the higher id
    /// adopts whichever lands first. First-wins on both sides could adopt
    /// mirrored connections that the other side just abandoned, redialing
    /// in a loop.
    async fn establish(&mut self) -> Option<(Connection, bool)> {
        self.set_state(PeerState::Connecting);

        if self.local_id < self.peer_id {
            match self.endpoint.connect(self.peer_id, sync::ALPN).await {
                Ok(conn) => Some((conn, true)),
                Err(err) => {
                    debug!(peer = %self.name, "dial failed: {err:#}");
                    None
                }
            }
        } else {
            tokio::select! {
                res = self.endpoint.connect(self.peer_id, sync::ALPN) => match res {
                    Ok(conn) => Some((conn, true)),
                    Err(err) => {
                        debug!(peer = %self.name, "dial failed: {err:#}");
                        None
                    }
                },
                Some(conn) = self.inbound.recv() => Some((conn, false)),
            }
        }
    }

    /// Holds an established connection until it closes, resolving duplicate
    /// connections, serving inbound announcement streams, and keeping the
    /// hub's registration current.
    async fn connected(&mut self, mut conn: Connection, mut outbound: bool) {
        let mut since = Instant::now();
        // The peer is authenticated but must not spawn unbounded work.
        let announce_permits = Arc::new(Semaphore::new(MAX_ANNOUNCE_STREAMS));

        self.hub.peer_connected(self.peer_id, &conn);
        loop {
            self.set_state(PeerState::Connected {
                conn: conn.clone(),
                since,
            });

            tokio::select! {
                reason = conn.closed() => {
                    debug!(peer = %self.name, "connection closed: {reason}");
                    break;
                }
                Some(new) = self.inbound.recv() => {
                    // Keep the connection dialed by the lower endpoint id:
                    // both sides pick the same one, so the duplicate dies
                    // without killing the surviving connection.
                    if outbound && self.local_id < self.peer_id {
                        new.close(0u32.into(), b"duplicate");
                    } else {
                        conn.close(0u32.into(), b"duplicate");
                        conn = new;
                        outbound = false;
                        since = Instant::now();
                        self.hub.peer_connected(self.peer_id, &conn);
                    }
                }
                stream = conn.accept_uni() => {
                    let Ok(stream) = stream else {
                        // The connection is going away; the closed() branch
                        // would report the same on the next iteration.
                        debug!(peer = %self.name, "connection lost");
                        break;
                    };
                    self.serve_announce(stream, &announce_permits);
                }
            }
        }
        self.hub.peer_disconnected(&self.peer_id);
    }

    /// Reads one announcement stream in its own task and routes it.
    fn serve_announce(&self, mut stream: iroh::endpoint::RecvStream, permits: &Arc<Semaphore>) {
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            debug!(peer = %self.name, "dropping announcement: too many open streams");
            return;
        };

        let hub = self.hub.clone();
        let peer = self.peer_id;
        let name = self.name.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match tokio::time::timeout(ANNOUNCE_TIMEOUT, sync::recv_announce(&mut stream)).await {
                Ok(Ok(announce)) => hub.route(peer, announce),
                Ok(Err(err)) => debug!(peer = %name, "bad announcement: {err:#}"),
                Err(_) => debug!(peer = %name, "announcement timed out"),
            }
        });
    }

    fn set_state(&self, state: PeerState) {
        *self.state.lock().unwrap() = state;
    }
}

/// Returns the jittered delay to wait and advances the backoff.
fn next_backoff(backoff: &mut Duration) -> Duration {
    let delay = jitter(*backoff);
    *backoff = (*backoff * 2).min(BACKOFF_MAX);
    delay
}

/// Jitters a delay by ±20% so peers don't reconnect in lockstep.
fn jitter(base: Duration) -> Duration {
    base.mul_f64(rand::random_range(0.8..1.2))
}

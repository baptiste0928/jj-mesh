//! Long-running sync daemon.
//!
//! Maintains persistent connections to every paired peer (allowlisted at
//! accept), watches every registered repo for op head changes and announces
//! them to peers, and serves live state on the control socket. The daemon
//! owns the mesh state (`mesh.json`): every mutation arrives through the
//! control socket or the membership gossip, and is persisted here.

mod backoff;
pub mod control;
mod hub;
mod pairing;
mod peers;
mod repos;
mod store;

use std::{sync::Arc, time::Instant};

use color_eyre::eyre::{Result, eyre};
use iroh::{Endpoint, EndpointId};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info, warn};

use self::{
    control::{ControlContext, ControlServer},
    hub::SyncHub,
    pairing::Pairing,
    peers::PeerSet,
    repos::RepoSet,
    store::MeshStore,
};
use crate::{
    config::{ConfigDir, MachineKey, Membership, MeshState},
    net::{EndpointOptions, bind_endpoint, pair, sync},
};

/// Maximum in-flight handshakes of not-yet-authenticated connections.
const MAX_PENDING_HANDSHAKES: usize = 32;

/// Pending inbound memberships; merging is fast, so a full channel means a
/// flood, and dropped snapshots are healed by the anti-entropy re-gossip.
const GOSSIP_QUEUE: usize = 16;

/// How often the membership is re-gossiped even when nothing changed, so a
/// snapshot dropped under load is not lost until the next change.
const GOSSIP_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

/// Handshake budget, so stalled attempts release their permit quickly
/// instead of waiting out the transport-level timeout.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The ALPNs the daemon always serves. The pairing window adds the pair
/// ALPN on top of these for its lifetime.
pub(crate) fn base_alpns() -> Vec<Vec<u8>> {
    vec![sync::ALPN.to_vec()]
}

/// A running daemon: all subsystems spawned, endpoint bound, control
/// socket served.
///
/// The binary drives it through [`run`]; tests start and stop it directly.
pub struct Daemon {
    tasks: tokio::task::JoinSet<()>,
    endpoint: Endpoint,
}

impl Daemon {
    /// Starts every daemon subsystem.
    pub async fn start(dir: &ConfigDir, options: &EndpointOptions) -> Result<Self> {
        let key = MachineKey::from_config(dir)?;
        let state = MeshState::load(dir)?;

        // Binding the control socket first doubles as the single-daemon
        // guard.
        let server = ControlServer::bind(dir)?;

        let endpoint = bind_endpoint(&key, base_alpns(), options).await?;
        info!("daemon started, endpoint id {}", key.endpoint_id());

        let hub = Arc::new(SyncHub::new());
        let (gossip_tx, gossip_rx) = mpsc::channel(GOSSIP_QUEUE);
        let peers = Arc::new(PeerSet::new(
            endpoint.clone(),
            key.endpoint_id(),
            hub.clone(),
            gossip_tx,
        ));
        let repos = Arc::new(RepoSet::new(hub.clone()));
        let store = Arc::new(MeshStore::new(
            dir.clone(),
            state,
            peers.clone(),
            repos.clone(),
            hub.clone(),
        ));
        let pairing = Arc::new(Pairing::new(endpoint.clone(), options.uses_relays()));

        let ctx = Arc::new(ControlContext {
            endpoint: endpoint.clone(),
            started: Instant::now(),
            peers: peers.clone(),
            repos,
            hub,
            store: store.clone(),
            pairing: pairing.clone(),
        });

        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { server.serve(ctx).await });
        tasks.spawn(accept_loop(endpoint.clone(), peers, pairing));
        tasks.spawn(membership_loop(gossip_rx, key.endpoint_id(), store.clone()));
        tasks.spawn(async move {
            loop {
                tokio::time::sleep(GOSSIP_INTERVAL).await;
                store.republish_membership();
            }
        });

        Ok(Daemon { tasks, endpoint })
    }

    /// Resolves when a subsystem terminates on its own. That is always
    /// fatal: it would leave a zombie daemon (no control socket, or no
    /// inbound connections) that still looks healthy.
    pub async fn failed(&mut self) -> color_eyre::Report {
        let res = self.tasks.join_next().await;
        eyre!("a daemon subsystem terminated unexpectedly: {res:?}")
    }

    /// Stops all subsystems and closes the endpoint. Waits for the aborted
    /// tasks to finish, so the control socket is released (its file
    /// removed, its lock dropped) before returning.
    pub async fn shutdown(mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        self.endpoint.close().await;
    }
}

/// Runs the daemon until SIGINT or SIGTERM.
pub async fn run(dir: &ConfigDir) -> Result<()> {
    let mut daemon = Daemon::start(dir, &EndpointOptions::default()).await?;

    let outcome = tokio::select! {
        () = wait_for_shutdown() => {
            info!("shutting down");
            Ok(())
        }
        err = daemon.failed() => Err(err),
    };

    daemon.shutdown().await;
    outcome
}

/// Accepts incoming connections and routes them by ALPN: sync connections
/// to their peer task (the `PeerSet` refuses unpaired endpoints), pairing
/// connections to the open pairing window.
async fn accept_loop(endpoint: Endpoint, peers: Arc<PeerSet>, pairing: Arc<Pairing>) {
    // Identity is only known after the handshake, so anyone can start one:
    // bound how many run concurrently.
    let handshakes = Arc::new(Semaphore::new(MAX_PENDING_HANDSHAKES));

    while let Some(incoming) = endpoint.accept().await {
        let Ok(permit) = handshakes.clone().try_acquire_owned() else {
            debug!("dropping incoming connection: too many pending handshakes");
            continue;
        };
        let Ok(connecting) = incoming.accept() else {
            continue;
        };

        // Handshakes complete in their own task so a slow one cannot hold
        // the accept loop.
        let peers = peers.clone();
        let pairing = pairing.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting).await {
                Ok(Ok(conn)) if conn.alpn() == sync::ALPN => peers.route_inbound(conn),
                Ok(Ok(conn)) if conn.alpn() == pair::ALPN => pairing.route_inbound(conn),
                Ok(Ok(conn)) => {
                    debug!("closing connection with unexpected ALPN");
                    conn.close(0u32.into(), b"unexpected alpn");
                }
                Ok(Err(err)) => debug!("incoming connection failed: {err}"),
                Err(_) => debug!("incoming handshake timed out"),
            }
        });
    }
}

/// Merges memberships received from peers into the mesh state. A merge
/// that changes anything is persisted and re-broadcast by the store, which
/// is what propagates membership across machines that are not directly
/// exchanging right now; a merge that changes nothing is silent, which is
/// what stops the echo.
async fn membership_loop(
    mut gossip: mpsc::Receiver<(EndpointId, Membership)>,
    local: EndpointId,
    store: Arc<MeshStore>,
) {
    while let Some((peer, membership)) = gossip.recv().await {
        let merged = store.update(|state| {
            state.merge_membership(&membership, &local);
            Ok(())
        });
        if let Err(err) = merged {
            warn!("cannot apply membership from {peer}: {err:#}");
        }
    }
}

/// Resolves on SIGINT or SIGTERM.
async fn wait_for_shutdown() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler must install");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

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

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

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
    config::{ConfigDir, MachineKey, Membership, MeshState, Settings},
    net::{EndpointOptions, bind_endpoint, pair, sync},
    repo,
};

/// Maximum in-flight handshakes of not-yet-authenticated connections.
const MAX_PENDING_HANDSHAKES: usize = 32;

/// Pending inbound memberships; merging is fast, so a full channel means a
/// flood, and dropped snapshots are healed by the anti-entropy re-gossip.
const GOSSIP_QUEUE: usize = 16;

/// How often the membership is re-gossiped even when nothing changed, so a
/// snapshot dropped under load is not lost until the next change.
const GOSSIP_INTERVAL: Duration = Duration::from_mins(5);

/// Handshake budget, so stalled attempts release their permit quickly
/// instead of waiting out the transport-level timeout.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Coalescing window for status changes: a failing repo can flip states
/// quickly, and peers only care about the latest.
const STATUS_DEBOUNCE: Duration = Duration::from_secs(1);

/// The ALPNs the daemon serves. The pairing ALPN is always among them:
/// pairing is gated by the one-time ticket, not by ALPN exposure, and
/// unknown endpoints can complete handshakes on the sync ALPN anyway
/// (they are refused post-handshake by the peer allowlist).
fn alpns() -> Vec<Vec<u8>> {
    vec![sync::ALPN.to_vec(), pair::ALPN.to_vec()]
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

        let endpoint = bind_endpoint(&key, alpns(), options).await?;
        info!("daemon started, endpoint id {}", key.endpoint_id());

        // The local jj version is a warning signal, never a gate: the
        // binary on the daemon's PATH is only a proxy for whichever jj
        // actually writes the repos.
        let jj_version = tokio::task::spawn_blocking(repo::local_jj_version)
            .await
            .unwrap_or(None);
        if let Some(warning) = repo::jj_version_warning(jj_version.as_deref()) {
            warn!("{warning}");
        }

        // Settings are loaded once here: edits to config.toml apply on
        // the next daemon restart. A broken file must not keep the daemon
        // down: fall back to the defaults.
        if let Err(err) = Settings::write_template(dir) {
            warn!("cannot write the config.toml template: {err:#}");
        }
        let settings = Arc::new(Settings::load(dir).unwrap_or_else(|err| {
            warn!("cannot load config.toml, using defaults: {err:#}");
            Settings::default()
        }));

        let hub = Arc::new(SyncHub::new());
        let (gossip_tx, gossip_rx) = mpsc::channel(GOSSIP_QUEUE);
        let peers = Arc::new(PeerSet::new(
            endpoint.clone(),
            key.endpoint_id(),
            hub.clone(),
            gossip_tx,
        ));
        let repos = Arc::new(RepoSet::new(hub.clone(), settings));
        let store = Arc::new(MeshStore::new(
            dir.clone(),
            key.endpoint_id(),
            state,
            peers.clone(),
            repos.clone(),
            hub.clone(),
        ));
        let pairing = Arc::new(Pairing::new(
            endpoint.clone(),
            options.uses_relays(),
            store.clone(),
        ));

        let ctx = Arc::new(ControlContext {
            endpoint: endpoint.clone(),
            started: SystemTime::now(),
            peers: peers.clone(),
            repos: repos.clone(),
            hub: hub.clone(),
            store: store.clone(),
            pairing: pairing.clone(),
            jj_version: jj_version.clone(),
        });

        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { server.serve(ctx).await });
        tasks.spawn(accept_loop(endpoint.clone(), peers, pairing));
        tasks.spawn(membership_loop(gossip_rx, store.clone()));
        tasks.spawn(status_loop(repos, hub, jj_version));
        tasks.spawn(gossip_loop(store));

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
/// connections to a pairing exchange (refused unless a ticket is
/// outstanding).
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
            // The permit is captured by the task and released when it
            // ends, covering the whole handshake.
            match tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting).await {
                Ok(Ok(conn)) if conn.alpn() == sync::ALPN => peers.route_inbound(conn),
                Ok(Ok(conn)) if conn.alpn() == pair::ALPN => {
                    // The exchange outlives the handshake phase; release
                    // the permit and let pairing bound its own work.
                    drop(permit);
                    pairing.serve_inbound(conn).await;
                }
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

/// Broadcasts this machine's health to the peers on repo state changes,
/// publishing only when it changed: an idle mesh stays silent. A report
/// lost with its connection is replayed on reconnect.
async fn status_loop(repos: Arc<RepoSet>, hub: Arc<SyncHub>, jj_version: Option<String>) {
    let mut published: Option<sync::StatusReport> = None;
    loop {
        let report = sync::StatusReport {
            daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
            jj_version: jj_version.clone(),
            repos: repos.health(),
        };
        if published.as_ref() != Some(&report) {
            hub.publish_status(report.clone());
            published = Some(report);
        }
        repos.changed().await;
        tokio::time::sleep(STATUS_DEBOUNCE).await;
    }
}

/// Merges memberships received from peers into the mesh state.
async fn membership_loop(
    mut gossip: mpsc::Receiver<(EndpointId, Membership)>,
    store: Arc<MeshStore>,
) {
    while let Some((peer, membership)) = gossip.recv().await {
        if let Err(err) = store.merge_membership(&membership) {
            warn!("cannot apply membership from {peer}: {err:#}");
        }
    }
}

/// Re-gossips the membership every [`GOSSIP_INTERVAL`], even when nothing
/// changed, so a snapshot dropped under load is not lost until the next
/// change.
async fn gossip_loop(store: Arc<MeshStore>) {
    loop {
        tokio::time::sleep(GOSSIP_INTERVAL).await;
        store.republish_membership();
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

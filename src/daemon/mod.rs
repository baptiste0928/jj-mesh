//! Long-running sync daemon.
//!
//! Phase 1 skeleton: maintains persistent connections to every paired peer
//! (allowlisted at accept), watches the configuration for changes, and
//! serves live state on the control socket. Repo syncing plugs in next.

pub mod control;
mod pairing;
mod peers;

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use color_eyre::eyre::{Result, eyre};
use iroh::Endpoint;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use self::{
    control::{ControlContext, ControlServer},
    pairing::Pairing,
    peers::PeerSet,
};
use crate::{
    config::{Config, ConfigDir, ConfigWatcher, MachineKey},
    net::{bind_endpoint, pair, sync},
};

/// Maximum in-flight handshakes of not-yet-authenticated connections.
const MAX_PENDING_HANDSHAKES: usize = 32;

/// Runs the daemon until SIGINT or SIGTERM.
pub async fn run(dir: &ConfigDir) -> Result<()> {
    let key = MachineKey::from_config(dir)?;
    let config = Config::from_config(dir)?;
    let watcher = ConfigWatcher::new(dir)?;

    // Binding the control socket first doubles as the single-daemon guard.
    let server = ControlServer::bind(dir)?;

    let endpoint = bind_endpoint(&key, vec![sync::ALPN.to_vec()]).await?;
    info!("daemon started, endpoint id {}", key.endpoint_id());

    let peers = Arc::new(PeerSet::new(endpoint.clone(), key.endpoint_id()));
    peers.sync(&config);
    let config = Arc::new(Mutex::new(config));
    let pairing = Arc::new(Pairing::new(endpoint.clone()));

    let ctx = Arc::new(ControlContext {
        dir: dir.clone(),
        endpoint: endpoint.clone(),
        started: Instant::now(),
        peers: peers.clone(),
        config: config.clone(),
        pairing: pairing.clone(),
    });

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move { server.serve(ctx).await });
    tasks.spawn(accept_loop(endpoint.clone(), peers.clone(), pairing));
    tasks.spawn(reload_loop(watcher, dir.clone(), peers, config));

    // A subsystem ending on its own would leave a zombie daemon (no config
    // reloads, or no inbound connections) that still looks healthy: treat
    // it as fatal instead.
    let outcome = tokio::select! {
        () = wait_for_shutdown() => {
            info!("shutting down");
            Ok(())
        }
        res = tasks.join_next() => {
            Err(eyre!("a daemon subsystem terminated unexpectedly: {res:?}"))
        }
    };

    // Aborting drops the control server, which removes its socket file.
    tasks.abort_all();
    endpoint.close().await;

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
            match connecting.await {
                Ok(conn) if conn.alpn() == pair::ALPN => {
                    pairing.route_inbound(conn);
                }
                Ok(conn) => peers.route_inbound(conn),
                Err(err) => debug!("incoming connection failed: {err}"),
            }
        });
    }
}

/// Reloads the configuration whenever it changes on disk.
async fn reload_loop(
    mut watcher: ConfigWatcher,
    dir: ConfigDir,
    peers: Arc<PeerSet>,
    config: Arc<Mutex<Config>>,
) {
    loop {
        if let Err(err) = watcher.changed().await {
            warn!("config watcher failed: {err}");
            return;
        }

        match Config::from_config(&dir) {
            Ok(new) => {
                info!("configuration changed, reloading");
                peers.sync(&new);
                *config.lock().unwrap() = new;
            }
            Err(err) => warn!("keeping previous configuration: {err:#}"),
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

//! Long-running sync daemon.
//!
//! Phase 1 skeleton: maintains persistent connections to every paired peer
//! (allowlisted at accept), watches the configuration for changes, and
//! serves live state on the control socket. Repo syncing plugs in next.

pub mod control;
mod peers;

use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use color_eyre::eyre::{Result, eyre};
use iroh::Endpoint;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use self::{control::ControlServer, peers::PeerSet};
use crate::{
    config::{Config, ConfigDir, ConfigWatcher, MachineKey},
    net::{bind_endpoint, sync},
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

    let started = Instant::now();
    let peers = Arc::new(PeerSet::new(endpoint.clone(), key.endpoint_id()));
    peers.sync(&config);
    let config = Arc::new(Mutex::new(config));

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(serve_control(
        server,
        started,
        key,
        peers.clone(),
        config.clone(),
    ));
    tasks.spawn(accept_loop(endpoint.clone(), peers.clone()));
    tasks.spawn(reload_loop(watcher, dir.clone(), peers.clone(), config));

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

/// Serves the control socket, snapshotting the daemon state per request.
async fn serve_control(
    server: ControlServer,
    started: Instant,
    key: MachineKey,
    peers: Arc<PeerSet>,
    config: Arc<Mutex<Config>>,
) {
    server
        .serve(move || control::Status {
            endpoint: key.endpoint_id(),
            uptime_secs: started.elapsed().as_secs(),
            peers: peers.statuses(),
            repos: config
                .lock()
                .unwrap()
                .repos
                .iter()
                .map(|(name, repo)| control::RepoStatus {
                    name: name.clone(),
                    path: repo.path.clone(),
                })
                .collect(),
        })
        .await;
}

/// Accepts incoming connections and routes them to their peer task; the
/// `PeerSet` refuses endpoints that are not paired.
async fn accept_loop(endpoint: Endpoint, peers: Arc<PeerSet>) {
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
        tokio::spawn(async move {
            let _permit = permit;
            match connecting.await {
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

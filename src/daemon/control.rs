//! Daemon control socket.
//!
//! The CLI talks to the running daemon over a unix socket with
//! length-prefixed postcard messages (see [`crate::net::wire`]). Most
//! requests are one request/response exchange; hosting a pairing gets two
//! responses (the ticket, then the outcome) on one connection.
//!
//! The daemon is the only holder of the machine-key endpoint, so both live
//! peer state and pairing are only reachable through here.

use std::{
    fs::{self, File, TryLockError},
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, eyre};
use iroh::{Endpoint, EndpointId};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    net::{UnixListener, UnixStream},
};
use tracing::{debug, info, warn};

use super::{pairing::Pairing, peers::PeerSet};
use crate::{
    config::{Config, ConfigDir, ConfigEdit, Peer},
    net::{
        pair,
        wire::{read_message, write_message},
    },
};

/// Maximum accepted size of a control message.
const MAX_MESSAGE_SIZE: u32 = 1 << 20;

/// Time budget for the quick parts of an exchange (request, status answer).
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Time budget for a whole join exchange, from dialing to completion.
const JOIN_TIMEOUT: Duration = Duration::from_mins(1);

/// Hard cap on a pairing window: bounds how long the unknown-endpoint pair
/// ALPN stays exposed when a host CLI is left waiting unattended.
const WINDOW_TIMEOUT: Duration = Duration::from_mins(15);

/// Initial retry delay after an accept error, escalating to the max.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// A request from the CLI to the daemon.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Report the daemon state; answered with [`Response::Status`].
    Status,
    /// Host a pairing: open the window and issue a ticket. Answered with
    /// [`Response::PairTicket`] immediately, then [`Response::Paired`] or
    /// [`Response::Error`] once the exchange concludes. The window closes
    /// when the requesting client disconnects.
    PairHost { name: String },
    /// Join a pairing hosted by another machine. Answered with
    /// [`Response::Paired`] or [`Response::Error`].
    PairJoin { ticket: String, name: String },
}

/// A daemon answer to a [`Request`].
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Status(Status),
    /// The pairing ticket to transmit to the other machine.
    PairTicket(String),
    /// Pairing succeeded and the peer is saved in the configuration.
    Paired {
        name: String,
        endpoint: EndpointId,
    },
    /// The request failed.
    Error(String),
}

/// Live daemon state, answering [`Request::Status`].
#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    /// This machine's endpoint id.
    pub endpoint: EndpointId,
    /// Seconds since the daemon started.
    pub uptime_secs: u64,
    /// State of every configured peer.
    pub peers: Vec<PeerStatus>,
    /// Repos registered on this machine.
    pub repos: Vec<RepoStatus>,
}

/// Live state of one configured peer.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeerStatus {
    pub name: String,
    pub endpoint: EndpointId,
    pub connection: ConnectionStatus,
}

/// State of the persistent connection to a peer.
#[derive(Debug, Serialize, Deserialize)]
pub enum ConnectionStatus {
    /// Dialing, or waiting for the peer to dial us.
    Connecting,
    /// Connection established.
    Connected {
        /// The selected network path, when already known.
        path: Option<PathInfo>,
        /// Seconds since the connection was established.
        since_secs: u64,
    },
    /// Last attempt failed; waiting before redialing.
    Backoff {
        /// Seconds until the next attempt.
        retry_in_secs: u64,
    },
}

/// The selected network path of a peer connection.
#[derive(Debug, Serialize, Deserialize)]
pub struct PathInfo {
    pub route: Route,
    /// Round-trip time on this path, in milliseconds.
    pub rtt_ms: u64,
}

/// How traffic reaches the peer.
#[derive(Debug, Serialize, Deserialize)]
pub enum Route {
    /// Hole-punched direct path to this socket address.
    Direct { addr: String },
    /// Traffic goes through this relay.
    Relay { url: String },
}

/// A repo registered in the configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct RepoStatus {
    pub name: String,
    pub path: PathBuf,
}

/// Everything the control handlers need from the daemon.
#[derive(Debug)]
pub struct ControlContext {
    pub dir: ConfigDir,
    pub endpoint: Endpoint,
    pub started: Instant,
    pub peers: Arc<PeerSet>,
    pub config: Arc<Mutex<Config>>,
    pub pairing: Arc<Pairing>,
}

impl ControlContext {
    /// Snapshots the daemon state.
    fn status(&self) -> Status {
        Status {
            endpoint: self.endpoint.secret_key().public(),
            uptime_secs: self.started.elapsed().as_secs(),
            peers: self.peers.statuses(),
            repos: self
                .config
                .lock()
                .unwrap()
                .repos
                .iter()
                .map(|(name, repo)| RepoStatus {
                    name: name.clone(),
                    path: repo.path.clone(),
                })
                .collect(),
        }
    }

    /// Clones the current configuration.
    fn config(&self) -> Config {
        self.config.lock().unwrap().clone()
    }
}

/// Server side of the control socket.
///
/// Dropping the server removes the socket file, covering both graceful
/// shutdown and early setup errors.
#[derive(Debug)]
pub struct ControlServer {
    listener: UnixListener,
    path: PathBuf,
    /// Exclusive lock held for the daemon's lifetime; this is the
    /// single-daemon guard (the socket file alone cannot arbitrate
    /// concurrent starts).
    _lock: File,
}

impl ControlServer {
    /// Binds the control socket, also acting as the single-daemon guard.
    pub fn bind(dir: &ConfigDir) -> Result<Self> {
        let path = dir.socket_path();

        let lock_path = path.with_extension("lock");
        let lock = File::create(&lock_path)
            .wrap_err_with(|| format!("cannot create {}", lock_path.display()))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                bail!("another jj-mesh daemon is already running");
            }
            Err(TryLockError::Error(err)) => {
                return Err(err).wrap_err_with(|| format!("cannot lock {}", lock_path.display()));
            }
        }

        // Bind on a temporary path and rename into place: the socket is
        // never observable with permissive modes, and a stale socket left
        // by a crash is replaced atomically.
        let tmp = path.with_extension("sock.tmp");
        let _ = fs::remove_file(&tmp);
        let listener = UnixListener::bind(&tmp)
            .wrap_err_with(|| format!("cannot bind control socket {}", tmp.display()))?;
        fs::set_permissions(&tmp, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        fs::rename(&tmp, &path)
            .wrap_err_with(|| format!("cannot move control socket to {}", path.display()))?;

        Ok(ControlServer {
            listener,
            path,
            _lock: lock,
        })
    }

    /// Serves control requests forever.
    pub async fn serve(self, ctx: Arc<ControlContext>) -> ! {
        let mut error_backoff = ACCEPT_ERROR_BACKOFF;

        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    error_backoff = ACCEPT_ERROR_BACKOFF;
                    tokio::spawn(handle_client(stream, ctx.clone()));
                }
                Err(err) => {
                    // Persistent errors (e.g. fd exhaustion) escalate the
                    // retry delay instead of hot-logging forever.
                    warn!("control socket accept failed: {err}");
                    tokio::time::sleep(error_backoff).await;
                    error_backoff = (error_backoff * 2).min(ACCEPT_ERROR_BACKOFF_MAX);
                }
            }
        }
    }
}

impl Drop for ControlServer {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Answers one client connection.
async fn handle_client(mut stream: UnixStream, ctx: Arc<ControlContext>) {
    let request =
        match tokio::time::timeout(CLIENT_TIMEOUT, read_message(&mut stream, MAX_MESSAGE_SIZE))
            .await
        {
            Ok(Ok(request)) => request,
            Ok(Err(err)) => return debug!("control client error: {err}"),
            Err(_) => return debug!("control client timed out"),
        };

    let served = match request {
        Request::Status => respond(&mut stream, &Response::Status(ctx.status())).await,
        Request::PairHost { name } => pair_host(&mut stream, &ctx, &name).await,
        Request::PairJoin { ticket, name } => pair_join(&mut stream, &ctx, &ticket, &name).await,
    };

    if let Err(err) = served {
        debug!("control client error: {err}");
    }
}

/// Writes a response, bounded so a client that stopped reading cannot park
/// the daemon task.
async fn respond(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    response: &Response,
) -> Result<()> {
    tokio::time::timeout(
        CLIENT_TIMEOUT,
        write_message(stream, response, MAX_MESSAGE_SIZE),
    )
    .await
    .map_err(|_| eyre!("control client stopped reading"))?
}

/// Hosts a pairing: opens the window, reports the ticket, then the outcome.
/// The window closes when this function returns: on completion, expiry, or
/// the client disconnecting.
async fn pair_host(stream: &mut UnixStream, ctx: &ControlContext, name: &str) -> Result<()> {
    let mut window = match ctx.pairing.open().await {
        Ok(window) => window,
        Err(err) => return respond(stream, &Response::Error(format!("{err:#}"))).await,
    };

    respond(stream, &Response::PairTicket(window.ticket().to_string())).await?;

    let (mut read_half, mut write_half) = stream.split();
    let result = tokio::select! {
        result = window.wait_for_peer(name, || ctx.config()) => result,
        () = tokio::time::sleep(WINDOW_TIMEOUT) => Err(eyre!(
            "the pairing window expired after {} minutes",
            WINDOW_TIMEOUT.as_secs() / 60,
        )),
        () = client_gone(&mut read_half) => {
            info!("pairing cancelled by the client");
            return Ok(());
        }
    };

    // Persist before confirming: the `paired` close is the joiner's commit
    // signal, so the host must never send it for a peer it did not save.
    let response = match result {
        Ok((peer, conn)) => match persist_peer(&ctx.dir, &peer) {
            Ok(()) => {
                pair::confirm_paired(&conn);
                info!(peer = %peer.name, "paired");
                Response::Paired {
                    name: peer.name,
                    endpoint: peer.endpoint,
                }
            }
            Err(err) => {
                conn.close(0u32.into(), b"failed");
                Response::Error(format!("cannot save the peer: {err:#}"))
            }
        },
        Err(err) => Response::Error(format!("{err:#}")),
    };

    respond(&mut write_half, &response).await
}

/// Joins a pairing hosted by another machine. Aborts the exchange when the
/// client disconnects, so a cancelled join cannot pair behind the user's
/// back.
async fn pair_join(
    stream: &mut UnixStream,
    ctx: &ControlContext,
    ticket: &str,
    name: &str,
) -> Result<()> {
    let (mut read_half, mut write_half) = stream.split();

    let exchange = async {
        let ticket: pair::PairTicket = ticket.parse()?;
        pair::join(&ctx.endpoint, &ticket, name, &ctx.config()).await
    };
    let result = tokio::select! {
        result = tokio::time::timeout(JOIN_TIMEOUT, exchange) => result.unwrap_or_else(|_| {
            Err(eyre!("pairing timed out after {}s", JOIN_TIMEOUT.as_secs()))
        }),
        () = client_gone(&mut read_half) => {
            info!("pairing cancelled by the client");
            return Ok(());
        }
    };

    let persisted = result.and_then(|peer| {
        persist_peer(&ctx.dir, &peer)?;
        Ok(peer)
    });
    let response = match persisted {
        Ok(peer) => {
            info!(peer = %peer.name, "paired");
            Response::Paired {
                name: peer.name,
                endpoint: peer.endpoint,
            }
        }
        Err(err) => Response::Error(format!("{err:#}")),
    };

    respond(&mut write_half, &response).await
}

/// Registers a paired peer in the configuration; a no-op when the endpoint
/// is already registered (idempotent re-pair). The daemon's own config
/// watcher picks the change up and starts connecting to the new peer.
fn persist_peer(dir: &ConfigDir, peer: &pair::PairedPeer) -> Result<()> {
    let mut edit = ConfigEdit::from_config(dir)?;
    if edit.config().peer_name(&peer.endpoint).is_some() {
        return Ok(());
    }

    edit.add_peer(
        peer.name.clone(),
        Peer {
            endpoint: peer.endpoint,
        },
    )?;
    edit.save()
}

/// Resolves when the client closes its end of the connection.
async fn client_gone(read: &mut (impl AsyncRead + Unpin)) {
    let mut buf = [0u8; 64];
    loop {
        match read.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            // Ignore stray bytes: requests are one-per-connection.
            Ok(_) => {}
        }
    }
}

/// Client side of the control socket.
#[derive(Debug)]
pub struct ControlClient {
    stream: UnixStream,
}

impl ControlClient {
    /// Connects to the daemon serving this configuration, or `None` when no
    /// daemon is running.
    pub async fn connect(dir: &ConfigDir) -> Result<Option<Self>> {
        let path = dir.socket_path();

        match UnixStream::connect(&path).await {
            Ok(stream) => Ok(Some(ControlClient { stream })),
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) =>
            {
                Ok(None)
            }
            Err(err) => Err(err).wrap_err_with(|| format!("cannot connect to {}", path.display())),
        }
    }

    /// Sends a request.
    pub async fn send(&mut self, request: &Request) -> Result<()> {
        write_message(&mut self.stream, request, MAX_MESSAGE_SIZE).await
    }

    /// Receives the next response, bounded by `limit` when given.
    pub async fn recv(&mut self, limit: Option<Duration>) -> Result<Response> {
        let read = read_message(&mut self.stream, MAX_MESSAGE_SIZE);
        match limit {
            Some(limit) => tokio::time::timeout(limit, read)
                .await
                .map_err(|_| eyre!("the daemon did not answer"))?,
            None => read.await,
        }
    }
}

/// Queries the status of the daemon serving this configuration.
///
/// Returns `None` when no daemon is running; errors when a daemon is
/// listening but does not answer properly.
pub async fn query_status(dir: &ConfigDir) -> Result<Option<Status>> {
    let Some(mut client) = ControlClient::connect(dir).await? else {
        return Ok(None);
    };

    client.send(&Request::Status).await?;
    match client.recv(Some(CLIENT_TIMEOUT)).await? {
        Response::Status(status) => Ok(Some(status)),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

/// Blocking convenience over [`query_status`] for CLI commands that have no
/// tokio runtime of their own.
pub fn query_status_blocking(dir: &ConfigDir) -> Result<Option<Status>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(query_status(dir))
}

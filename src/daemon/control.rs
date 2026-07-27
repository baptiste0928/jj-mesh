//! Daemon control socket.
//!
//! The CLI talks to the running daemon over a unix socket with
//! length-prefixed postcard messages (see [`crate::net::wire`]). Most
//! requests are one request/response exchange; hosting a pairing gets two
//! responses (the ticket, then the outcome) on one connection.
//!
//! The daemon is the only holder of the machine-key endpoint and the only
//! writer of the mesh state, so live peer state, pairing and every mesh
//! mutation go through here.

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

use super::{
    hub::{JoinSource, SyncHub},
    pairing::Pairing,
    peers::PeerSet,
    repos::RepoSet,
};
use crate::{
    config::{ConfigDir, MeshState, Peer, Repo, RepoId},
    net::{
        pair,
        wire::{read_message, write_message},
    },
    repo::{JjRepo, transfer},
};

/// Maximum accepted size of a control message.
const MAX_MESSAGE_SIZE: u32 = 1 << 20;

/// Time budget for the quick parts of an exchange (request, status answer).
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Time budget for a whole join exchange, from dialing to completion.
const JOIN_TIMEOUT: Duration = Duration::from_mins(1);

/// Time budget for a join's initial repo pull; it may transfer an entire
/// repository.
const JOIN_PULL_TIMEOUT: Duration = Duration::from_mins(30);

/// Time budget the CLI grants quick mutating requests (add, remove).
pub const MUTATE_WAIT: Duration = Duration::from_secs(10);

/// Time budget the CLI grants a whole join request: the pull budget plus a
/// margin for validation and registration. Kept here next to the pull
/// budget so the two cannot drift apart.
pub const JOIN_WAIT: Duration = JOIN_PULL_TIMEOUT.saturating_add(Duration::from_mins(1));

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
    /// Pull the full state of the mesh repo named `name` into a freshly
    /// initialized local repo at `path` and register it (see `jj-mesh
    /// join`). Answered with [`Response::Joined`] or [`Response::Error`].
    JoinRepo { name: String, path: PathBuf },
    /// Register the repo at `path` under `name`, with a fresh id. Answered
    /// with [`Response::RepoAdded`] or [`Response::Error`].
    AddRepo { name: String, path: PathBuf },
    /// Remove a paired peer, disconnecting it. Answered with
    /// [`Response::PeerRemoved`] or [`Response::Error`].
    RemovePeer { name: String },
}

/// A daemon answer to a [`Request`].
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Status(Status),
    /// The pairing ticket to transmit to the other machine.
    PairTicket(String),
    /// Pairing succeeded and the peer is saved in the mesh state.
    Paired {
        name: String,
        endpoint: EndpointId,
    },
    /// The join pull completed and the repo is registered.
    Joined {
        ops: u64,
        git_objects: u64,
    },
    /// The repo is registered (with a freshly generated internal id).
    RepoAdded,
    /// The peer is removed from the mesh state.
    PeerRemoved(EndpointId),
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
    /// Repo names contested by peers announcing a different repo.
    pub conflicts: Vec<ConflictStatus>,
}

/// A repo name contested by a peer: it announces a different repo (by id)
/// under a name registered here. Sync with that peer is suspended for the
/// repo until one side renames or removes it.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConflictStatus {
    pub repo: String,
    pub peer: EndpointId,
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

/// A registered repo.
#[derive(Debug, Serialize, Deserialize)]
pub struct RepoStatus {
    pub name: String,
    pub path: PathBuf,
    pub watch: WatchStatus,
}

/// State of the op-heads watch on a repo.
#[derive(Debug, Serialize, Deserialize)]
pub enum WatchStatus {
    /// The repo is being opened.
    Opening,
    /// Watching for op head changes.
    Watching {
        /// Current number of op heads (more than one means divergence).
        op_heads: u64,
        /// Seconds since the last observed change, if any since starting.
        last_change_secs: Option<u64>,
        /// Seconds since operations were last fetched from a peer.
        last_sync_secs: Option<u64>,
    },
    /// Opening or watching failed; waiting before retrying.
    Failed { error: String, retry_in_secs: u64 },
}

/// Everything the control handlers need from the daemon.
#[derive(Debug)]
pub struct ControlContext {
    pub dir: ConfigDir,
    pub endpoint: Endpoint,
    pub started: Instant,
    pub peers: Arc<PeerSet>,
    pub repos: Arc<RepoSet>,
    pub hub: Arc<SyncHub>,
    pub state: Arc<Mutex<MeshState>>,
    pub pairing: Arc<Pairing>,
}

impl ControlContext {
    /// Snapshots the daemon state.
    fn status(&self) -> Status {
        let peers = self.peers.statuses();
        // Conflicts recorded for a since-unpaired peer are dropped: they
        // can no longer be acted on, and the endpoint is not ours to show.
        let conflicts = self
            .hub
            .conflicts()
            .into_iter()
            .filter(|(_, peer)| peers.iter().any(|status| &status.endpoint == peer))
            .map(|(repo, peer)| ConflictStatus { repo, peer })
            .collect();

        Status {
            endpoint: self.endpoint.secret_key().public(),
            uptime_secs: self.started.elapsed().as_secs(),
            peers,
            repos: self.repos.statuses(),
            conflicts,
        }
    }

    /// Clones the current mesh state.
    fn state(&self) -> MeshState {
        self.state.lock().unwrap().clone()
    }

    /// Mutates the mesh state: persists the change to `peers.json` and then
    /// aligns the peer and repo sets with it. Nothing is committed (in
    /// memory or on disk) when the mutation or the save fails. The lock is
    /// deliberately held across the set syncs, so concurrent mutations
    /// apply their syncs in commit order.
    fn update_state<T>(&self, mutate: impl FnOnce(&mut MeshState) -> Result<T>) -> Result<T> {
        let mut state = self.state.lock().unwrap();

        let mut next = state.clone();
        let value = mutate(&mut next)?;
        next.save(&self.dir)?;
        *state = next;

        self.peers.sync(&state);
        self.repos.sync(&state);

        Ok(value)
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
        Request::JoinRepo { name, path } => join_repo(&mut stream, &ctx, &name, &path).await,
        Request::AddRepo { name, path } => add_repo(&mut stream, &ctx, name, path).await,
        Request::RemovePeer { name } => remove_peer(&mut stream, &ctx, &name).await,
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
        result = window.wait_for_peer(name, || ctx.state()) => result,
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
        Ok((peer, conn)) => match persist_peer(ctx, &peer) {
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
        pair::join(&ctx.endpoint, &ticket, name, &ctx.state()).await
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
        persist_peer(ctx, &peer)?;
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

/// Pulls the mesh repo named `name` into the freshly initialized repo at
/// `path`, from a connected peer that announced it, then registers it. The
/// repo's mesh id is adopted from the announcements. The pull runs on an
/// ad-hoc repo handle in this connection's task, as the repo set only
/// manages it once registered.
async fn join_repo(
    stream: &mut UnixStream,
    ctx: &ControlContext,
    name: &str,
    path: &std::path::Path,
) -> Result<()> {
    let joined = async {
        let (repo_id, sources) = ctx.hub.join_sources(name)?;

        // Fail before the (long) pull when the registration cannot succeed.
        // Only the re-validation inside `update_state` below is
        // authoritative: the lock is released during the pull.
        {
            let state = ctx.state.lock().unwrap();
            state.validate_new_repo(name, path)?;
            if let Some(existing) = state.repo_name(&repo_id) {
                bail!("`{name}` is the repo already registered here as `{existing}`");
            }
        }

        let outcome = join_pull(ctx, name, &repo_id, sources, path).await?;

        ctx.update_state(|state| {
            state.add_repo(
                name.to_owned(),
                Repo {
                    id: repo_id.clone(),
                    path: path.to_owned(),
                },
            )
        })?;
        Ok::<_, color_eyre::Report>(outcome)
    }
    .await;

    let response = match joined {
        Ok((ops, git_objects)) => Response::Joined { ops, git_objects },
        Err(err) => Response::Error(format!("{err:#}")),
    };
    respond(stream, &response).await
}

async fn join_pull(
    ctx: &ControlContext,
    name: &str,
    repo_id: &RepoId,
    sources: Vec<JoinSource>,
    path: &std::path::Path,
) -> Result<(u64, u64)> {
    use jj_lib::op_store::OperationId;

    let repo_path = path.to_owned();
    let repo = tokio::task::spawn_blocking(move || -> Result<_> {
        Ok(Arc::new(JjRepo::discover(&repo_path)?.open()?))
    })
    .await
    .wrap_err("repo open task failed")??;

    let mut last_error = eyre!("no usable source peer");
    for (peer, heads) in sources {
        let wants: Vec<OperationId> = heads.into_iter().map(OperationId::new).collect();
        let Some(conn) = ctx.hub.connection(&peer) else {
            continue;
        };

        // The fresh repo's single head is the baseline for the ref mirror.
        let local_heads = repo.op_heads().await?;

        let pull = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            let outcome =
                transfer::fetch(&repo, name, repo_id, &wants, &mut send, &mut recv).await?;
            let _ = send.finish();
            Ok::<_, color_eyre::Report>(outcome)
        };
        match tokio::time::timeout(JOIN_PULL_TIMEOUT, pull).await {
            Err(_) => last_error = eyre!("pull from {peer} timed out"),
            Ok(Err(err)) => last_error = err.wrap_err(format!("pull from {peer} failed")),
            Ok(Ok(outcome)) => {
                // Seed the colocated .git so the first jj command does not
                // misread replicated refs as deletions. With several mesh
                // heads there is no single view to mirror; jj's merge and
                // the next fast-forward sync then converge the refs.
                if let ([mesh_head], [old_head]) = (&outcome.published[..], &local_heads[..]) {
                    transfer::mirror_after_join(&repo, mesh_head, old_head).await?;
                } else {
                    info!("joined with divergent mesh heads; git refs not seeded");
                }
                return Ok((outcome.ops as u64, outcome.git_objects as u64));
            }
        }
        warn!("join pull attempt failed: {last_error:#}");
    }
    Err(last_error)
}

/// Registers the repo at `path` under `name` with a fresh id.
async fn add_repo(
    stream: &mut UnixStream,
    ctx: &ControlContext,
    name: String,
    path: PathBuf,
) -> Result<()> {
    let result = ctx.update_state(|state| {
        state.add_repo(
            name.clone(),
            Repo {
                id: RepoId::generate(),
                path,
            },
        )
    });

    let response = match result {
        Ok(()) => Response::RepoAdded,
        Err(err) => Response::Error(format!("{err:#}")),
    };
    respond(stream, &response).await
}

/// Removes a paired peer; the peer set disconnects it immediately.
async fn remove_peer(stream: &mut UnixStream, ctx: &ControlContext, name: &str) -> Result<()> {
    let response = match ctx.update_state(|state| state.remove_peer(name)) {
        Ok(peer) => {
            info!(peer = %name, "peer removed");
            Response::PeerRemoved(peer.endpoint)
        }
        Err(err) => Response::Error(format!("{err:#}")),
    };
    respond(stream, &response).await
}

/// Registers a paired peer in the mesh state; a no-op when the endpoint is
/// already registered (idempotent re-pair). The peer set starts connecting
/// as part of the update.
fn persist_peer(ctx: &ControlContext, peer: &pair::PairedPeer) -> Result<()> {
    ctx.update_state(|state| {
        if state.peer_name(&peer.endpoint).is_some() {
            return Ok(());
        }
        state.add_peer(
            peer.name.clone(),
            Peer {
                endpoint: peer.endpoint,
            },
        )
    })
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

/// Sends one request and returns the daemon's answer, for CLI commands with
/// no tokio runtime of their own. Errors when no daemon is running, and
/// turns [`Response::Error`] into an error, so callers only match their
/// success variant.
pub fn request_blocking(dir: &ConfigDir, request: &Request, limit: Duration) -> Result<Response> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let Some(mut client) = ControlClient::connect(dir).await? else {
                bail!("the jj-mesh daemon is not running; start it with `jj-mesh daemon` first");
            };

            client.send(request).await?;
            match client.recv(Some(limit)).await? {
                Response::Error(message) => bail!("{message}"),
                response => Ok(response),
            }
        })
}

//! Server side of the control socket: the listener, the per-connection
//! request handlers, and the daemon context they act on.

use std::{
    fs::{self, File, TryLockError},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, eyre};
use iroh::Endpoint;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    net::{UnixListener, UnixStream},
};
use tracing::{debug, info, warn};

use super::protocol::{
    CLIENT_TIMEOUT, ConflictStatus, JOIN_PULL_TIMEOUT, MAX_MESSAGE_SIZE, Request, Response, Status,
};
use crate::{
    config::{ConfigDir, MeshState, Repo, RepoId},
    daemon::{
        backoff::Backoff,
        hub::{JoinSource, SyncHub},
        pairing::Pairing,
        peers::PeerSet,
        repos::RepoSet,
        store::MeshStore,
    },
    net::{
        pair,
        wire::{read_message, write_message},
    },
    repo::{JjRepo, transfer},
};

/// Time budget for a whole join exchange, from dialing to completion.
const JOIN_TIMEOUT: Duration = Duration::from_mins(1);

/// Hard cap on a pairing window: bounds how long the unknown-endpoint pair
/// ALPN stays exposed when a host CLI is left waiting unattended.
const WINDOW_TIMEOUT: Duration = Duration::from_mins(15);

/// Initial retry delay after an accept error, escalating to the max.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Everything the control handlers need from the daemon.
#[derive(Debug)]
pub struct ControlContext {
    pub endpoint: Endpoint,
    pub started: Instant,
    pub peers: Arc<PeerSet>,
    pub repos: Arc<RepoSet>,
    pub hub: Arc<SyncHub>,
    pub store: Arc<MeshStore>,
    pub pairing: Arc<Pairing>,
}

impl ControlContext {
    /// Snapshots the daemon state.
    fn status(&self) -> Status {
        let state = self.state();
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
        // Mesh repos not registered here are joinable.
        let available = state
            .mesh_repo_names()
            .filter(|name| !state.repos.contains_key(*name))
            .map(str::to_owned)
            .collect();

        Status {
            endpoint: self.endpoint.secret_key().public(),
            uptime_secs: self.started.elapsed().as_secs(),
            peers,
            repos: self.repos.statuses(),
            available,
            conflicts,
        }
    }

    /// Clones the current mesh state.
    fn state(&self) -> MeshState {
        self.store.snapshot()
    }

    /// Mutates the mesh state through the store: persisted, applied to the
    /// live sets, and broadcast when the membership changed.
    fn update_state<T>(&self, mutate: impl FnOnce(&mut MeshState) -> Result<T>) -> Result<T> {
        self.store.update(mutate)
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
        let mut backoff = Backoff::new(ACCEPT_ERROR_BACKOFF, ACCEPT_ERROR_BACKOFF_MAX);

        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    backoff.reset();
                    tokio::spawn(handle_client(stream, ctx.clone()));
                }
                Err(err) => {
                    // Persistent errors (e.g. fd exhaustion) escalate the
                    // retry delay instead of hot-logging forever.
                    warn!("control socket accept failed: {err}");
                    tokio::time::sleep(backoff.next_delay()).await;
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

    // Pairing owns its stream (two responses, plus client-cancel handling);
    // the rest return a single response and share one error conversion.
    let served = match request {
        Request::PairHost { name } => pair_host(&mut stream, &ctx, &name).await,
        Request::PairJoin { ticket, name } => pair_join(&mut stream, &ctx, &ticket, &name).await,
        Request::Status => reply(&mut stream, Ok(Response::Status(ctx.status()))).await,
        Request::JoinRepo { name, path } => {
            reply(&mut stream, join_repo(&ctx, &name, &path).await).await
        }
        Request::AddRepo { name, path } => reply(&mut stream, add_repo(&ctx, name, path)).await,
        Request::ForgetRepo { name } => reply(&mut stream, forget_repo(&ctx, &name)).await,
        Request::RemovePeer { peer } => reply(&mut stream, remove_peer(&ctx, &peer)).await,
    };

    if let Err(err) = served {
        debug!("control client error: {err}");
    }
}

/// Sends a handler's outcome, turning any failure into an error response so
/// every handler does not repeat the conversion.
async fn reply(stream: &mut UnixStream, result: Result<Response>) -> Result<()> {
    let response = result.unwrap_or_else(|err| Response::Error(format!("{err:#}")));
    respond(stream, &response).await
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
async fn join_repo(ctx: &ControlContext, name: &str, path: &std::path::Path) -> Result<Response> {
    let (repo_id, sources) = ctx.hub.join_sources(name)?;

    // Fail before the (long) pull when the registration cannot succeed. Only
    // the re-validation inside `update_state` below is authoritative: the
    // state may change during the pull.
    {
        let state = ctx.state();
        state.validate_new_repo(name, path)?;
        state.ensure_mesh_id(name, &repo_id)?;
        if let Some(existing) = state.repo_name(&repo_id) {
            bail!("`{name}` is the repo already registered here as `{existing}`");
        }
    }

    let (ops, git_objects) = join_pull(ctx, name, &repo_id, sources, path).await?;

    ctx.update_state(|state| {
        state.add_repo(
            name.to_owned(),
            Repo {
                id: repo_id.clone(),
                path: path.to_owned(),
            },
        )
    })?;
    Ok(Response::Joined { ops, git_objects })
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
            Ok(Ok(outcome)) => return Ok((outcome.ops as u64, outcome.git_objects as u64)),
        }
        warn!("join pull attempt failed: {last_error:#}");
    }
    Err(last_error)
}

/// Registers the repo at `path` under `name` with a fresh id.
fn add_repo(ctx: &ControlContext, name: String, path: PathBuf) -> Result<Response> {
    ctx.update_state(|state| {
        state.add_repo(
            name,
            Repo {
                id: RepoId::generate(),
                path,
            },
        )
    })?;
    Ok(Response::RepoAdded)
}

/// Retires a repo name from the mesh; the repo set stops watching it here
/// and the gossip propagates the removal to the other machines.
fn forget_repo(ctx: &ControlContext, name: &str) -> Result<Response> {
    let was_local = ctx.update_state(|state| state.forget_repo(name))?;
    info!(repo = %name, "repo forgotten");
    Ok(Response::RepoForgotten { was_local })
}

/// Tombstones a paired peer; the peer set disconnects it immediately and
/// the gossip propagates the removal.
fn remove_peer(ctx: &ControlContext, peer: &str) -> Result<Response> {
    let endpoint = ctx.update_state(|state| state.remove_peer(peer))?;
    info!(peer = %peer, "peer removed");
    Ok(Response::PeerRemoved(endpoint))
}

/// Registers a paired peer in the mesh state; a no-op when the endpoint is
/// already alive (idempotent re-pair). The peer set starts connecting and
/// the gossip introduces the peer to the rest of the mesh as part of the
/// update.
fn persist_peer(ctx: &ControlContext, peer: &pair::PairedPeer) -> Result<()> {
    ctx.update_state(|state| {
        if state.peer_name(&peer.endpoint).is_some() {
            return Ok(());
        }
        state.add_peer(peer.endpoint, peer.name.clone())
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

//! Server side of the control socket: the listener, the per-connection
//! request handlers, and the daemon context they act on. The clone
//! handler, which streams progress, lives in [`super::clone`].

use std::{
    fs::{self, File, TryLockError},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, eyre};
use iroh::Endpoint;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _},
    net::{UnixListener, UnixStream},
};
use tracing::{debug, info, warn};

use super::{
    clone,
    protocol::{
        BUILD, CLIENT_TIMEOUT, ConflictStatus, MAX_MESSAGE_SIZE, PeerReport, Request, Response,
        Status, build_path,
    },
};
use crate::{
    config::{ConfigDir, Repo, RepoId},
    daemon::{
        backoff::Backoff, hub::SyncHub, pairing::Pairing, peers::PeerSet, repos::RepoSet,
        store::MeshStore,
    },
    net::{
        pair,
        wire::{read_message, write_message},
    },
    repo::JjRepo,
};

/// Time budget for a whole pairing exchange, from dialing to completion.
const PAIRING_TIMEOUT: Duration = Duration::from_mins(1);

/// Initial retry delay after an accept error, escalating to the max.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Everything the control handlers need from the daemon.
#[derive(Debug)]
pub struct ControlContext {
    pub endpoint: Endpoint,
    pub started: SystemTime,
    pub peers: Arc<PeerSet>,
    pub repos: Arc<RepoSet>,
    pub hub: Arc<SyncHub>,
    pub store: Arc<MeshStore>,
    pub pairing: Arc<Pairing>,
    /// The jj version found on PATH at daemon start, for status warnings.
    pub jj_version: Option<String>,
}

impl ControlContext {
    /// Snapshots the daemon state.
    fn status(&self) -> Status {
        let state = self.store.snapshot();
        let peers = self.peers.statuses();
        // Peer-related entries are resolved to (and filtered by) the
        // paired name: hub state recorded for a since-unpaired peer can no
        // longer be acted on, and the endpoint is not ours to show.
        let peer_name = |peer: &iroh::EndpointId| {
            peers
                .iter()
                .find(|status| &status.endpoint == peer)
                .map(|status| status.name.clone())
        };
        let conflicts = self
            .hub
            .conflicts()
            .into_iter()
            .filter_map(|(repo, peer)| {
                Some(ConflictStatus {
                    repo,
                    peer: peer_name(&peer)?,
                })
            })
            .collect();
        let peer_reports = self
            .hub
            .peer_reports()
            .into_iter()
            .filter_map(|(peer, report)| {
                Some(PeerReport {
                    peer: peer_name(&peer)?,
                    report,
                })
            })
            .collect();
        // Mesh repos not registered here are clonable.
        let available = state
            .mesh_repo_names()
            .filter(|name| !state.repos.contains_key(*name))
            .map(str::to_owned)
            .collect();

        Status {
            name: state.machine.name,
            endpoint: self.endpoint.secret_key().public(),
            uptime_secs: self.started.elapsed().unwrap_or_default().as_secs(),
            jj_version: self.jj_version.clone(),
            peers,
            repos: self.repos.statuses(),
            available,
            conflicts,
            peer_reports,
        }
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
        // Published before the socket: a client that can connect always
        // finds the build of the daemon it reached.
        let build = build_path(&path);
        fs::write(&build, BUILD).wrap_err_with(|| format!("cannot write {}", build.display()))?;
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
        let _ = fs::remove_file(build_path(&self.path));
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

    // PairJoin and CloneRepo own their stream (client-cancel handling, and
    // progress streaming for the clone); the rest return a single response
    // and share one error conversion.
    let served = match request {
        Request::Status => reply(&mut stream, Ok(Response::Status(ctx.status()))).await,
        Request::PairHost => reply(&mut stream, pair_host(&ctx).await).await,
        Request::PairJoin { ticket } => pair_join(&mut stream, &ctx, &ticket).await,
        Request::CloneRepo { name, path } => {
            clone::clone_repo(&mut stream, &ctx, &name, &path).await
        }
        Request::AddRepo { name, path } => {
            reply(&mut stream, add_repo(&ctx, name, path).await).await
        }
        Request::RemoveRepo { name } => reply(&mut stream, remove_repo(&ctx, &name)).await,
        Request::RemovePeer { peer } => reply(&mut stream, remove_peer(&ctx, &peer)).await,
        Request::ForgetRepo { name } => reply(&mut stream, forget_repo(&ctx, &name)).await,
        Request::RenameMachine { name } => reply(&mut stream, rename_machine(&ctx, &name)).await,
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
pub(super) async fn respond(
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

/// Hosts a pairing: issues a fresh one-time ticket, revoking any
/// outstanding one. The exchange itself runs in the daemon once the other
/// machine redeems the ticket.
async fn pair_host(ctx: &ControlContext) -> Result<Response> {
    let ticket = ctx.pairing.host().await?;
    Ok(Response::PairTicket(ticket.to_string()))
}

/// Joins a pairing hosted by another machine. Aborts the exchange when the
/// client disconnects, so a cancelled join cannot pair behind the user's
/// back.
async fn pair_join(stream: &mut UnixStream, ctx: &ControlContext, ticket: &str) -> Result<()> {
    let (mut read_half, mut write_half) = stream.split();

    let exchange = async {
        let ticket: pair::PairTicket = ticket.parse()?;
        pair::join(&ctx.endpoint, &ticket, &ctx.store.snapshot()).await
    };
    let result = tokio::select! {
        result = tokio::time::timeout(PAIRING_TIMEOUT, exchange) => result.unwrap_or_else(|_| {
            Err(eyre!("pairing timed out after {}s", PAIRING_TIMEOUT.as_secs()))
        }),
        () = client_gone(&mut read_half) => {
            info!("pairing cancelled by the client");
            return Ok(());
        }
    };

    let persisted = result.and_then(|peer| {
        ctx.store.add_paired_peer(&peer)?;
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

/// Registers the repo at `path` under `name` with a fresh id. The path is
/// validated to be a mesh-compatible repo here, not just in the CLI: the
/// daemon is the authority, and registering an invalid path would only
/// surface later as a watch failure. Storing the discovered workspace root
/// (canonicalized) also keeps two spellings of one repo from registering
/// twice.
async fn add_repo(ctx: &ControlContext, name: String, path: PathBuf) -> Result<Response> {
    let root = tokio::task::spawn_blocking(move || -> Result<PathBuf> {
        Ok(JjRepo::discover(&path)?.root().to_owned())
    })
    .await
    .wrap_err("repo validation task failed")??;

    ctx.store.update(|state| {
        state.add_repo(
            name,
            Repo {
                id: RepoId::generate(),
                path: root,
            },
        )
    })?;
    Ok(Response::RepoAdded)
}

/// Retires a repo name from the mesh; the repo set stops watching it here
/// and the gossip propagates the removal to the other machines.
fn remove_repo(ctx: &ControlContext, name: &str) -> Result<Response> {
    let was_local = ctx.store.update(|state| state.remove_repo(name))?;
    info!(repo = %name, "repo removed from the mesh");
    Ok(Response::RepoRemoved { was_local })
}

/// Unregisters a repo on this machine only: the repo set stops watching
/// it, announcements stop, and the mesh record stays untouched (no gossip
/// change), so the repo remains clonable here.
fn forget_repo(ctx: &ControlContext, name: &str) -> Result<Response> {
    let repo = ctx.store.update(|state| state.forget_repo(name))?;
    info!(repo = %name, "repo forgotten locally");
    Ok(Response::RepoForgotten { path: repo.path })
}

/// Tombstones a paired peer; the peer set disconnects it immediately and
/// the gossip propagates the removal.
fn remove_peer(ctx: &ControlContext, peer: &str) -> Result<Response> {
    let endpoint = ctx.store.update(|state| state.remove_peer(peer))?;
    info!(peer = %peer, "peer removed");
    Ok(Response::PeerRemoved(endpoint))
}

/// Renames this machine; the store gossips the change.
fn rename_machine(ctx: &ControlContext, name: &str) -> Result<Response> {
    ctx.store
        .update(|state| state.rename_machine(name.to_owned()))?;
    info!(name = %name, "machine renamed");
    Ok(Response::MachineRenamed)
}

/// Resolves when the client closes its end of the connection.
pub(super) async fn client_gone(read: &mut (impl AsyncRead + Unpin)) {
    let mut buf = [0u8; 64];
    loop {
        match read.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            // Ignore stray bytes: requests are one-per-connection.
            Ok(_) => {}
        }
    }
}

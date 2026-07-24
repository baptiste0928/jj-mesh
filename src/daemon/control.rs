//! Daemon control socket.
//!
//! The CLI talks to the running daemon over a unix socket, with one
//! length-prefixed postcard request/response exchange per connection (see
//! [`crate::net::wire`]). The daemon is the only holder of live peer state:
//! the CLI cannot bind the machine-key endpoint while the daemon runs, so
//! reachability is only observable from here.

use std::{
    fs::{self, File, TryLockError},
    io,
    path::PathBuf,
    time::Duration,
};

use color_eyre::eyre::{Result, WrapErr as _, bail, eyre};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, warn};

use crate::{
    config::ConfigDir,
    net::wire::{read_message, write_message},
};

/// Maximum accepted size of a control message.
const MAX_MESSAGE_SIZE: u32 = 1 << 20;

/// Time budget for a whole client exchange with the daemon.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

/// Initial retry delay after an accept error, escalating to the max.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const ACCEPT_ERROR_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// A request from the CLI to the daemon.
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    /// Report the daemon state; answered with [`Response::Status`].
    Status,
}

/// A daemon answer to a [`Request`].
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Status(Status),
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
                return Err(err)
                    .wrap_err_with(|| format!("cannot lock {}", lock_path.display()));
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

    /// Serves control requests forever; `status` snapshots the daemon state.
    pub async fn serve(self, status: impl Fn() -> Status + Clone + Send + Sync + 'static) -> ! {
        let mut error_backoff = ACCEPT_ERROR_BACKOFF;

        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    error_backoff = ACCEPT_ERROR_BACKOFF;
                    tokio::spawn(handle_client(stream, status.clone()));
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
async fn handle_client(mut stream: UnixStream, status: impl Fn() -> Status) {
    let exchange = async {
        match read_message(&mut stream, MAX_MESSAGE_SIZE).await? {
            Request::Status => {
                write_message(&mut stream, &Response::Status(status()), MAX_MESSAGE_SIZE).await
            }
        }
    };

    match tokio::time::timeout(CLIENT_TIMEOUT, exchange).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => debug!("control client error: {err}"),
        Err(_) => debug!("control client timed out"),
    }
}

/// Queries the status of the daemon serving this configuration.
///
/// Returns `None` when no daemon is running; errors when a daemon is
/// listening but does not answer properly.
pub async fn query_status(dir: &ConfigDir) -> Result<Option<Status>> {
    let path = dir.socket_path();

    let mut stream = match UnixStream::connect(&path).await {
        Ok(stream) => stream,
        Err(err)
            if matches!(
                err.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(err) => {
            return Err(err).wrap_err_with(|| format!("cannot connect to {}", path.display()));
        }
    };

    let exchange = async {
        write_message(&mut stream, &Request::Status, MAX_MESSAGE_SIZE).await?;
        read_message(&mut stream, MAX_MESSAGE_SIZE).await
    };
    let response = tokio::time::timeout(CLIENT_TIMEOUT, exchange)
        .await
        .map_err(|_| eyre!("the daemon on {} did not answer", path.display()))?
        .wrap_err("cannot query the daemon")?;

    let Response::Status(status) = response;
    Ok(Some(status))
}

/// Blocking convenience over [`query_status`] for CLI commands that have no
/// tokio runtime of their own.
pub fn query_status_blocking(dir: &ConfigDir) -> Result<Option<Status>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(query_status(dir))
}

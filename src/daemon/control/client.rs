//! Client side of the control socket: connecting to the daemon and the
//! blocking helpers CLI commands use, which have no tokio runtime of their
//! own.

use std::{future::Future, io, time::Duration};

use color_eyre::eyre::{Report, Result, WrapErr as _, bail, eyre};
use tokio::net::UnixStream;

use super::protocol::{CLIENT_TIMEOUT, CloneProgress, MAX_MESSAGE_SIZE, Request, Response, Status};
use crate::{
    config::ConfigDir,
    net::wire::{read_message, write_message},
};

/// Error of every command that needs the daemon when none is running.
///
/// The CLI entry point recognizes this type and reports it as a plain
/// message rather than an error report: not having started the daemon yet
/// is an expected situation, not a failure to debug.
#[derive(Debug)]
pub struct DaemonNotRunning;

impl std::fmt::Display for DaemonNotRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The jj-mesh daemon is not running. Install it with `jj-mesh service install`."
        )
    }
}

impl std::error::Error for DaemonNotRunning {}

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

    /// Connects to the daemon serving this configuration; errors with
    /// [`DaemonNotRunning`] when none is. Every command that needs the
    /// daemon connects through here, so they all fail the same way.
    pub async fn connect_required(dir: &ConfigDir) -> Result<Self> {
        Self::connect(dir)
            .await?
            .ok_or_else(|| Report::new(DaemonNotRunning))
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

/// Runs a control-socket future on a fresh current-thread runtime, for CLI
/// commands that have no tokio runtime of their own.
pub fn block_on<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(future)
}

/// Queries the status of the daemon serving this configuration. Errors when
/// no daemon is running, or when one is listening but does not answer
/// properly. Blocking.
pub fn query_status_blocking(dir: &ConfigDir) -> Result<Status> {
    block_on(async {
        let mut client = ControlClient::connect_required(dir).await?;

        client.send(&Request::Status).await?;
        match client.recv(Some(CLIENT_TIMEOUT)).await? {
            Response::Status(status) => Ok(status),
            other => bail!("unexpected response from the daemon: {other:?}"),
        }
    })
}

/// Checks that a daemon is running, erroring with [`DaemonNotRunning`]
/// otherwise: for commands that want to fail fast before doing local work
/// they would otherwise have to undo.
pub fn ensure_daemon_blocking(dir: &ConfigDir) -> Result<()> {
    block_on(ControlClient::connect_required(dir)).map(drop)
}

/// Sends one request and returns the daemon's answer, for CLI commands with
/// no tokio runtime of their own. Errors when no daemon is running, and
/// turns [`Response::Error`] into an error, so callers only match their
/// success variant. Progress frames, if any, are dropped.
pub fn request_blocking(dir: &ConfigDir, request: &Request, limit: Duration) -> Result<Response> {
    request_streaming_blocking(dir, request, limit, |_| {})
}

/// Like [`request_blocking`], for requests answered by a progress stream:
/// `on_progress` sees every [`Response::CloneProgress`] frame, and the first
/// terminal response is returned. `idle` bounds the gap between frames,
/// not the whole exchange; the daemon heartbeats progress while it works.
pub fn request_streaming_blocking(
    dir: &ConfigDir,
    request: &Request,
    idle: Duration,
    mut on_progress: impl FnMut(CloneProgress),
) -> Result<Response> {
    block_on(async {
        let mut client = ControlClient::connect_required(dir).await?;

        client.send(request).await?;
        loop {
            match client.recv(Some(idle)).await? {
                Response::CloneProgress(progress) => on_progress(progress),
                Response::Error(message) => bail!("{message}"),
                response => return Ok(response),
            }
        }
    })
}

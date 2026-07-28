//! Client side of the control socket: connecting to the daemon and the
//! blocking helpers CLI commands use, which have no tokio runtime of their
//! own.

use std::{io, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _, bail, eyre};
use tokio::net::UnixStream;

use super::protocol::{CLIENT_TIMEOUT, JoinProgress, MAX_MESSAGE_SIZE, Request, Response, Status};
use crate::{
    config::ConfigDir,
    net::wire::{read_message, write_message},
};

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
/// success variant. Progress frames, if any, are dropped.
pub fn request_blocking(dir: &ConfigDir, request: &Request, limit: Duration) -> Result<Response> {
    request_streaming_blocking(dir, request, limit, |_| {})
}

/// Like [`request_blocking`], for requests answered by a progress stream:
/// `on_progress` sees every [`Response::JoinProgress`] frame, and the first
/// terminal response is returned. `idle` bounds the gap between frames,
/// not the whole exchange; the daemon heartbeats progress while it works.
pub fn request_streaming_blocking(
    dir: &ConfigDir,
    request: &Request,
    idle: Duration,
    mut on_progress: impl FnMut(JoinProgress),
) -> Result<Response> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let Some(mut client) = ControlClient::connect(dir).await? else {
                bail!("the jj-mesh daemon is not running; start it with `jj-mesh daemon` first");
            };

            client.send(request).await?;
            loop {
                match client.recv(Some(idle)).await? {
                    Response::JoinProgress(progress) => on_progress(progress),
                    Response::Error(message) => bail!("{message}"),
                    response => return Ok(response),
                }
            }
        })
}

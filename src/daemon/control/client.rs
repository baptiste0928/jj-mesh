//! Client side of the control socket: connecting to the daemon and the
//! blocking helpers CLI commands use, which have no tokio runtime of their
//! own.

use std::{future::Future, io, time::Duration};

use color_eyre::eyre::{Report, Result, WrapErr as _, bail, eyre};
use tokio::net::UnixStream;

use super::protocol::{
    BUILD, CLIENT_TIMEOUT, CloneProgress, MAX_BUILD_SIZE, MAX_MESSAGE_SIZE, Request, Response,
    Status,
};
use crate::{
    config::ConfigDir,
    net::wire::{read_message, write_message},
};

/// Hint for a daemon of another build.
const RESTART_HINT: &str = "restart it with `jj-mesh service restart`";

/// Environment variable that, when set to a non-empty value, lets the CLI
/// talk to a daemon of another build, for testing builds known to be
/// compatible.
const IGNORE_BUILD_VAR: &str = "JJ_MESH_IGNORE_BUILD";

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
            "The jj-mesh daemon is not running. Set it up with `jj-mesh setup`."
        )
    }
}

impl std::error::Error for DaemonNotRunning {}

/// Whether two builds are known to differ: builds without a known commit
/// cannot be compared.
fn builds_differ(daemon: &str, cli: &str) -> bool {
    let known = |build: &str| !build.is_empty() && !build.starts_with("unknown");
    known(daemon) && known(cli) && daemon != cli
}

/// Client side of the control socket.
#[derive(Debug)]
pub struct ControlClient {
    stream: UnixStream,
}

impl ControlClient {
    /// Connects to the daemon serving this configuration, or `None` when no
    /// daemon is running. Errors when the daemon is another build than
    /// this CLI: the exchange would likely fail to decode.
    pub async fn connect(dir: &ConfigDir) -> Result<Option<Self>> {
        let path = dir.socket_path();

        let mut stream = match UnixStream::connect(path).await {
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
        let greeting = read_message::<String>(&mut stream, MAX_BUILD_SIZE);
        let build = tokio::time::timeout(CLIENT_TIMEOUT, greeting)
            .await
            .ok()
            .and_then(Result::ok)
            .ok_or_else(|| {
                eyre!("the daemon did not send its build, it likely runs an older jj-mesh: {RESTART_HINT}")
            })?;
        let ignore = std::env::var_os(IGNORE_BUILD_VAR).is_some_and(|value| !value.is_empty());
        if !ignore && builds_differ(&build, BUILD) {
            bail!(
                "the daemon runs jj-mesh build {build} while this command is build {BUILD}: \
                 {RESTART_HINT}"
            );
        }
        Ok(Some(ControlClient { stream }))
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

    /// Receives the next response, bounded by `limit` when given. A
    /// response this build cannot decode most likely comes from a daemon
    /// of another build, and says so.
    pub async fn recv(&mut self, limit: Option<Duration>) -> Result<Response> {
        let read = read_message(&mut self.stream, MAX_MESSAGE_SIZE);
        let response = match limit {
            Some(limit) => tokio::time::timeout(limit, read)
                .await
                .map_err(|_| eyre!("the daemon did not answer"))?,
            None => read.await,
        };
        response.wrap_err_with(|| {
            format!("cannot read the daemon's answer: if it runs another build, {RESTART_HINT}")
        })
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

#[cfg(test)]
mod tests {
    use tokio::{net::UnixListener, sync::watch};

    use super::*;
    use crate::daemon::control::server::ControlServer;

    /// A config dir with a fake daemon socket.
    fn fake_daemon() -> (tempfile::TempDir, ConfigDir, UnixListener) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        let listener = UnixListener::bind(dir.socket_path()).unwrap();
        (tmp, dir, listener)
    }

    /// A CLI reaching a daemon of another build is told to restart it,
    /// before any exchange that would fail to decode.
    #[tokio::test]
    async fn refuses_daemon_of_another_build() {
        if BUILD.starts_with("unknown") {
            return; // Built without a commit: nothing to compare.
        }
        if std::env::var_os(IGNORE_BUILD_VAR).is_some_and(|value| !value.is_empty()) {
            return; // The check is disabled for this shell.
        }
        let (_tmp, dir, listener) = fake_daemon();
        let daemon = tokio::spawn(async move {
            for build in ["0000dead", BUILD] {
                let (mut stream, _) = listener.accept().await.unwrap();
                write_message(&mut stream, &build, MAX_BUILD_SIZE)
                    .await
                    .unwrap();
            }
        });

        let err = ControlClient::connect(&dir).await.unwrap_err();
        assert!(err.to_string().contains("0000dead"), "{err}");
        assert!(err.to_string().contains("service restart"), "{err}");
        assert!(ControlClient::connect(&dir).await.unwrap().is_some());
        daemon.await.unwrap();
    }

    /// A starting daemon greets clients before it can serve requests.
    #[tokio::test]
    async fn connects_to_starting_daemon() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        let server = ControlServer::bind(&dir).unwrap();
        let (_ctx, ctx_rx) = watch::channel(None);
        let daemon = tokio::spawn(server.serve(ctx_rx));

        assert!(ControlClient::connect(&dir).await.unwrap().is_some());
        daemon.abort();
    }

    /// Daemons predating the greeting send nothing first.
    #[tokio::test]
    async fn refuses_daemon_without_greeting() {
        let (_tmp, dir, listener) = fake_daemon();
        let daemon = tokio::spawn(async move { listener.accept().await.unwrap() });

        let err = ControlClient::connect(&dir).await.unwrap_err();
        assert!(err.to_string().contains("service restart"), "{err}");
        drop(daemon.await.unwrap());
    }
}

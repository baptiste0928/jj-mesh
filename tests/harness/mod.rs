//! In-process multi-daemon harness for end-to-end tests.
//!
//! A [`TestMesh`] hosts any number of [`Machine`]s, each running a real
//! daemon on its own tempdir configuration; peers resolve each other
//! in-memory without any relay. Machines drive their daemon through the
//! control socket exactly as the CLI would, and the free functions cover
//! the recurring flows: pairing, sharing a repo across two machines, and
//! waiting for op logs to converge.

// Each test binary compiles this module separately and uses its own
// subset of the helpers.
#![allow(dead_code)]

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use iroh::address_lookup::MemoryLookup;
use jj_mesh::{
    config::{ConfigDir, MeshState},
    daemon::{
        Daemon,
        control::{ConnectionStatus, ControlClient, Request, Response, Status},
    },
    net::EndpointOptions,
    repo::JjRepo,
    testing::Fixture,
};

/// How long a test waits for an expected state before failing.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(2);

/// A hermetic mesh: the shared address lookup and a jj scratch space.
pub struct TestMesh {
    lookup: MemoryLookup,
    /// Drives the real `jj` binary against repos under its scratch dir.
    pub jj: Fixture,
}

impl TestMesh {
    pub fn new() -> Self {
        // Make daemon logs available to failing tests via RUST_LOG.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        TestMesh {
            lookup: MemoryLookup::new(),
            jj: Fixture::new(),
        }
    }

    /// Starts a fresh machine: a tempdir config with a running daemon.
    pub async fn machine(&self, name: &str) -> Machine {
        let mut machine = Machine {
            name: name.to_owned(),
            config: tempfile::tempdir().unwrap(),
            lookup: self.lookup.clone(),
            daemon: None,
        };
        machine.start().await;
        machine.rename(name).await;
        machine
    }

    /// Starts "machine-a" and "machine-b", paired and connected.
    pub async fn connected_pair(&self) -> (Machine, Machine) {
        let a = self.machine("machine-a").await;
        let b = self.machine("machine-b").await;
        connect(&a, &b).await;
        (a, b)
    }
}

/// Polls `step` every 100ms until it succeeds, panicking with the last
/// error once [`WAIT_TIMEOUT`] has elapsed.
async fn poll<T>(mut step: impl AsyncFnMut() -> Result<T, String>) -> T {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match step().await {
            Ok(value) => return value,
            Err(why) => assert!(tokio::time::Instant::now() < deadline, "{why}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One mesh machine: a config dir (which persists its identity across
/// restarts) and the daemon running on it.
pub struct Machine {
    pub name: String,
    config: tempfile::TempDir,
    lookup: MemoryLookup,
    daemon: Option<Daemon>,
}

impl Machine {
    fn config_dir(&self) -> ConfigDir {
        ConfigDir::new(Some(self.config.path().to_owned())).unwrap()
    }

    /// Starts the daemon; the machine keeps its identity and mesh state.
    pub async fn start(&mut self) {
        assert!(
            self.daemon.is_none(),
            "{}: daemon already running",
            self.name
        );
        let options = EndpointOptions::LocalTest {
            lookup: self.lookup.clone(),
        };
        let daemon = Daemon::start(&self.config_dir(), &options)
            .await
            .unwrap_or_else(|err| panic!("{}: daemon failed to start: {err:#}", self.name));
        self.daemon = Some(daemon);
    }

    /// Edits the stored mesh state directly, as a user (or a bug) could.
    /// The daemon must be stopped: it owns the file while running.
    pub fn edit_state(&self, edit: impl FnOnce(&mut MeshState)) {
        assert!(self.daemon.is_none(), "{}: daemon running", self.name);
        let dir = self.config_dir();
        let mut state = MeshState::load(&dir).unwrap();
        edit(&mut state);
        state.save(&dir).unwrap();
    }

    /// Stops the daemon, releasing its control socket.
    pub async fn stop(&mut self) {
        self.daemon
            .take()
            .unwrap_or_else(|| panic!("{}: daemon not running", self.name))
            .shutdown()
            .await;
    }

    /// Connects a control client, as the CLI would.
    async fn client(&self) -> ControlClient {
        ControlClient::connect(&self.config_dir())
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{}: daemon not reachable", self.name))
    }

    /// One request/response exchange, returning daemon-level errors.
    pub async fn try_request(&self, request: &Request) -> Response {
        let mut client = self.client().await;
        client.send(request).await.unwrap();
        client.recv(Some(WAIT_TIMEOUT)).await.unwrap()
    }

    /// One request/terminal-response exchange for requests answered by a
    /// progress stream: skips every progress frame, as the CLI's streaming
    /// helper does. [`WAIT_TIMEOUT`] bounds each frame gap.
    async fn try_streaming_request(&self, request: &Request) -> Response {
        let mut client = self.client().await;
        client.send(request).await.unwrap();
        loop {
            match client.recv(Some(WAIT_TIMEOUT)).await.unwrap() {
                Response::CloneProgress(_) => {}
                response => return response,
            }
        }
    }

    /// One request/response exchange, panicking on a daemon error.
    pub async fn request(&self, request: &Request) -> Response {
        match self.try_request(request).await {
            Response::Error(message) => {
                panic!("{}: {request:?} failed: {message}", self.name)
            }
            response => response,
        }
    }

    /// Snapshots the daemon status.
    pub async fn status(&self) -> Status {
        match self.request(&Request::Status).await {
            Response::Status(status) => status,
            other => panic!("{}: unexpected response {other:?}", self.name),
        }
    }

    /// Polls the daemon status until `pred` holds, panicking after
    /// [`WAIT_TIMEOUT`].
    pub async fn wait(&self, what: &str, pred: impl Fn(&Status) -> bool) {
        poll(async || {
            if pred(&self.status().await) {
                Ok(())
            } else {
                Err(format!("{}: timed out waiting for {what}", self.name))
            }
        })
        .await;
    }

    /// Renames this machine, as `jj-mesh peer rename` would.
    pub async fn rename(&self, name: &str) {
        let response = self
            .request(&Request::RenameMachine {
                name: name.to_owned(),
            })
            .await;
        assert!(matches!(response, Response::MachineRenamed), "{response:?}");
    }

    /// Issues a pairing ticket on this machine, as `jj-mesh peer add` would.
    pub async fn host_pairing(&self) -> String {
        match self.request(&Request::PairHost).await {
            Response::PairTicket(ticket) => ticket,
            other => panic!("{}: expected a pairing ticket, got {other:?}", self.name),
        }
    }

    /// Attempts to join a pairing with `ticket` and returns the daemon's
    /// verdict.
    pub async fn try_join_pairing(&self, ticket: String) -> Response {
        self.try_request(&Request::PairJoin { ticket }).await
    }

    /// Joins a pairing with `ticket`. The host persists the peer before
    /// confirming, so the `Paired` answer asserted here means both sides
    /// are registered.
    pub async fn join_pairing(&self, ticket: String) {
        let joined = self.try_join_pairing(ticket).await;
        assert!(
            matches!(joined, Response::Paired { .. }),
            "{}: {joined:?}",
            self.name,
        );
    }

    /// Pairs this machine (hosting) with `other` (joining), through both
    /// control sockets, as `jj-mesh peer add` would. Each side announces its
    /// own name and stores the peer under the peer's announced one.
    pub async fn pair_with(&self, other: &Machine) {
        let ticket = self.host_pairing().await;
        other.join_pairing(ticket).await;
    }

    /// Registers the repo at `path` under `name`, as `jj-mesh repo add` would.
    pub async fn add_repo(&self, name: &str, path: &Path) {
        let added = self
            .request(&Request::AddRepo {
                name: name.to_owned(),
                path: std::fs::canonicalize(path).unwrap(),
            })
            .await;
        assert!(matches!(added, Response::RepoAdded), "{added:?}");
    }

    /// Waits until the peer named `peer` has an established connection.
    pub async fn wait_peer_connected(&self, peer: &str) {
        self.wait(&format!("{peer} connected"), |s| {
            s.peers.iter().any(|p| {
                p.name == peer && matches!(p.connection, ConnectionStatus::Connected { .. })
            })
        })
        .await;
    }

    /// Waits until the mesh repo `name` is clonable on this machine.
    pub async fn wait_available(&self, name: &str) {
        self.wait(&format!("{name} available"), |s| {
            s.available.iter().any(|repo| repo == name)
        })
        .await;
    }

    /// Clones the mesh repo `name` into `path` (which must be a freshly
    /// initialized repo, see [`Fixture::init_pull_target`]), retrying while the
    /// daemon still lacks a usable source announcement.
    pub async fn clone_repo(&self, name: &str, path: &Path) {
        let request = Request::CloneRepo {
            name: name.to_owned(),
            path: std::fs::canonicalize(path).unwrap(),
        };
        poll(async || match self.try_streaming_request(&request).await {
            Response::Cloned { .. } => Ok(()),
            Response::Error(message) => Err(format!(
                "{}: clone of `{name}` kept failing: {message}",
                self.name,
            )),
            other => panic!("{}: unexpected response {other:?}", self.name),
        })
        .await;
    }
}

/// Pairs `a` (hosting) with `b` (joining) and waits until the persistent
/// connection is established in both directions.
pub async fn connect(a: &Machine, b: &Machine) {
    a.pair_with(b).await;
    a.wait_peer_connected(&b.name).await;
    b.wait_peer_connected(&a.name).await;
}

/// Creates a repo named `name`, registers it on `on`, clones it from `from`
/// into a fresh repo, and waits until both are in sync. Returns the original
/// and the cloned repo's paths.
pub async fn add_and_clone(
    mesh: &TestMesh,
    on: &Machine,
    from: &Machine,
    name: &str,
) -> (PathBuf, PathBuf) {
    let dir = mesh.jj.init_repo(name);
    on.add_repo(name, &dir).await;
    from.wait_available(name).await;

    let cloned = mesh
        .jj
        .init_pull_target(&format!("{name}-on-{}", from.name), &from.name);
    from.clone_repo(name, &cloned).await;
    // Any jj command merges the fresh workspace into the pulled history;
    // its operations then flow back until both sides agree.
    mesh.jj.jj(&cloned, &["status"]);
    wait_converged(&dir, &cloned).await;
    (dir, cloned)
}

/// The descriptions of every commit in the repo at `dir`, one per line.
pub fn descriptions(mesh: &TestMesh, dir: &Path) -> String {
    mesh.jj.jj_output(
        dir,
        &["log", "-r", "all()", "--no-graph", "-T", "description"],
    )
}

/// Waits until both repos report the same jj op heads, i.e. their
/// operation logs converged.
pub async fn wait_converged(a: &Path, b: &Path) {
    poll(async || {
        let (heads_a, heads_b) = (op_heads(a).await, op_heads(b).await);
        if heads_a == heads_b {
            Ok(())
        } else {
            Err(format!(
                "repos did not converge: {} has {heads_a:?}, {} has {heads_b:?}",
                a.display(),
                b.display(),
            ))
        }
    })
    .await;
}

/// Reads a repo's sorted op heads (as their debug form, which is enough
/// for equality checks).
pub async fn op_heads(path: &Path) -> Vec<String> {
    let repo = JjRepo::discover(path).unwrap().open().unwrap();
    let mut heads: Vec<String> = repo
        .op_heads()
        .await
        .unwrap()
        .iter()
        .map(|id| format!("{id:?}"))
        .collect();
    heads.sort_unstable();
    heads
}

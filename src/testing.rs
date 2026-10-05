//! Test fixtures: runs the `jj` binary against temporary repos, syncs two
//! local repos over in-memory streams, and queries git.
//!
//! Available to this crate's unit tests and, through the `test-util`
//! feature, to the integration tests.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use color_eyre::eyre::Result;
use jj_lib::{object_id::ObjectId as _, op_store::OperationId};
use tokio::io::{AsyncRead, AsyncWrite};

pub use crate::{net::fetch::GitTransferFormat, repo::transfer::FetchOutcome};
use crate::{
    net::{
        fetch::{FetchRequest, MAX_OP_FRAME_SIZE},
        wire::read_message,
    },
    repo::{
        JjRepo, OpenRepo,
        transfer::{FetchOptions, ProgressSink, RepoIdent, fetch, mirror, serve},
    },
};

/// Network deadline of test fetches, large enough to never fire.
const NET_TIMEOUT: Duration = Duration::from_mins(1);

/// A tempdir with a hermetic jj setup: empty jj and git configs, identity
/// from env.
pub struct Fixture {
    tmp: tempfile::TempDir,
    config: PathBuf,
}

impl Fixture {
    pub fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let config = tmp.path().join("jj-config.toml");
        fs::write(&config, "").unwrap();
        Fixture { tmp, config }
    }

    /// Replaces the user-level jj config.
    pub fn set_user_config(&self, text: &str) {
        fs::write(&self.config, text).unwrap();
    }

    /// The fixture's scratch directory.
    pub fn path(&self) -> &Path {
        self.tmp.path()
    }

    /// Runs a jj command in `dir`, panicking on failure.
    pub fn jj(&self, dir: &Path, args: &[&str]) {
        self.jj_output(dir, args);
    }

    /// Runs a jj command in `dir`, panicking on failure and returning its
    /// stdout.
    pub fn jj_output(&self, dir: &Path, args: &[&str]) -> String {
        let out = self
            .jj_command(dir, args)
            .output()
            .expect("jj must be installed to run these tests");
        assert!(
            out.status.success(),
            "jj {args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Runs a jj command in `dir`, returning whether it succeeded.
    pub fn jj_ok(&self, dir: &Path, args: &[&str]) -> bool {
        self.jj_command(dir, args)
            .output()
            .is_ok_and(|out| out.status.success())
    }

    /// The hermetic jj invocation shared by all runners.
    fn jj_command(&self, dir: &Path, args: &[&str]) -> Command {
        let mut cmd = Command::new(crate::repo::jj_bin());
        Self::git_env(&mut cmd)
            .current_dir(dir)
            .env("JJ_CONFIG", &self.config)
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test@example.com")
            .env("JJ_OP_HOSTNAME", "test-host")
            .env("JJ_OP_USERNAME", "test-user")
            .args(args);
        cmd
    }

    pub fn git_env(cmd: &mut Command) -> &mut Command {
        cmd.env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Test User")
            .env("GIT_AUTHOR_EMAIL", "test@example.com")
            .env("GIT_COMMITTER_NAME", "Test User")
            .env("GIT_COMMITTER_EMAIL", "test@example.com")
    }

    /// Creates a jj repo named `name` with an initial described commit.
    pub fn init_repo(&self, name: &str) -> PathBuf {
        let dir = self.tmp.path().join(name);
        self.jj(self.tmp.path(), &["git", "init", name]);
        self.jj(&dir, &["describe", "-m", "base"]);
        dir
    }

    /// Initializes a fresh non-colocated repo named `name` with a
    /// machine-unique workspace name, ready to receive a pull.
    pub fn init_pull_target(&self, name: &str, workspace: &str) -> PathBuf {
        self.init_target(name, workspace, "--no-colocate")
    }

    /// [`Self::init_pull_target`], colocated.
    pub fn init_colocated_pull_target(&self, name: &str, workspace: &str) -> PathBuf {
        self.init_target(name, workspace, "--colocate")
    }

    fn init_target(&self, name: &str, workspace: &str, colocate: &str) -> PathBuf {
        let dir = self.tmp.path().join(name);
        self.jj(self.tmp.path(), &["git", "init", colocate, name]);
        self.jj(&dir, &["workspace", "rename", workspace]);
        dir
    }

    /// Writes `message` into `file` and commits it under the same message
    /// in the repo at `dir`.
    pub fn commit_file(&self, dir: &Path, file: &str, message: &str) {
        fs::write(dir.join(file), format!("{message}\n")).unwrap();
        self.jj(dir, &["commit", "-m", message]);
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Fixture::new()
    }
}

pub fn open(dir: &Path) -> Arc<OpenRepo> {
    Arc::new(JjRepo::discover(dir).unwrap().open().unwrap())
}

/// Opens a repo like the daemon on watch start: self-check, then heals.
pub async fn open_healed(dir: &Path) -> Arc<OpenRepo> {
    let repo = open(dir);
    repo.self_check().await.unwrap();
    let heads = repo.op_heads().await.unwrap();
    let missing = repo.unindexed(&heads).await;
    let healed = repo.clone();
    crate::spawn_blocking(move || {
        healed.build_commit_indexes(&missing);
        mirror::heal(&healed)
    })
    .await
    .unwrap()
    .unwrap();
    repo
}

/// Copies a repo directory.
pub fn fork(from: &Path, to: &Path) {
    let cp = Command::new("cp")
        .arg("-r")
        .args([from, to])
        .status()
        .unwrap();
    assert!(cp.success());
}

/// Fetches `wants` from `server` into `fetcher` over an in-memory stream pair.
pub async fn sync_once(
    fetcher: &Arc<OpenRepo>,
    server: &Arc<OpenRepo>,
    wants: &[OperationId],
) -> FetchOutcome {
    try_sync(fetcher, server, wants, GitTransferFormat::Loose)
        .await
        .unwrap()
}

/// [`sync_once`] with an explicit git format, returning fetch errors.
pub async fn try_sync(
    fetcher: &Arc<OpenRepo>,
    server: &Arc<OpenRepo>,
    wants: &[OperationId],
    format: GitTransferFormat,
) -> Result<FetchOutcome> {
    let (client, remote) = tokio::io::duplex(1 << 20);
    let (mut client_rx, mut client_tx) = tokio::io::split(client);
    let (mut server_rx, mut server_tx) = tokio::io::split(remote);

    let server = server.clone();
    let serve_task = tokio::spawn(async move {
        let request: FetchRequest = read_message(&mut server_rx, MAX_OP_FRAME_SIZE)
            .await
            .unwrap();
        serve(&server, request, &mut server_tx, &mut server_rx)
            .await
            .unwrap();
    });

    let outcome = fetch_from(
        fetcher,
        wants,
        format,
        &mut client_tx,
        &mut client_rx,
        ProgressSink::default(),
    )
    .await;
    serve_task.await.unwrap();
    outcome
}

/// Runs the fetcher side over `send`/`recv` with a random repo identity.
pub async fn fetch_from(
    fetcher: &Arc<OpenRepo>,
    wants: &[OperationId],
    format: GitTransferFormat,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<FetchOutcome> {
    fetch(
        fetcher,
        RepoIdent {
            name: "test",
            id: &crate::config::RepoId::generate(),
        },
        wants,
        FetchOptions {
            format,
            net_timeout: NET_TIMEOUT,
        },
        send,
        recv,
        progress,
    )
    .await
}

/// Fetches the heads `dst` lacks from `src`; returns whether there were any.
pub async fn sync_missing(dst: &Arc<OpenRepo>, src: &Arc<OpenRepo>) -> bool {
    let wants = dst.missing_heads(&src.op_heads().await.unwrap()).unwrap();
    if wants.is_empty() {
        return false;
    }
    sync_once(dst, src, &wants).await;
    true
}

/// Asserts every op head of `repo` has its commit index.
pub async fn assert_heads_indexed(repo: &Arc<OpenRepo>) {
    for head in repo.op_heads().await.unwrap() {
        assert!(
            repo.has_commit_index(&head).await,
            "op head {} published without a commit index",
            head.hex(),
        );
    }
}

/// The backing git repo of a non-colocated jj repo.
pub fn store_git_dir(dir: &Path) -> PathBuf {
    dir.join(".jj/repo/store/git")
}

/// Resolves `rev` in the colocated `.git` of `dir`.
pub fn git_rev(dir: &Path, rev: &str) -> String {
    git_rev_at(&dir.join(".git"), rev)
}

/// Resolves `rev` in the git repo at `git_dir`.
pub fn git_rev_at(git_dir: &Path, rev: &str) -> String {
    git_output(git_dir, &["rev-parse", rev])
}

/// Runs a git command against `git_dir`, panicking on failure.
pub fn git(git_dir: &Path, args: &[&str]) {
    git_output(git_dir, args);
}

/// Runs a git command against `git_dir`, returning its trimmed stdout.
pub fn git_output(git_dir: &Path, args: &[&str]) -> String {
    let out = git_command(git_dir, args).output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?} failed in {}:\n{}",
        git_dir.display(),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}

/// Runs a git command against `git_dir`, returning whether it succeeded.
pub fn git_ok(git_dir: &Path, args: &[&str]) -> bool {
    git_command(git_dir, args)
        .output()
        .is_ok_and(|out| out.status.success())
}

fn git_command(git_dir: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    Fixture::git_env(&mut cmd)
        .arg("--git-dir")
        .arg(git_dir)
        .args(args);
    cmd
}

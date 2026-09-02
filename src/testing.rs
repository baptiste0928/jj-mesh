//! Test fixtures driving the real `jj` binary against temporary repos.
//!
//! Available to this crate's unit tests and, through the `test-util`
//! feature, to the integration tests.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// A tempdir with a hermetic jj setup: empty config, identity from env.
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
        cmd.current_dir(dir)
            .env("JJ_CONFIG", &self.config)
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test@example.com")
            .env("JJ_OP_HOSTNAME", "test-host")
            .env("JJ_OP_USERNAME", "test-user")
            .args(args);
        cmd
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
        let dir = self.tmp.path().join(name);
        self.jj(self.tmp.path(), &["git", "init", "--no-colocate", name]);
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

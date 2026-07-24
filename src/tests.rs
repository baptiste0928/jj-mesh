//! Test fixtures driving the real `jj` binary against temporary repos.

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
        let out = Command::new("jj")
            .current_dir(dir)
            .env("JJ_CONFIG", &self.config)
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test@example.com")
            .env("JJ_OP_HOSTNAME", "test-host")
            .env("JJ_OP_USERNAME", "test-user")
            .args(args)
            .output()
            .expect("jj must be installed to run these tests");
        assert!(
            out.status.success(),
            "jj {args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr),
        );
    }

    /// Creates a jj repo named `name` with an initial described commit.
    pub fn init_repo(&self, name: &str) -> PathBuf {
        let dir = self.tmp.path().join(name);
        self.jj(self.tmp.path(), &["git", "init", name]);
        self.jj(&dir, &["describe", "-m", "base"]);
        dir
    }
}

//! Local jj repository handling.
//!
//! jj's on-disk formats are internal and unstable, so before touching a repo
//! we verify its store `type` files match the backends this build supports
//! (git commit backend, simple op store and op heads store).
//!
//! [`JjRepo`] is a validated repo on disk and [`StoreFingerprint`] its
//! captured store configuration. The `jj` module invokes the user's jj
//! binary, `open` opens the stores through jj_lib (with raw batched
//! writes in `write`), `codec` validates replicated bytes, and
//! `transfer` moves them between peers.

mod codec;
mod jj;
mod open;
pub(crate) mod transfer;
mod write;

use std::{
    fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};

pub use self::{
    jj::{jj_bin, jj_peer_warning, jj_version_warning, local_jj_version, repo_present, run_jj},
    open::OpenRepo,
};

/// The jj release series this build accepts, as minors of `0.<minor>.<patch>`
/// versions.
const SUPPORTED_JJ_MINORS: std::ops::RangeInclusive<u32> = 45..=45;

/// Store backends supported by jj-mesh, as `(<store dir>, <expected type>)`
/// pairs relative to `.jj/repo`.
const SUPPORTED_STORES: [(&str, &str); 3] = [
    ("store", "git"),
    ("op_store", "simple_op_store"),
    ("op_heads", "simple_op_heads_store"),
];

/// A local jj repository, validated to be mesh-compatible.
#[derive(Debug, Clone)]
pub struct JjRepo {
    root: PathBuf,
}

/// What a working copy was last updated to: its workspace, and the
/// operation whose view it reflects.
#[derive(Debug, Clone)]
pub struct Checkout {
    pub workspace: String,
    pub operation: jj_lib::op_store::OperationId,
}

/// The store configuration captured at repo open, compared against a fresh
/// capture to detect a repo changing underneath a running daemon (converted
/// colocation, swapped backend, replaced repo).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreFingerprint {
    /// Contents of the store `type` files, in [`SUPPORTED_STORES`] order.
    store_types: [String; SUPPORTED_STORES.len()],
    /// Contents of `store/git_target`: where the git data lives, and
    /// thereby whether the repo is colocated.
    git_target: String,
    /// Device and inode of the op heads directory.
    op_heads_dir: (u64, u64),
}

impl JjRepo {
    /// Finds the repo containing `path` (like jj, by walking up to the
    /// closest `.jj` directory) and validates it.
    pub fn discover(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path)
            .wrap_err_with(|| format!("cannot resolve {}", path.display()))?;
        let root = path
            .ancestors()
            .find(|dir| dir.join(".jj").is_dir())
            .ok_or_else(|| eyre!("no jj repo found in {} or its parents", path.display()))?;

        let repo = JjRepo {
            root: root.to_owned(),
        };
        repo.validate()?;

        Ok(repo)
    }

    /// The workspace root (the directory containing `.jj`).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The repo storage directory (`.jj/repo`).
    pub fn repo_dir(&self) -> PathBuf {
        self.root.join(".jj").join("repo")
    }

    /// Directory holding the op head marker files. Every mutating jj
    /// command atomically swaps files here, so watching it yields a
    /// per-command change signal.
    pub fn op_heads_dir(&self) -> PathBuf {
        self.repo_dir().join("op_heads").join("heads")
    }

    /// Opens the repo's stores through jj_lib for sync operations.
    pub fn open(&self) -> Result<OpenRepo> {
        OpenRepo::open(self.clone())
    }

    /// The name of the workspace at the repo root (`default` until it is
    /// renamed).
    pub fn workspace_name(&self) -> Result<String> {
        Ok(self.checkout()?.workspace)
    }

    /// What the working copy at the repo root was last updated to. Read
    /// without the working-copy lock: a jj command racing this read only
    /// makes the staleness check it feeds conservative.
    pub fn checkout(&self) -> Result<Checkout> {
        use jj_lib::{
            default_backend_factories::{
                default_backend_factories, default_working_copy_factories,
            },
            workspace::Workspace,
        };

        let workspace = Workspace::load(
            open::settings()?,
            &self.root,
            &default_backend_factories(),
            &default_working_copy_factories(),
        )
        .wrap_err_with(|| format!("cannot load the workspace at {}", self.root.display()))?;
        Ok(Checkout {
            workspace: workspace.workspace_name().as_str().to_owned(),
            operation: workspace.working_copy().operation_id().clone(),
        })
    }

    /// Captures the store configuration an open repo depends on. jj_lib
    /// resolves it once at open and never re-reads it, so a daemon holding
    /// the repo open must detect drift itself: a changed fingerprint means
    /// the open stores are stale and the repo must be reopened.
    pub fn fingerprint(&self) -> Result<StoreFingerprint> {
        use std::os::unix::fs::MetadataExt as _;

        let git_target = self.repo_dir().join("store").join("git_target");
        let git_target = fs::read_to_string(&git_target)
            .wrap_err_with(|| format!("cannot read {}", git_target.display()))?;
        // The identity (not just the path) of the op heads directory: a
        // repo replaced wholesale keeps the path but not the inode, and
        // leaves the directory watch dead on the old one.
        let heads = fs::metadata(self.op_heads_dir())
            .wrap_err_with(|| format!("cannot stat {}", self.op_heads_dir().display()))?;

        Ok(StoreFingerprint {
            store_types: self.store_types()?,
            git_target,
            op_heads_dir: (heads.dev(), heads.ino()),
        })
    }

    /// Reads the (trimmed) contents of the store `type` files, in
    /// [`SUPPORTED_STORES`] order.
    fn store_types(&self) -> Result<[String; SUPPORTED_STORES.len()]> {
        let mut types: [String; SUPPORTED_STORES.len()] = Default::default();
        for (actual, (store, _)) in types.iter_mut().zip(SUPPORTED_STORES) {
            let path = self.repo_dir().join(store).join("type");
            fs::read_to_string(&path)
                .wrap_err_with(|| format!("cannot read {}", path.display()))?
                .trim()
                .clone_into(actual);
        }
        Ok(types)
    }

    /// Checks that the repo owns its storage and uses supported backends.
    fn validate(&self) -> Result<()> {
        // In workspaces created by `jj workspace add`, `.jj/repo` is a file
        // pointing to the main workspace's repo directory.
        ensure!(
            self.repo_dir().is_dir(),
            "{} is a secondary workspace, add the main workspace instead",
            self.root.display(),
        );

        for (actual, (store, expected)) in self.store_types()?.iter().zip(SUPPORTED_STORES) {
            ensure!(
                actual == expected,
                "unsupported `{store}` backend in {}: `{actual}` (jj-mesh requires `{expected}`)",
                self.root.display(),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a fake mesh-compatible repo layout under `root`.
    fn fake_repo(root: &Path) {
        for (store, name) in SUPPORTED_STORES {
            let dir = root.join(".jj").join("repo").join(store);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("type"), name).unwrap();
        }
    }

    #[test]
    fn discover_from_subdirectory() {
        let tmp = tempfile::tempdir().unwrap();
        fake_repo(tmp.path());
        let subdir = tmp.path().join("src/deep");
        fs::create_dir_all(&subdir).unwrap();

        let repo = JjRepo::discover(&subdir).unwrap();
        assert_eq!(repo.root(), fs::canonicalize(tmp.path()).unwrap());
    }

    #[test]
    fn discover_rejects_non_repo() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(JjRepo::discover(tmp.path()).is_err());
    }

    #[test]
    fn discover_rejects_secondary_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir_all(tmp.path().join(".jj")).unwrap();
        fs::write(tmp.path().join(".jj/repo"), "../main/.jj/repo").unwrap();

        let err = JjRepo::discover(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("secondary workspace"));
    }

    #[test]
    fn fingerprint_detects_store_changes() {
        let tmp = tempfile::tempdir().unwrap();
        fake_repo(tmp.path());
        let store = tmp.path().join(".jj/repo/store");
        fs::write(store.join("git_target"), "git").unwrap();
        let heads = tmp.path().join(".jj/repo/op_heads/heads");
        fs::create_dir_all(&heads).unwrap();

        let repo = JjRepo::discover(tmp.path()).unwrap();
        let before = repo.fingerprint().unwrap();
        assert_eq!(repo.fingerprint().unwrap(), before);

        // A colocation conversion rewrites git_target.
        fs::write(store.join("git_target"), "../../../.git").unwrap();
        assert_ne!(repo.fingerprint().unwrap(), before);
        fs::write(store.join("git_target"), "git").unwrap();
        assert_eq!(repo.fingerprint().unwrap(), before);

        // A replaced op heads directory (repo recreated wholesale) changes
        // the fingerprint even when every file reads the same. Keep the
        // old directory around: a freed inode may be handed straight back.
        fs::rename(&heads, tmp.path().join("old-heads")).unwrap();
        fs::create_dir_all(&heads).unwrap();
        assert_ne!(repo.fingerprint().unwrap(), before);
    }

    #[test]
    fn workspace_name_follows_renames() {
        let fixture = crate::testing::Fixture::new();
        let dir = fixture.init_repo("proj");

        let repo = JjRepo::discover(&dir).unwrap();
        assert_eq!(repo.workspace_name().unwrap(), "default");

        fixture.jj(&dir, &["workspace", "rename", "machine-a"]);
        assert_eq!(repo.workspace_name().unwrap(), "machine-a");
    }

    #[test]
    fn discover_rejects_unsupported_backend() {
        let tmp = tempfile::tempdir().unwrap();
        fake_repo(tmp.path());
        let type_path = tmp.path().join(".jj/repo/store/type");
        fs::write(&type_path, "local").unwrap();

        let err = JjRepo::discover(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("unsupported `store` backend"));
    }
}

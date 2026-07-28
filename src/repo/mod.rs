//! Local jj repository handling.
//!
//! jj's on-disk formats are internal and unstable, so before touching a repo
//! we verify its store `type` files match the backends this build supports
//! (git commit backend, simple op store and op heads store).

pub mod codec;
mod mesh;
pub mod transfer;

use std::{
    fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};

pub use self::mesh::MeshRepo;

/// The jj release series whose on-disk formats this build was written
/// against. Must match the exact `jj-lib` pin in `Cargo.toml`: repos
/// written by another jj series may use formats this build mis-reads.
pub const SUPPORTED_JJ_SERIES: &str = "0.43";

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
    fn repo_dir(&self) -> PathBuf {
        self.root.join(".jj").join("repo")
    }

    /// Directory holding the op head marker files. Every mutating jj
    /// command atomically swaps files here, so watching it yields a
    /// per-command change signal.
    pub fn op_heads_dir(&self) -> PathBuf {
        self.repo_dir().join("op_heads").join("heads")
    }

    /// Opens the repo's stores through jj_lib for sync operations.
    pub fn open(&self) -> Result<MeshRepo> {
        MeshRepo::open(self.clone())
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
    fn store_types(&self) -> Result<Vec<String>> {
        SUPPORTED_STORES
            .iter()
            .map(|(store, _)| {
                let path = self.repo_dir().join(store).join("type");
                let actual = fs::read_to_string(&path)
                    .wrap_err_with(|| format!("cannot read {}", path.display()))?;
                Ok(actual.trim().to_owned())
            })
            .collect()
    }

    /// Checks that the repo owns its storage and uses supported backends.
    fn validate(&self) -> Result<()> {
        // In workspaces created by `jj workspace add`, `.jj/repo` is a file
        // pointing to the main workspace's repo directory.
        ensure!(
            self.repo_dir().is_dir(),
            "{} is a secondary workspace sharing another repo's storage; \
             add the main workspace instead",
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

/// Whether a jj repo exists at `root` (its `.jj` marker is present).
/// Errors other than absence (permissions, I/O) count as present: the repo
/// may be fine, only we cannot look right now.
pub fn repo_present(root: &Path) -> bool {
    match fs::metadata(root.join(".jj")) {
        Ok(_) => true,
        Err(err) => err.kind() != std::io::ErrorKind::NotFound,
    }
}

/// The version of the `jj` binary on PATH (`X.Y.Z`), or `None` when jj is
/// not runnable or its output is unrecognized. This is a heuristic: the
/// daemon cannot know which jj binary actually writes the repos, so the
/// answer is only ever used to warn, never to refuse.
pub fn local_jj_version() -> Option<String> {
    let output = std::process::Command::new("jj")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let stdout = String::from_utf8(output.stdout).ok()?;
    parse_jj_version(&stdout)
}

/// Extracts the version from `jj --version` output (`jj 0.43.0[-hash]`).
fn parse_jj_version(output: &str) -> Option<String> {
    let version = output.trim().strip_prefix("jj ")?.split('-').next()?;
    let plausible = !version.is_empty() && version.chars().all(|c| c.is_ascii_digit() || c == '.');
    plausible.then(|| version.to_owned())
}

/// Whether a jj version belongs to the [`SUPPORTED_JJ_SERIES`].
pub fn jj_version_supported(version: &str) -> bool {
    version
        .strip_prefix(SUPPORTED_JJ_SERIES)
        .is_some_and(|rest| rest.starts_with('.'))
}

/// The warning a detected jj version deserves, `None` when it is
/// supported. One wording, shared by the daemon log and the CLI.
pub fn jj_version_warning(version: Option<&str>) -> Option<String> {
    match version {
        Some(version) if jj_version_supported(version) => None,
        Some(version) => Some(format!(
            "jj {version} found on PATH; jj-mesh supports the jj \
             {SUPPORTED_JJ_SERIES} series, syncing repos written by another \
             series may misbehave"
        )),
        None => Some(format!(
            "cannot determine the local jj version (is jj on PATH?); \
             jj-mesh supports the jj {SUPPORTED_JJ_SERIES} series"
        )),
    }
}

/// The store configuration captured at repo open, compared against a fresh
/// capture to detect a repo changing underneath a running daemon (converted
/// colocation, swapped backend, replaced repo).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreFingerprint {
    /// Contents of the store `type` files, in [`SUPPORTED_STORES`] order.
    store_types: Vec<String>,
    /// Contents of `store/git_target`: where the git data lives, and
    /// thereby whether the repo is colocated.
    git_target: String,
    /// Device and inode of the op heads directory.
    op_heads_dir: (u64, u64),
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
        // the fingerprint even when every file reads the same.
        fs::remove_dir_all(&heads).unwrap();
        fs::create_dir_all(&heads).unwrap();
        assert_ne!(repo.fingerprint().unwrap(), before);
    }

    #[test]
    fn parses_and_matches_jj_versions() {
        assert_eq!(parse_jj_version("jj 0.43.0\n"), Some("0.43.0".to_owned()));
        assert_eq!(
            parse_jj_version("jj 0.43.0-dev+abcdef\n"),
            Some("0.43.0".to_owned()),
        );
        assert_eq!(parse_jj_version("command not found: jj"), None);
        assert_eq!(parse_jj_version("jj whatever"), None);
        assert_eq!(parse_jj_version(""), None);

        assert!(jj_version_supported("0.43.0"));
        assert!(jj_version_supported("0.43.12"));
        assert!(!jj_version_supported("0.44.0"));
        assert!(!jj_version_supported("0.4.30"));
        assert!(!jj_version_supported("0.430.0"));
        assert!(!jj_version_supported("0.43"));
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

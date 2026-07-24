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

    /// Checks that the repo owns its storage and uses supported backends.
    fn validate(&self) -> Result<()> {
        // In workspaces created by `jj workspace add`, `.jj/repo` is a file
        // pointing to the main workspace's repo directory.
        let repo_dir = self.repo_dir();
        ensure!(
            repo_dir.is_dir(),
            "{} is a secondary workspace sharing another repo's storage; \
             add the main workspace instead",
            self.root.display(),
        );

        for (store, expected) in SUPPORTED_STORES {
            let type_path = repo_dir.join(store).join("type");
            let actual = read_capped(&type_path, MAX_TYPE_LEN)
                .wrap_err_with(|| format!("cannot read {}", type_path.display()))?;
            ensure!(
                actual.trim() == expected,
                "unsupported `{store}` backend in {}: `{}` (jj-mesh requires `{expected}`)",
                self.root.display(),
                actual.trim(),
            );
        }

        Ok(())
    }
}

/// Longest accepted `type` file; real ones are ~20 bytes. Repo files are
/// not ours, so reads are capped and their content lossily decoded.
const MAX_TYPE_LEN: usize = 64;

/// Reads at most `max` bytes of a file as (lossy) UTF-8.
fn read_capped(path: &Path, max: usize) -> Result<String> {
    use std::io::Read as _;

    let mut buf = Vec::with_capacity(max);
    fs::File::open(path)?
        .take(max as u64)
        .read_to_end(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
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
    fn discover_rejects_unsupported_backend() {
        let tmp = tempfile::tempdir().unwrap();
        fake_repo(tmp.path());
        let type_path = tmp.path().join(".jj/repo/store/type");
        fs::write(&type_path, "local").unwrap();

        let err = JjRepo::discover(tmp.path()).unwrap_err();
        assert!(err.to_string().contains("unsupported `store` backend"));
    }

    /// `type` files come from the repo, so oversized ones must not blow up
    /// memory or error messages.
    #[test]
    fn discover_caps_type_file_read() {
        let tmp = tempfile::tempdir().unwrap();
        fake_repo(tmp.path());
        let type_path = tmp.path().join(".jj/repo/store/type");
        fs::write(&type_path, "x".repeat(1 << 20)).unwrap();

        let err = JjRepo::discover(tmp.path()).unwrap_err();
        assert!(err.to_string().len() < 500);
    }
}

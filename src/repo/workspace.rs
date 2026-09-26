//! Workspaces: the working copies sharing one repo's stores.
//!
//! The main workspace holds the repo in its `.jj/repo` directory; a
//! secondary one (`jj workspace add`) has a `.jj/repo` file pointing there
//! instead. jj records every workspace's root in the repo's workspace
//! store, which stays on the machine (it is not part of the op log).
//! Entries outlive deleted workspaces (and forgotten ones, in later jj
//! releases), so a recorded root only counts while it still points back
//! to the repo and the view still names the workspace it holds.

use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs,
    os::unix::ffi::OsStrExt as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, eyre};
use jj_lib::{op_store::OperationId, protos};
use prost::Message as _;

use super::JjRepo;

/// A jj workspace on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Workspace {
    root: PathBuf,
    /// The repo storage directory it uses, canonical.
    repo_dir: PathBuf,
}

/// What a working copy was last updated to: its workspace, and the
/// operation whose view it reflects.
#[derive(Debug, Clone)]
pub struct Checkout {
    pub workspace: String,
    pub operation: OperationId,
}

impl Workspace {
    /// Finds the workspace containing `path`, like jj, by walking up to the
    /// closest `.jj` directory.
    pub fn discover(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path)
            .wrap_err_with(|| format!("cannot resolve {}", path.display()))?;
        let root = path
            .ancestors()
            .find(|dir| dir.join(".jj").is_dir())
            .ok_or_else(|| eyre!("no jj repo found in {} or its parents", path.display()))?;
        Self::at(root)
    }

    /// The workspace rooted at `root`, a canonical path. Resolves
    /// `.jj/repo` the way jj does: a file holds the repo directory's path,
    /// relative to `.jj`.
    pub(super) fn at(root: &Path) -> Result<Self> {
        let jj_dir = root.join(".jj");
        let pointer = jj_dir.join("repo");
        let repo_dir = if pointer.is_file() {
            let target = fs::read(&pointer)
                .wrap_err_with(|| format!("cannot read {}", pointer.display()))?;
            jj_dir.join(OsStr::from_bytes(&target))
        } else {
            pointer
        };
        let repo_dir = fs::canonicalize(&repo_dir)
            .wrap_err_with(|| format!("cannot resolve the repo of {}", root.display()))?;

        Ok(Workspace {
            root: root.to_owned(),
            repo_dir,
        })
    }

    /// The workspace root (the directory containing `.jj`).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether this is the workspace holding the repo storage.
    pub fn is_main(&self) -> bool {
        self.main_root() == Some(&self.root)
    }

    /// The repo this workspace uses, validated.
    pub fn repo(&self) -> Result<JjRepo> {
        let root = self.main_root().ok_or_else(|| {
            eyre!(
                "{} uses a repo outside any workspace: {}",
                self.root.display(),
                self.repo_dir.display(),
            )
        })?;
        JjRepo::at(root)
    }

    /// The root of the main workspace, whose `.jj` holds the storage.
    fn main_root(&self) -> Option<&Path> {
        self.repo_dir
            .parent()
            .filter(|dir| dir.file_name() == Some(".jj".as_ref()))
            .and_then(Path::parent)
    }

    /// The workspace's current name.
    pub fn name(&self) -> Result<String> {
        Ok(self.checkout()?.workspace)
    }

    /// What the working copy was last updated to, read from its checkout
    /// file (a single small read; the exact `jj-lib` pin protects the
    /// format). Read without the working-copy lock: jj replaces the file
    /// atomically, and a jj command racing this read only makes the
    /// staleness check it feeds conservative.
    pub fn checkout(&self) -> Result<Checkout> {
        let path = self.root.join(".jj").join("working_copy").join("checkout");
        let bytes = fs::read(&path).wrap_err_with(|| format!("cannot read {}", path.display()))?;
        let checkout = protos::local_working_copy::Checkout::decode(&*bytes)
            .wrap_err_with(|| format!("cannot decode {}", path.display()))?;
        Ok(Checkout {
            // An empty name is jj's legacy form of `default`.
            workspace: match checkout.workspace_name {
                name if name.is_empty() => "default".to_owned(),
                name => name,
            },
            operation: OperationId::new(checkout.operation_id),
        })
    }
}

impl JjRepo {
    /// The main workspace.
    pub fn workspace(&self) -> Result<Workspace> {
        Workspace::at(self.root())
    }

    /// The workspaces of this repo on this machine that the view names
    /// (`names`), with their names: the main one, then the recorded roots
    /// that still point back to this repo. Workspaces that cannot be read
    /// are skipped.
    pub fn workspaces(&self, names: &BTreeSet<String>) -> Result<Vec<(String, Workspace)>> {
        let main = self.workspace()?;
        let mut roots = vec![main.root.clone()];
        roots.extend(self.recorded_roots(names)?);

        let mut found: Vec<(String, Workspace)> = Vec::new();
        for root in roots {
            let Ok(workspace) = Workspace::at(&root) else {
                continue;
            };
            if workspace.repo_dir != main.repo_dir || found.iter().any(|(_, ws)| ws.root == root) {
                continue;
            }
            if let Ok(name) = workspace.name()
                && names.contains(&name)
            {
                found.push((name, workspace));
            }
        }
        Ok(found)
    }

    /// The canonical roots the workspace store records for `names`. The
    /// index is decoded directly: loading the store through jj_lib creates
    /// it when missing, and the daemon never writes to a repo it does not
    /// sync.
    fn recorded_roots(&self, names: &BTreeSet<String>) -> Result<Vec<PathBuf>> {
        let index = self.repo_dir().join("workspace_store").join("index");
        let bytes = match fs::read(&index) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(err).wrap_err_with(|| format!("cannot read {}", index.display()));
            }
        };
        let store = protos::simple_workspace_store::Workspaces::decode(&*bytes)
            .wrap_err_with(|| format!("cannot decode {}", index.display()))?;

        Ok(store
            .workspaces
            .into_iter()
            .filter(|entry| names.contains(&entry.name))
            // Recorded relative to the repo dir.
            .filter_map(|entry| {
                fs::canonicalize(self.repo_dir().join(OsStr::from_bytes(&entry.path))).ok()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Fixture;

    #[test]
    fn discover_resolves_secondary_workspaces() {
        let fx = Fixture::new();
        let main = fs::canonicalize(fx.init_repo("main")).unwrap();
        fx.jj(&main, &["workspace", "add", "../child"]);
        let child = fs::canonicalize(fx.path().join("child")).unwrap();
        fs::create_dir(child.join("sub")).unwrap();

        let workspace = Workspace::discover(&child.join("sub")).unwrap();
        assert_eq!(workspace.root(), child);
        assert!(!workspace.is_main());
        assert_eq!(workspace.repo().unwrap().root(), main);
        assert_eq!(workspace.name().unwrap(), "child");

        assert!(Workspace::discover(&main).unwrap().is_main());
    }

    #[test]
    fn lists_live_workspaces_only() {
        let fx = Fixture::new();
        let main = fx.init_repo("main");
        fx.jj(&main, &["workspace", "add", "../live"]);
        fx.jj(&main, &["workspace", "add", "../deleted"]);
        fx.jj(&main, &["workspace", "add", "../forgotten"]);
        fs::remove_dir_all(fx.path().join("deleted")).unwrap();
        fx.jj(&main, &["workspace", "forget", "forgotten"]);
        let repo = JjRepo::discover(&main).unwrap();

        let roots = |names: &[&str]| -> Vec<PathBuf> {
            let names = names.iter().map(|name| (*name).to_owned()).collect();
            let workspaces = repo.workspaces(&names).unwrap();
            workspaces
                .iter()
                .map(|(_, ws)| ws.root().to_owned())
                .collect()
        };
        let canonical = |name: &str| fs::canonicalize(fx.path().join(name)).unwrap();
        assert_eq!(
            roots(&["default", "live", "deleted"]),
            [canonical("main"), canonical("live")],
        );
        // Only the workspaces the view names count, the main one included.
        assert_eq!(roots(&["live", "forgotten"]), [canonical("live")]);

        // A recorded root now holding another repo's workspace is not ours.
        fx.init_repo("other");
        fx.jj(
            &fx.path().join("other"),
            &["workspace", "add", "../deleted"],
        );
        assert_eq!(
            roots(&["default", "live", "deleted"]),
            [canonical("main"), canonical("live")],
        );
    }
}

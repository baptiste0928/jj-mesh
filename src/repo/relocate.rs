//! Moving a repo's storage to another root, keeping its workspaces.
//!
//! Server mode moves a repo into a bare repo this way (see `daemon::bare`).
//! [`JjRepo::relocation`] only checks; the caller registers the new root,
//! so that it is never deleted as unused, before [`Relocation::apply`]
//! changes anything:
//!
//! ```text
//! JjRepo::relocation ── checks the target, the filesystem and the git store
//!         │
//!   caller registers the new root, makes the repo non-colocated
//!         ▼
//! Relocation::apply ─┬─ records the workspace roots as absolute paths
//!                    ├─ writes the new workspace pointers aside
//!                    ├─ renames `.jj/repo`: the commit point
//!                    └─ swaps the pointers in, the main workspace's first
//! ```
//!
//! A failure before the rename leaves the repo usable where it was. A crash
//! between the rename and the swaps leaves workspaces to repoint by hand.

use std::{
    ffi::OsStr,
    fs,
    io::ErrorKind,
    os::unix::{ffi::OsStrExt as _, fs::MetadataExt as _},
    path::{Path, PathBuf},
};

use anyhow::{Context as _, Error, Result, bail, ensure};
use jj_lib::{
    file_util::{normalize_path, relative_path},
    lock::FileLock,
};
use prost::Message as _;

use super::{JjRepo, Workspace};
use crate::hint::hinted;

/// A checked move of a repo's storage (see [`JjRepo::relocation`]).
#[derive(Debug)]
pub struct Relocation {
    repo: JjRepo,
    /// The current storage directory, canonical.
    from: PathBuf,
    /// The storage directory at the new root.
    to: PathBuf,
    workspaces: Vec<Workspace>,
    colocated: bool,
}

/// Why [`Relocation::apply`] failed.
#[derive(Debug)]
pub enum RelocationError {
    /// Nothing moved.
    Unmoved(Error),
    /// The storage moved, but workspaces may not point to it.
    Moved(Error),
}

/// Name of the new workspace pointers, written aside in `.jj`.
const NEW_POINTER: &str = "repo.new";

impl JjRepo {
    /// Checks that the storage can move to `root`: `root` does not exist,
    /// is on the same filesystem, and the git store is inside the storage
    /// or colocated at the main workspace root.
    pub fn relocation(&self, root: &Path) -> Result<Relocation> {
        match fs::symlink_metadata(root) {
            Ok(_) => bail!("{} already exists", root.display()),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("cannot stat {}", root.display()));
            }
        }
        let link = fs::symlink_metadata(self.repo_dir())
            .with_context(|| format!("cannot stat {}", self.repo_dir().display()))?;
        ensure!(
            !link.is_symlink(),
            "{} is a symlink",
            self.repo_dir().display()
        );
        let from = self.canonical_repo_dir()?;
        let parent = root.parent().context("cannot move a repo to /")?;
        let device = |path: &Path| {
            fs::metadata(path)
                .map(|meta| meta.dev())
                .with_context(|| format!("cannot stat {}", path.display()))
        };
        ensure!(
            device(&from)? == device(parent)?,
            "{} is on another filesystem than {}",
            self.root().display(),
            parent.display(),
        );

        let git = self.git_dir()?;
        let colocated = self.is_colocated()?;
        ensure!(
            colocated || git.starts_with(&from),
            "{} uses an external git repo",
            self.root().display(),
        );
        // Colocated secondary workspaces are git worktrees, which record
        // the git store's path.
        let worktrees = fs::read_dir(git.join("worktrees")).map_or(0, Iterator::count);
        ensure!(
            worktrees == 0,
            hinted(
                format!(
                    "{} has colocated secondary workspaces (git worktrees)",
                    self.root().display()
                ),
                "Forget them first with `jj workspace forget`"
            )
        );

        Ok(Relocation {
            repo: self.clone(),
            from,
            to: root.join(".jj").join("repo"),
            workspaces: self.local_workspaces()?,
            colocated,
        })
    }

    /// Rewrites the workspace roots the workspace store records, relative
    /// to the storage, as absolute paths. Under jj's lock on the store.
    fn record_absolute_roots(&self) -> Result<()> {
        let path = self.workspace_index_path();
        let _lock = FileLock::lock(path.with_extension("lock"))?;
        let Some(mut index) = self.workspace_index()? else {
            return Ok(());
        };
        let repo_dir = self.canonical_repo_dir()?;
        for workspace in &mut index.workspaces {
            let root = normalize_path(&repo_dir.join(OsStr::from_bytes(&workspace.path)));
            workspace.path = root.into_os_string().into_encoded_bytes();
        }

        let dir = path.parent().expect("the index is in a directory");
        let temp = tempfile::NamedTempFile::new_in(dir)
            .with_context(|| format!("cannot write in {}", dir.display()))?;
        fs::write(temp.path(), index.encode_to_vec())
            .with_context(|| format!("cannot write {}", temp.path().display()))?;
        temp.persist(&path)
            .with_context(|| format!("cannot replace {}", path.display()))?;
        Ok(())
    }
}

impl Relocation {
    /// Whether the repo must be made non-colocated before [`Self::apply`].
    pub fn colocated(&self) -> bool {
        self.colocated
    }

    /// Moves the storage, then points the workspaces to it, like `jj
    /// workspace add` does.
    pub fn apply(self) -> Result<(), RelocationError> {
        self.prepare().map_err(|err| {
            self.remove_pointers();
            RelocationError::Unmoved(err)
        })?;
        fs::rename(&self.from, &self.to)
            .with_context(|| {
                format!(
                    "cannot move {} to {}",
                    self.from.display(),
                    self.to.display()
                )
            })
            .map_err(|err| {
                self.remove_pointers();
                RelocationError::Unmoved(err)
            })?;
        self.swap_pointers().map_err(|err| {
            RelocationError::Moved(err.context(format!("the repo moved to {}", self.to.display())))
        })
    }

    /// Everything that can fail before the rename: the git store is inside
    /// the storage, the workspace store and the new pointers are written.
    fn prepare(&self) -> Result<()> {
        ensure!(
            self.repo.git_dir()?.starts_with(&self.from),
            "the git store of {} is outside `.jj/repo`",
            self.repo.root().display(),
        );
        self.repo.record_absolute_roots()?;
        for workspace in &self.workspaces {
            let jj_dir = workspace.root().join(".jj");
            let pointer = relative_path(&jj_dir, &self.to);
            let path = jj_dir.join(NEW_POINTER);
            fs::write(&path, pointer.as_os_str().as_bytes())
                .with_context(|| format!("cannot write {}", path.display()))?;
        }
        let jj_dir = self.to.parent().expect("`.jj/repo` has a parent");
        fs::create_dir_all(jj_dir).with_context(|| format!("cannot create {}", jj_dir.display()))
    }

    /// Moves the new pointers in place, the main workspace's first.
    fn swap_pointers(&self) -> Result<()> {
        let (main, others): (Vec<_>, Vec<_>) = self.workspaces.iter().partition(|ws| ws.is_main());
        for workspace in main.into_iter().chain(others) {
            let jj_dir = workspace.root().join(".jj");
            fs::rename(jj_dir.join(NEW_POINTER), jj_dir.join("repo"))
                .with_context(|| format!("cannot point {} to it", workspace.root().display()))?;
        }
        Ok(())
    }

    /// Removes the new pointers written aside, best-effort.
    fn remove_pointers(&self) {
        for workspace in &self.workspaces {
            let _ = fs::remove_file(workspace.root().join(".jj").join(NEW_POINTER));
        }
    }
}

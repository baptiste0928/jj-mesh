//! jj_lib-backed access to a repo's stores, scoped to what sync needs.
//!
//! [`MeshRepo`] wraps a [`RepoLoader`] and exposes op-head enumeration, op
//! DAG walking, op/view transfer primitives and the git backend. It never
//! loads a full repo; the commit index is only touched by the explicit
//! post-sync build (see [`MeshRepo::build_commit_index`]).
//!
//! Invariants:
//! - Ops and views replicate as raw stored bytes under the sender's ids.
//!   jj computes these ids from its in-memory structs at write time and
//!   never re-verifies them, so ids written by older jj versions do not
//!   survive a decode + re-encode round trip; only byte-verbatim copies
//!   keep them identical across the mesh. Raw writes are atomic and never
//!   overwrite an existing object: for a content-addressed store the first
//!   write wins. They are staged and persisted in batches (see
//!   [`RawWriteBatch`]): one parallel sync pass, then the renames into
//!   place, preserving jj's sync-before-rename durability order.
//! - The root operation is never transferred; it is identical in every repo.
//!
//! jj's store traits are async in signature only: every call does blocking
//! file I/O underneath (writes even fsync). Fine for single ops; bulk work
//! (initial replication, catch-up) must run on a blocking thread, and the
//! raw byte reads/writes here are plain blocking I/O.

use std::{
    collections::HashSet,
    io::Write as _,
    path::{Path, PathBuf},
    sync::LazyLock,
};

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};
use jj_lib::{
    backend::CommitId,
    config::StackedConfig,
    git_backend::GitBackend,
    object_id::{HexPrefix, ObjectId, PrefixResolution},
    op_store::{Operation, OperationId, View, ViewId},
    repo::{RepoLoader, StoreFactories},
    settings::UserSettings,
};
use pollster::FutureExt as _;

use super::JjRepo;

/// A repo opened through jj_lib, ready for sync operations.
///
/// One long-lived instance per registered repo; all methods take `&self` and
/// are safe under concurrent jj commands (jj's stores are designed for
/// multiple writers, see its concurrency docs).
pub struct MeshRepo {
    repo: JjRepo,
    loader: RepoLoader,
}

impl std::fmt::Debug for MeshRepo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshRepo")
            .field("root", &self.repo.root())
            .finish_non_exhaustive()
    }
}

/// jj settings shared by every opened repo: jj's built-in defaults only.
/// The daemon must not depend on the user's jj configuration; settings
/// affect commit creation and merges, neither of which happens here.
static SETTINGS: LazyLock<Result<UserSettings, String>> = LazyLock::new(|| {
    UserSettings::from_config(StackedConfig::with_defaults()).map_err(|err| err.to_string())
});

/// The shared built-in-defaults jj settings (see [`SETTINGS`]).
pub(super) fn settings() -> Result<&'static UserSettings> {
    SETTINGS
        .as_ref()
        .map_err(|err| eyre!("cannot build jj settings: {err}"))
}

impl MeshRepo {
    /// Opens the stores of a validated repo.
    pub(super) fn open(repo: JjRepo) -> Result<Self> {
        let settings = settings()?;
        let loader = RepoLoader::init_from_file_system(
            settings,
            &repo.repo_dir(),
            &StoreFactories::default(),
        )
        .wrap_err_with(|| format!("cannot open jj repo at {}", repo.root().display()))?;

        let mesh = MeshRepo { repo, loader };
        ensure!(
            mesh.loader.store().backend_impl::<GitBackend>().is_some(),
            "{} does not use the git commit backend",
            mesh.repo.root().display(),
        );
        Ok(mesh)
    }

    /// The validated repo this was opened from.
    pub fn repo(&self) -> &JjRepo {
        &self.repo
    }

    /// The id of the root operation, common to all repos.
    pub fn root_operation_id(&self) -> &OperationId {
        self.loader.op_store().root_operation_id()
    }

    /// Current heads of the op log (multiple when divergent).
    pub async fn op_heads(&self) -> Result<Vec<OperationId>> {
        Ok(self.loader.op_heads_store().get_op_heads().await?)
    }

    /// Whether the op store contains the given operation. A single stat, no
    /// object read.
    pub async fn has_operation(&self, id: &OperationId) -> Result<bool> {
        // Stored ids all have the store's fixed hash length; anything else
        // is absent by construction. Checked here because a shorter id would
        // otherwise be treated as a prefix and could match a different op.
        if id.as_bytes().len() != self.root_operation_id().as_bytes().len() {
            return Ok(false);
        }
        let resolution = self
            .loader
            .op_store()
            .resolve_operation_id_prefix(&HexPrefix::from_id(id))
            .await?;
        Ok(matches!(resolution, PrefixResolution::SingleMatch(_)))
    }

    /// Reads an operation.
    pub async fn read_operation(&self, id: &OperationId) -> Result<Operation> {
        Ok(self.loader.op_store().read_operation(id).await?)
    }

    /// Builds the commit index at the given operation, incrementally from
    /// the nearest indexed ancestor operation (the same build `jj debug
    /// reindex` runs, minus the wipe). Syncs call it after publishing an
    /// op head so the user's next jj command loads the index instead of
    /// building it; after a clone that build would cover the entire
    /// replicated history. Blocking.
    pub fn build_commit_index(&self, id: &OperationId) -> Result<()> {
        use jj_lib::default_index::DefaultIndexStore;

        let Some(index_store) = self
            .loader
            .index_store()
            .downcast_ref::<DefaultIndexStore>()
        else {
            // Another index backend indexes on its own terms.
            return Ok(());
        };
        let data = self.loader.op_store().read_operation(id).block_on()?;
        let operation =
            jj_lib::operation::Operation::new(self.loader.op_store().clone(), id.clone(), data);
        index_store
            .build_index_at_operation(&operation, self.loader.store())
            .block_on()
            .wrap_err_with(|| format!("cannot build commit index at {}", id.hex()))?;
        Ok(())
    }

    /// Reads a view.
    pub async fn read_view(&self, id: &ViewId) -> Result<View> {
        Ok(self.loader.op_store().read_view(id).await?)
    }

    /// Whether the op store contains the given view.
    pub async fn has_view(&self, id: &ViewId) -> Result<bool> {
        use jj_lib::op_store::OpStoreError;
        match self.loader.op_store().read_view(id).await {
            Ok(_) => Ok(true),
            Err(OpStoreError::ObjectNotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Reads an operation's raw stored bytes, for byte-verbatim
    /// replication to a peer.
    pub fn read_operation_bytes(&self, id: &OperationId) -> Result<Vec<u8>> {
        read_raw(&self.op_store_dir().join("operations"), id)
    }

    /// Reads a view's raw stored bytes (see
    /// [`Self::read_operation_bytes`]).
    pub fn read_view_bytes(&self, id: &ViewId) -> Result<Vec<u8>> {
        read_raw(&self.op_store_dir().join("views"), id)
    }

    /// Starts a batch of replicated raw object writes (see
    /// [`RawWriteBatch`]).
    pub fn raw_write_batch(&self) -> RawWriteBatch<'_> {
        RawWriteBatch {
            repo: self,
            staged: Vec::new(),
        }
    }

    /// Rejects replicated ids the store could never have produced: wrong
    /// length, or the all-zeros root id, which jj synthesizes instead of
    /// storing.
    fn check_replicated_id(&self, kind: &str, id: &impl ObjectId) -> Result<()> {
        let root = self.root_operation_id().as_bytes();
        ensure!(
            id.as_bytes().len() == root.len(),
            "bad {kind} id length ({})",
            id.as_bytes().len(),
        );
        ensure!(id.as_bytes() != root, "refusing to store the root {kind}");
        Ok(())
    }

    /// The simple op store's storage directory.
    fn op_store_dir(&self) -> PathBuf {
        self.repo.repo_dir().join("op_store")
    }

    /// Publishes `new` as an op head, removing the `old` heads it
    /// supersedes. The op and its ancestry (ops, views, commits) must
    /// already be stored; a head pointing at a missing op would make every
    /// jj command in the repo fail to load, so this is checked here.
    ///
    /// Security contract: callers must establish from *local* data that
    /// each `old` head is an ancestor of `new`. Trusting a peer's claim
    /// would let it unlist arbitrary heads (op log rollback).
    pub async fn update_op_heads(&self, old: &[OperationId], new: &OperationId) -> Result<()> {
        ensure!(
            self.has_operation(new).await?,
            "refusing to publish op head {}: operation is not stored",
            new.hex(),
        );
        self.loader
            .op_heads_store()
            .update_op_heads(old, new)
            .await?;
        Ok(())
    }

    /// Collects the ancestor operations of `heads` down to (excluding) the
    /// `known` ops and the root, returned in parents-first order — the safe
    /// order to replicate them in. `heads` and `known` must be stored.
    ///
    /// Only the exact `known` ids are excluded, not their ancestors: if a
    /// head reaches past `known` through another path (typical for
    /// divergence-merge ops), the shared history below it is emitted again.
    /// Replication stays correct because writes are idempotent, but callers
    /// negotiating a transfer should pass a frontier that covers all paths,
    /// not just their current heads.
    pub async fn ancestors_until(
        &self,
        heads: &[OperationId],
        known: &[OperationId],
    ) -> Result<Vec<(OperationId, Operation)>> {
        enum Frame {
            Enter(OperationId),
            Exit(OperationId, Box<Operation>),
        }

        let known: HashSet<&OperationId> = known.iter().collect();
        let mut visited: HashSet<OperationId> = HashSet::new();
        let mut order = Vec::new();
        let mut stack: Vec<Frame> = heads.iter().cloned().map(Frame::Enter).collect();

        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter(id) => {
                    if known.contains(&id)
                        || id == *self.root_operation_id()
                        || !visited.insert(id.clone())
                    {
                        continue;
                    }
                    let op = self.read_operation(&id).await.wrap_err_with(|| {
                        format!("op DAG walk reached unreadable operation {}", id.hex())
                    })?;
                    let parents = op.parents.clone();
                    stack.push(Frame::Exit(id, Box::new(op)));
                    stack.extend(parents.into_iter().map(Frame::Enter));
                }
                // All parents were pushed above this frame, so they are
                // fully emitted by the time it pops: parents-first holds.
                Frame::Exit(id, op) => order.push((id, *op)),
            }
        }

        Ok(order)
    }

    /// Proves the repo's stored formats are ones this build understands:
    /// reads every current op head and its view through the mesh codec,
    /// the same validation applied to replicated bytes. A repo written by
    /// a different jj series fails here at open, with a pointed error,
    /// instead of failing obscurely mid-sync.
    ///
    /// Also verifies the working-copy commits carry the `change-id` git
    /// header: change ids replicate through the git objects alone (the
    /// local extras table never leaves the machine), so a jj writing
    /// without the header would sync commits whose change identity
    /// silently diverges across the mesh. The working-copy commit is
    /// checked because it is always jj-written; commits imported from
    /// plain git legitimately lack the header and are consistent anyway
    /// (their synthetic change ids derive from the commit id).
    pub async fn self_check(&self) -> Result<()> {
        for head in self.op_heads().await? {
            if head == *self.root_operation_id() {
                continue;
            }
            let parse = || -> Result<()> {
                let meta = super::codec::parse_operation(&self.read_operation_bytes(&head)?)?;
                super::codec::parse_view(&self.read_view_bytes(&meta.view_id)?)?;
                Ok(())
            };
            parse().wrap_err_with(|| {
                format!(
                    "cannot decode op head {}; was the repo written by a jj \
                     release other than the supported {} series?",
                    head.hex(),
                    super::SUPPORTED_JJ_SERIES,
                )
            })?;

            let op = self.read_operation(&head).await?;
            let view = self.read_view(&op.view_id).await?;
            for wc_commit in view.wc_commit_ids.values() {
                self.check_change_id_header(wc_commit)?;
            }
        }
        Ok(())
    }

    /// Ensures a jj-written commit carries the `change-id` git header (see
    /// [`Self::self_check`]).
    fn check_change_id_header(&self, id: &CommitId) -> Result<()> {
        // jj's virtual root commit is never a real git object.
        if id.as_bytes().iter().all(|byte| *byte == 0) {
            return Ok(());
        }

        let git = self.git_backend().git_repo();
        let oid = gix::ObjectId::try_from(id.as_bytes())
            .map_err(|err| eyre!("bad working-copy commit id: {err}"))?;
        let object = git
            .find_object(oid)
            .map_err(|err| eyre!("cannot read working-copy commit {}: {err}", id.hex()))?;
        let commit = object
            .try_to_commit_ref()
            .map_err(|err| eyre!("cannot decode working-copy commit {}: {err}", id.hex()))?;

        ensure!(
            jj_lib::git_backend::extract_change_id_from_commit(&commit).is_some(),
            "working-copy commit {} has no change-id header, so change ids \
             would not survive syncing; enable `git.write-change-id-header` \
             in the jj config (the default) and run any jj command in the \
             repo",
            id.hex(),
        );
        Ok(())
    }

    /// The git backend of the repo.
    pub fn git_backend(&self) -> &GitBackend {
        self.loader
            .store()
            .backend_impl::<GitBackend>()
            .expect("backend type checked at open")
    }

    /// Path to the git repository holding the commit data (the `.git`
    /// directory when colocated), for direct object access via gitoxide.
    pub fn git_repo_path(&self) -> &Path {
        self.git_backend().git_repo_path()
    }

    /// Whether the repo is colocated with a user-visible `.git`, which
    /// then must be kept in sync when applying remote operations.
    pub fn is_colocated(&self) -> bool {
        self.git_repo_path() == self.repo.root().join(".git")
    }
}

/// Reads a raw object file from a simple op store directory.
fn read_raw(dir: &Path, id: &impl ObjectId) -> Result<Vec<u8>> {
    let path = dir.join(id.hex());
    std::fs::read(&path).wrap_err_with(|| format!("cannot read {}", path.display()))
}

/// A batch of replicated raw object writes, staged as temp files and
/// persisted together: one parallel sync pass over all staged files, then
/// the atomic renames into place. This replaces the per-file fsync of
/// jj's own store writes, which dominates apply time for large batches
/// (a clone replicates the whole op log), while preserving jj's
/// sync-before-rename order: an object observed under its final id
/// always holds durable, complete content, so an interrupted batch can
/// be retried with the existing objects skipped.
///
/// Nothing is readable under its final id until [`Self::persist`];
/// dropping the batch instead discards the staged files.
pub struct RawWriteBatch<'a> {
    repo: &'a MeshRepo,
    staged: Vec<(tempfile::TempPath, PathBuf)>,
}

impl RawWriteBatch<'_> {
    /// Stages a replicated operation's raw bytes under the id it has on
    /// the sender. Callers must have validated the bytes structurally
    /// (see [`super::codec`]); in particular jj `assert!`s on reading a
    /// parentless non-root op.
    pub fn write_operation_bytes(&mut self, id: &OperationId, bytes: &[u8]) -> Result<()> {
        self.repo.check_replicated_id("operation", id)?;
        self.stage(&self.repo.op_store_dir().join("operations"), id, bytes)
    }

    /// Stages a replicated view's raw bytes under the id it has on the
    /// sender (see [`Self::write_operation_bytes`]).
    pub fn write_view_bytes(&mut self, id: &ViewId, bytes: &[u8]) -> Result<()> {
        self.repo.check_replicated_id("view", id)?;
        self.stage(&self.repo.op_store_dir().join("views"), id, bytes)
    }

    /// Stages one object file, leaving any existing object untouched
    /// (the first write wins, see the module invariants). The temp file
    /// is closed right away so a large batch does not hold thousands of
    /// open descriptors.
    fn stage(&mut self, dir: &Path, id: &impl ObjectId, bytes: &[u8]) -> Result<()> {
        let path = dir.join(id.hex());
        if path.exists() {
            return Ok(());
        }
        let temp = tempfile::NamedTempFile::new_in(dir)
            .wrap_err_with(|| format!("cannot create temp file in {}", dir.display()))?;
        temp.as_file().write_all(bytes)?;
        self.staged.push((temp.into_temp_path(), path));
        Ok(())
    }

    /// Makes all staged files durable in one parallel sync pass, then
    /// renames each into place in staging order (so parents-first write
    /// order carries over to visibility). Directory entries are not
    /// synced, matching the durability of jj's own store writes.
    pub fn persist(self) -> Result<()> {
        // The barriers are independent across files; issuing them from a
        // few threads lets the device coalesce the flushes instead of
        // paying them serially.
        const SYNC_THREADS: usize = 8;

        let chunk_size = self.staged.len().div_ceil(SYNC_THREADS).max(1);
        std::thread::scope(|scope| {
            let workers: Vec<_> = self
                .staged
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || -> Result<()> {
                        for (temp, path) in chunk {
                            std::fs::File::open(temp)
                                .and_then(|file| file.sync_data())
                                .wrap_err_with(|| {
                                    format!("cannot sync staged write of {}", path.display())
                                })?;
                        }
                        Ok(())
                    })
                })
                .collect();
            workers
                .into_iter()
                .try_for_each(|worker| worker.join().expect("sync worker panicked"))
        })?;

        for (temp, path) in self.staged {
            if cfg!(windows) {
                // Overwriting can fail on Windows if the file is open
                // elsewhere; an existing file holds equivalent content
                // (content-addressed), so keep it.
                match temp.persist_noclobber(&path) {
                    Ok(()) => {}
                    Err(_) if path.exists() => {}
                    Err(err) => {
                        return Err(err.error)
                            .wrap_err_with(|| format!("cannot persist {}", path.display()));
                    }
                }
            } else {
                // On Unix the rename atomically replaces any raced
                // duplicate with equivalent content.
                temp.persist(&path)
                    .map_err(|err| err.error)
                    .wrap_err_with(|| format!("cannot persist {}", path.display()))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;
    use crate::testing::Fixture;

    fn open(dir: &Path) -> MeshRepo {
        JjRepo::discover(dir).unwrap().open().unwrap()
    }

    #[tokio::test]
    async fn reads_heads_and_walks_to_root() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        fx.jj(&dir, &["new", "-m", "second"]);
        let repo = open(&dir);

        let heads = repo.op_heads().await.unwrap();
        assert_eq!(heads.len(), 1);
        assert!(repo.has_operation(&heads[0]).await.unwrap());

        // init + describe + new, in parents-first order ending at the head.
        let ops = repo.ancestors_until(&heads, &[]).await.unwrap();
        assert!(ops.len() >= 3);
        assert_eq!(ops.last().unwrap().0, heads[0]);
        let mut seen = HashSet::new();
        for (id, op) in &ops {
            for parent in &op.parents {
                assert!(
                    seen.contains(parent) || parent == repo.root_operation_id(),
                    "operation emitted before its parent",
                );
            }
            seen.insert(id.clone());
        }

        // Every op's view is readable, and the git repo is where it says.
        for (_, op) in &ops {
            repo.read_view(&op.view_id).await.unwrap();
        }
        assert!(repo.git_repo_path().is_dir());
        // A repo written by the supported jj passes the format self-check.
        repo.self_check().await.unwrap();

        let absent = OperationId::new(vec![0xff; 64]);
        assert!(!repo.has_operation(&absent).await.unwrap());
        // The all-zeros root operation is synthesized, not stored, but must
        // still count as present.
        assert!(repo.has_operation(repo.root_operation_id()).await.unwrap());
        // A truncated id is a prefix of a stored one, but not stored itself.
        let truncated = OperationId::new(heads[0].as_bytes()[..8].to_vec());
        assert!(!repo.has_operation(&truncated).await.unwrap());
    }

    /// Ops and views replicated as raw bytes keep their ids byte for byte,
    /// and jj itself must accept the result.
    #[tokio::test]
    async fn replicates_ops_with_identical_ids() {
        let fx = Fixture::new();
        let a = fx.init_repo("a");
        let b = fx.path().join("b");
        let cp = Command::new("cp")
            .arg("-r")
            .args([&a, &b])
            .status()
            .unwrap();
        assert!(cp.success());
        fx.jj(&a, &["new", "-m", "after-fork"]);
        fx.jj(&a, &["describe", "-m", "after-fork-amended"]);

        let (ra, rb) = (open(&a), open(&b));
        let a_heads = ra.op_heads().await.unwrap();
        let b_heads = rb.op_heads().await.unwrap();
        assert_ne!(a_heads, b_heads);

        // Mirror the sync write order: git objects must land before the ops
        // that reference them (jj indexes the referenced commits as soon as
        // it loads the repo at the new head).
        let objects = Command::new("cp")
            .arg("-rn")
            .arg(format!("{}/objects/.", ra.git_repo_path().display()))
            .arg(format!("{}/objects/", rb.git_repo_path().display()))
            .status()
            .unwrap();
        assert!(objects.success());

        let missing = ra.ancestors_until(&a_heads, &b_heads).await.unwrap();
        assert!(!missing.is_empty());
        let mut writes = rb.raw_write_batch();
        for (id, op) in &missing {
            let view = ra.read_view_bytes(&op.view_id).unwrap();
            writes.write_view_bytes(&op.view_id, &view).unwrap();
            let bytes = ra.read_operation_bytes(id).unwrap();
            writes.write_operation_bytes(id, &bytes).unwrap();
        }
        // Staged writes are invisible until the batch persists.
        let (head_op, _) = missing.last().unwrap();
        assert!(!rb.has_operation(head_op).await.unwrap());
        writes.persist().unwrap();
        for (id, _) in &missing {
            assert_eq!(
                rb.read_operation_bytes(id).unwrap(),
                ra.read_operation_bytes(id).unwrap(),
            );
        }
        rb.update_op_heads(&b_heads, &a_heads[0]).await.unwrap();

        assert_eq!(rb.op_heads().await.unwrap(), a_heads);
        // jj itself must accept the replicated op log.
        fx.jj(&b, &["op", "log"]);
    }

    /// A repo whose jj writes commits without the change-id header must
    /// fail the self-check: its change ids would not survive syncing.
    #[tokio::test]
    async fn self_check_rejects_missing_change_id_header() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        fx.jj(
            &dir,
            &[
                "config",
                "set",
                "--repo",
                "git.write-change-id-header",
                "false",
            ],
        );
        // Rewrite the working-copy commit under the disabled setting.
        fx.jj(&dir, &["new", "-m", "no header"]);

        let repo = open(&dir);
        let err = repo.self_check().await.unwrap_err();
        assert!(
            format!("{err:#}").contains("write-change-id-header"),
            "{err:#}",
        );

        // Re-enabling the header heals the repo on the next jj command.
        fx.jj(
            &dir,
            &[
                "config",
                "set",
                "--repo",
                "git.write-change-id-header",
                "true",
            ],
        );
        fx.jj(&dir, &["new", "-m", "healed"]);
        open(&dir).self_check().await.unwrap();
    }

    /// Raw writes never replace stored history: the first write wins, and
    /// impossible ids (root, wrong length) are rejected outright.
    #[tokio::test]
    async fn raw_writes_reject_bad_ids_and_never_clobber() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        let repo = open(&dir);
        let head = repo.op_heads().await.unwrap().remove(0);
        let stored = repo.read_operation_bytes(&head).unwrap();

        let mut writes = repo.raw_write_batch();
        let err = writes
            .write_operation_bytes(repo.root_operation_id(), &stored)
            .unwrap_err();
        assert!(err.to_string().contains("root"), "{err:#}");

        let short = OperationId::new(vec![1; 8]);
        let err = writes.write_operation_bytes(&short, &stored).unwrap_err();
        assert!(err.to_string().contains("id length"), "{err:#}");

        writes.write_operation_bytes(&head, b"garbage").unwrap();
        writes.persist().unwrap();
        assert_eq!(repo.read_operation_bytes(&head).unwrap(), stored);
    }

    /// A dropped batch leaves no trace: nothing becomes readable and the
    /// staged temp files are cleaned up.
    #[tokio::test]
    async fn dropped_write_batch_discards_staged_files() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        let repo = open(&dir);
        let head = repo.op_heads().await.unwrap().remove(0);
        let bytes = repo.read_operation_bytes(&head).unwrap();

        let unstored = OperationId::new(vec![0xff; 64]);
        let mut writes = repo.raw_write_batch();
        writes.write_operation_bytes(&unstored, &bytes).unwrap();
        drop(writes);

        assert!(!repo.has_operation(&unstored).await.unwrap());
        let ops_dir = repo.op_store_dir().join("operations");
        for entry in std::fs::read_dir(&ops_dir).unwrap() {
            let name = entry.unwrap().file_name();
            let name = name.to_string_lossy();
            assert!(!name.starts_with(".tmp"), "leftover temp file {name}");
        }
    }

    #[tokio::test]
    async fn publish_rejects_unstored_head() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        let repo = open(&dir);

        let heads = repo.op_heads().await.unwrap();
        let unstored = OperationId::new(vec![0xff; 64]);
        let err = repo.update_op_heads(&heads, &unstored).await.unwrap_err();
        assert!(err.to_string().contains("not stored"));

        // The bogus publish must not have touched the existing heads.
        assert_eq!(repo.op_heads().await.unwrap(), heads);
    }
}

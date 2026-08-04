//! Batched writes of replicated raw objects into the op store.

use std::{
    io::Write as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _};
use jj_lib::{
    object_id::ObjectId,
    op_store::{OperationId, ViewId},
};

use super::open::OpenRepo;

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
    repo: &'a OpenRepo,
    staged: Vec<(tempfile::TempPath, PathBuf)>,
}

impl<'a> RawWriteBatch<'a> {
    pub(super) fn new(repo: &'a OpenRepo) -> Self {
        RawWriteBatch {
            repo,
            staged: Vec::new(),
        }
    }

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
    /// (the first write wins, see the `open` module invariants). The temp file
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
            // The rename atomically replaces any raced duplicate with
            // equivalent content.
            temp.persist(&path)
                .map_err(|err| err.error)
                .wrap_err_with(|| format!("cannot persist {}", path.display()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use jj_lib::op_store::OperationId;

    use crate::{repo::JjRepo, testing::Fixture};

    fn open(dir: &std::path::Path) -> crate::repo::OpenRepo {
        JjRepo::discover(dir).unwrap().open().unwrap()
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
}

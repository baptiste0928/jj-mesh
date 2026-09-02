//! Applying a validated batch, in the crash-safe order: anti-GC keep
//! refs, views and ops (parents first, persisted with one batched
//! durability sync), change-id extras, the commit index for the incoming
//! heads, the git ref mirror, and only then the op head publication
//! that makes anything visible to jj.
//!
//! The apply is split in two so [`super::fetch`] can run the index build
//! in between: [`stage`] lands everything on disk and computes what to
//! publish, [`publish`] makes it visible.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use color_eyre::eyre::{Result, eyre};
use jj_lib::{backend::CommitId, object_id::ObjectId as _, op_store::OperationId};
use pollster::FutureExt as _;
use tracing::info;

use super::{OpBatch, mirror, to_gix_id};
use crate::repo::{OpenRepo, codec::OpMeta};

/// Walk budget for [`superseded_by`]: stopping early is safe (unwalked
/// local heads stay listed and jj reconciles the divergence), so deep
/// histories need not be traversed exhaustively.
const SUPERSEDE_WALK_BUDGET: usize = 1 << 16;

/// A staged batch: everything is on disk and readable; the index build,
/// the git ref mirror and the op head publication remain.
pub(super) struct Staged {
    /// Wants to publish, each with the local heads it supersedes.
    to_publish: Vec<(OperationId, Vec<OperationId>)>,
    /// Ops the batch carried, for the publication log line.
    ops: usize,
}

/// Stages a validated batch: keep refs, views, ops, extras, and the
/// supersession computation, leaving nothing visible to jj yet. Runs on a
/// blocking thread.
pub(super) fn stage(
    repo: &Arc<OpenRepo>,
    batch: &OpBatch,
    wants: &[OperationId],
    local_heads: &[OperationId],
) -> Result<Staged> {
    // Anti-GC keep refs for every new view head, before anything
    // references those commits.
    let new_commit_heads: HashSet<CommitId> = batch
        .views
        .iter()
        .flat_map(|view| view.meta.head_ids.iter().cloned())
        .collect();
    write_keep_refs(repo, &new_commit_heads)?;

    // Staged unsynced and persisted with one parallel sync pass, instead
    // of a serial fsync per file (a clone stages the whole op log here).
    let mut writes = repo.raw_write_batch();
    for view in &batch.views {
        writes.write_view_bytes(&view.id, &view.bytes)?;
    }
    for op in &batch.ops {
        writes.write_operation_bytes(&op.id, &op.bytes)?;
    }
    writes.persist()?;

    // Materialize change-id extras for the new commits eagerly instead of
    // relying on jj's lazy import fallback.
    let heads: Vec<&CommitId> = new_commit_heads.iter().collect();
    repo.git_backend()
        .import_head_commits(heads)
        .map_err(|err| eyre!("cannot import commit metadata: {err}"))?;

    // Which local heads each want supersedes, established by walking the
    // want's ancestry through validated data only (the parsed batch and
    // the local store). Batch membership or parent claims alone are NOT
    // proof of ancestry: a hostile batch op naming a local head as its
    // parent must not unlist that head.
    let by_id = batch.ops_by_id();
    let to_publish: Vec<(OperationId, Vec<OperationId>)> = wants
        .iter()
        .filter(|want| !local_heads.contains(want))
        .map(|want| {
            let superseded = superseded_by(repo, &by_id, want, local_heads);
            (want.clone(), superseded)
        })
        .collect();

    // Before any head points at the replicated bytes, jj itself must be
    // able to read the ops being published and their views.
    for (want, _) in &to_publish {
        let op = repo.read_operation(want).block_on()?;
        repo.read_view(&op.view_id).block_on()?;
    }

    Ok(Staged {
        to_publish,
        ops: batch.ops.len(),
    })
}

/// Publishes a staged batch: the git ref mirror, then the op head
/// publication that makes everything visible to jj. Runs on a blocking
/// thread.
pub(super) fn publish(repo: &Arc<OpenRepo>, staged: &Staged) -> Result<()> {
    let Staged { to_publish, ops } = staged;

    mirror::run(repo, to_publish)?;

    // Each want removes exactly the heads its own ancestry covers, so a
    // crash between two to_publish cannot unlist a head whose replacement
    // was never published.
    for (want, covered) in to_publish {
        repo.update_op_heads(covered, want).block_on()?;
    }
    if !to_publish.is_empty() {
        info!(heads = to_publish.len(), ops, "applied synced operations");
    }
    Ok(())
}

/// Parents of an op, from the validated batch first and the local store
/// as fallback. Empty when neither has the op: without it the ancestry
/// below cannot be verified, and the walk treats that as a boundary.
fn parents_of(
    repo: &OpenRepo,
    batch: &HashMap<&OperationId, &OpMeta>,
    id: &OperationId,
) -> Vec<OperationId> {
    match batch.get(id) {
        Some(meta) => meta.parents.clone(),
        None => repo
            .read_operation(id)
            .block_on()
            .map(|op| op.parents)
            .unwrap_or_default(),
    }
}

/// The local heads that are ancestors of `want`, walking parent links of
/// validated ops.
fn superseded_by(
    repo: &OpenRepo,
    batch: &HashMap<&OperationId, &OpMeta>,
    want: &OperationId,
    local_heads: &[OperationId],
) -> Vec<OperationId> {
    let mut superseded = Vec::new();
    let mut visited: HashSet<OperationId> = HashSet::new();
    let mut stack = vec![want.clone()];
    let mut budget = SUPERSEDE_WALK_BUDGET;

    while let Some(id) = stack.pop() {
        if budget == 0 {
            break;
        }
        budget -= 1;

        if id != *want && local_heads.contains(&id) {
            // A local head's own ancestry contains no other current head,
            // so there is no need to descend further.
            superseded.push(id);
            continue;
        }
        for parent in parents_of(repo, batch, &id) {
            if parent != *repo.root_operation_id() && !visited.contains(&parent) {
                visited.insert(parent.clone());
                stack.push(parent);
            }
        }
    }

    superseded
}

/// Writes `refs/jj/keep/*` refs so git GC cannot prune commits jj has not
/// imported yet (mirrors the git backend's own convention).
fn write_keep_refs(repo: &OpenRepo, commit_ids: &HashSet<CommitId>) -> Result<()> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};

    let git = repo.git_backend().git_repo();
    let edits: Vec<RefEdit> = commit_ids
        .iter()
        .map(|id| -> Result<RefEdit> {
            Ok(RefEdit {
                change: Change::Update {
                    log: LogChange::default(),
                    expected: PreviousValue::Any,
                    new: gix::refs::Target::Object(to_gix_id(id)?),
                },
                name: format!("refs/jj/keep/{}", id.hex())
                    .try_into()
                    .map_err(|err| eyre!("bad ref name: {err}"))?,
                deref: false,
            })
        })
        .collect::<Result<_>>()?;

    git.edit_references(edits)
        .map_err(|err| eyre!("cannot write keep refs: {err}"))?;
    Ok(())
}

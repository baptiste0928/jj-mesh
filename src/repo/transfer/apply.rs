//! Applying a validated batch, in the crash-safe order: keep refs, views
//! and ops (parents first, persisted with one batched durability sync),
//! change-id extras, the colocated ref mirror, and only then the op head
//! publication that makes anything visible to jj.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use color_eyre::eyre::{Result, eyre};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OperationId, RefTarget, View},
};
use pollster::FutureExt as _;
use tracing::{info, warn};

use super::{OpBatch, to_gix_id};
use crate::repo::{OpenRepo, codec::OpMeta};

/// Walk budget for [`superseded_by`]: stopping early is safe (unwalked
/// local heads stay listed and jj reconciles the divergence), so deep
/// histories need not be traversed exhaustively.
const SUPERSEDE_WALK_BUDGET: usize = 1 << 16;

/// Applies a validated batch: keep refs, views, ops, extras, the colocated
/// ref mirror, then head publication. Runs on a blocking thread.
pub(super) fn apply(
    repo: &Arc<OpenRepo>,
    batch: &OpBatch,
    wants: &[OperationId],
    local_heads: &[OperationId],
) -> Result<Vec<OperationId>> {
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
    let mut to_publish: Vec<(OperationId, Vec<OperationId>)> = Vec::new();
    let mut all_superseded: HashSet<OperationId> = HashSet::new();
    for want in wants {
        if local_heads.contains(want) {
            continue;
        }
        let superseded = superseded_by(repo, &by_id, want, local_heads);
        all_superseded.extend(superseded.iter().cloned());
        to_publish.push((want.clone(), superseded));
    }

    // Before any head points at the replicated bytes, jj itself must be
    // able to read the ops being published and their views.
    for (want, _) in &to_publish {
        let op = repo.read_operation(want).block_on()?;
        repo.read_view(&op.view_id).block_on()?;
    }

    // The colocated git mirror is only safe when the fetch fast-forwards
    // a single old head to a single new one; under divergence the merged
    // view decides, and with several old heads there is no single previous
    // view to reconcile git refs against.
    let fast_forward = to_publish.len() == 1 && local_heads.len() == 1 && all_superseded.len() == 1;
    if repo.is_colocated() && !to_publish.is_empty() {
        if fast_forward {
            let new_op = repo.read_operation(&to_publish[0].0).block_on()?;
            let new_view = repo.read_view(&new_op.view_id).block_on()?;
            let old_op = repo.read_operation(&local_heads[0]).block_on()?;
            let old_view = repo.read_view(&old_op.view_id).block_on()?;
            mirror_git_refs(repo, &new_view, &old_view)?;
        } else {
            warn!("divergent sync in colocated repo: git refs not mirrored");
        }
    }

    // Each want removes exactly the heads its own ancestry covers, so a
    // crash between two to_publish cannot unlist a head whose replacement
    // was never published.
    let mut published = Vec::new();
    for (want, superseded) in &to_publish {
        repo.update_op_heads(superseded, want).block_on()?;
        published.push(want.clone());
    }
    if !published.is_empty() {
        info!(
            heads = published.len(),
            ops = batch.ops.len(),
            "applied synced operations",
        );
    }
    Ok(published)
}

/// The local heads that are ancestors of `want`, walking parent links of
/// validated ops (batch first, local store as fallback).
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
        let parents = match batch.get(&id) {
            Some(meta) => meta.parents.clone(),
            None => match repo.read_operation(&id).block_on() {
                Ok(op) => op.parents,
                // Boundary: without the op the ancestry below cannot be
                // verified; not superseding is the safe direction.
                Err(_) => continue,
            },
        };
        for parent in parents {
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

/// Mirrors the applied view's `git_refs` (all namespaces: heads, tags,
/// remotes) into the colocated `.git`, so the next git import does not
/// misread the replicated refs as local git changes.
///
/// Exporter semantics, like jj's own: only refs known to the previous
/// view are touched, each with compare-and-swap against its old value.
/// Refs the user created or moved directly in git are left alone (a
/// failed swap is logged and skipped; jj's importer reconciles it), and
/// HEAD is never touched.
fn mirror_git_refs(repo: &OpenRepo, view: &View, old_view: &View) -> Result<()> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};

    let git = repo.git_backend().git_repo();
    let mut edits: Vec<RefEdit> = Vec::new();

    // Create or move refs to the new view's targets.
    for (name, target) in &view.git_refs {
        let Some(new_id) = target.as_normal() else {
            // Conflicted refs cannot be exported; leave the local ref.
            continue;
        };
        let old = old_view.git_refs.get(name);
        let expected = match old.map(RefTarget::as_normal) {
            // Known before with a clean value: swap only if unchanged.
            Some(Some(old_id)) if old_id == new_id => continue,
            Some(Some(old_id)) => {
                PreviousValue::MustExistAndMatch(gix::refs::Target::Object(to_gix_id(old_id)?))
            }
            // Previously conflicted: no reliable baseline, leave it.
            Some(None) => continue,
            // New ref: create, unless git already has one (user's).
            None => PreviousValue::MustNotExist,
        };
        edits.push(RefEdit {
            change: Change::Update {
                log: LogChange::default(),
                expected,
                new: gix::refs::Target::Object(to_gix_id(new_id)?),
            },
            name: name
                .as_str()
                .try_into()
                .map_err(|err| eyre!("bad ref name: {err}"))?,
            deref: false,
        });
    }

    // Prune refs the new view no longer has, again only from their known
    // old value.
    for (name, old_target) in &old_view.git_refs {
        if view.git_refs.contains_key(name) {
            continue;
        }
        let Some(old_id) = old_target.as_normal() else {
            continue;
        };
        edits.push(RefEdit {
            change: Change::Delete {
                expected: PreviousValue::MustExistAndMatch(gix::refs::Target::Object(to_gix_id(
                    old_id,
                )?)),
                log: gix::refs::transaction::RefLog::AndReference,
            },
            name: name
                .as_str()
                .try_into()
                .map_err(|err| eyre!("bad ref name: {err}"))?,
            deref: false,
        });
    }

    // Edits apply individually: one ref the user raced must not abort the
    // rest of the mirror.
    for edit in edits {
        let name = edit.name.as_bstr().to_owned();
        if let Err(err) = git.edit_references(Some(edit)) {
            warn!("skipping git ref mirror of {name}: {err}");
        }
    }
    Ok(())
}

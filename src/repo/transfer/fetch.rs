//! Fetcher side of a fetch: requesting the op-log delta, validating it,
//! pulling the git objects it references, then applying it (see [`super`]
//! for the crash-safe apply order).

use std::{collections::HashSet, sync::Arc};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure, eyre};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OperationId, ViewId},
};
use pollster::FutureExt as _;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{FetchOutcome, OpBatch, StoredOp, StoredView, apply::apply, to_gix_id, to_gix_kind};
use crate::{
    config::{RepoId, sanitize},
    net::{
        sync::{
            FetchRequest, GitFrame, GitRequest, MAX_GIT_FRAME_SIZE, MAX_GIT_HAVES,
            MAX_OP_FRAME_SIZE, MAX_WANTS, OpFrame,
        },
        wire::{read_message, write_message},
    },
    repo::{MeshRepo, codec},
};

/// Read budget when sampling have-ancestors for a fetch request.
const SAMPLE_BUDGET: usize = 128;

/// Git objects are written in chunks, flushed once either bound is reached,
/// to amortize the blocking-thread hops without letting large blobs pile up
/// in memory: the byte bound caps resident data whatever the object sizes.
const GIT_WRITE_CHUNK: usize = 256;
const GIT_WRITE_BYTES: usize = 32 << 20;

/// Upper bound on op frames accepted in one fetch, against runaway
/// streams. Far above any real op log delta.
const MAX_OP_FRAMES: usize = 1 << 20;

/// Fetches the given op heads from a peer over the stream pair and applies
/// them locally in crash-safe order.
pub async fn fetch(
    repo: &Arc<MeshRepo>,
    name: &str,
    repo_id: &RepoId,
    wants: &[OperationId],
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<FetchOutcome> {
    ensure!(!wants.is_empty() && wants.len() <= MAX_WANTS, "bad wants");
    let local_heads = repo.op_heads().await?;
    let haves = sample_haves(repo, &local_heads).await?;

    let request = FetchRequest {
        name: name.to_owned(),
        id: repo_id.clone(),
        wants: wants.iter().map(|id| id.as_bytes().to_vec()).collect(),
        haves: haves.iter().map(|id| id.as_bytes().to_vec()).collect(),
    };
    write_message(send, &request, MAX_OP_FRAME_SIZE).await?;

    let batch = receive_ops(repo, wants, recv).await?;
    let ops_received = batch.ops.len();

    // Everything the new views (and op predecessor records) reference and
    // the local git store lacks is requested from the peer.
    let referenced = referenced_commits(&batch);
    let (missing, git_haves) = {
        let repo = repo.clone();
        let local_heads = local_heads.clone();
        tokio::task::spawn_blocking(move || -> Result<_> {
            let git = repo.git_backend().git_repo();
            let mut missing: Vec<CommitId> = Vec::new();
            for id in referenced {
                // jj's virtual root commit is never a real git object.
                if id.as_bytes().iter().all(|byte| *byte == 0) {
                    continue;
                }
                if !git.has_object(to_gix_id(&id)?) {
                    missing.push(id);
                }
            }
            let mut git_haves: Vec<CommitId> = Vec::new();
            for head in &local_heads {
                let op = repo.read_operation(head).block_on()?;
                let view = repo.read_view(&op.view_id).block_on()?;
                git_haves.extend(view.head_ids.iter().cloned());
            }
            git_haves.sort_unstable();
            git_haves.dedup();
            git_haves.truncate(MAX_GIT_HAVES);
            Ok((missing, git_haves))
        })
        .await
        .wrap_err("git check task failed")??
    };
    let git_request = GitRequest {
        wants: missing.iter().map(|id| id.as_bytes().to_vec()).collect(),
        haves: git_haves.iter().map(|id| id.as_bytes().to_vec()).collect(),
    };
    write_message(send, &git_request, MAX_GIT_FRAME_SIZE).await?;
    let git_objects = receive_git_objects(repo, recv).await?;

    // Nothing threw: objects are on disk, batch is closed and verified.
    let published = {
        let repo = repo.clone();
        let wants = wants.to_vec();
        tokio::task::spawn_blocking(move || apply(&repo, &batch, &wants, &local_heads))
            .await
            .wrap_err("apply task failed")??
    };

    Ok(FetchOutcome {
        published,
        ops: ops_received,
        git_objects,
    })
}

/// Receives and validates the op phase: every frame must decode as jj's
/// proto schema, every op's parents and view must be part of the batch or
/// already stored, and every want must be covered.
async fn receive_ops(
    repo: &Arc<MeshRepo>,
    wants: &[OperationId],
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<OpBatch> {
    let id_len = repo.root_operation_id().as_bytes().len();
    let mut ops: Vec<StoredOp> = Vec::new();
    let mut views: Vec<StoredView> = Vec::new();
    let mut op_ids: HashSet<OperationId> = HashSet::new();
    let mut view_ids: HashSet<ViewId> = HashSet::new();

    for _ in 0..MAX_OP_FRAMES {
        match read_message(recv, MAX_OP_FRAME_SIZE).await? {
            OpFrame::View { id, view } => {
                ensure!(id.len() == id_len, "bad view id length ({})", id.len());
                let id = ViewId::new(id);
                let meta =
                    codec::parse_view(&view).wrap_err_with(|| format!("view {}", id.hex()))?;
                if view_ids.insert(id.clone()) {
                    views.push(StoredView {
                        id,
                        bytes: view,
                        meta,
                    });
                }
            }
            OpFrame::Op { id, op } => {
                ensure!(id.len() == id_len, "bad op id length ({})", id.len());
                let id = OperationId::new(id);
                ensure!(
                    id != *repo.root_operation_id(),
                    "peer sent an op claiming the root id",
                );
                let meta =
                    codec::parse_operation(&op).wrap_err_with(|| format!("op {}", id.hex()))?;
                ensure!(!op_ids.contains(&id), "op {} sent twice", id.hex());
                for parent in &meta.parents {
                    ensure!(
                        op_ids.contains(parent)
                            || parent == repo.root_operation_id()
                            || repo.has_operation(parent).await?,
                        "op {} arrived before its parent {}",
                        id.hex(),
                        parent.hex(),
                    );
                }
                ensure!(
                    view_ids.contains(&meta.view_id) || repo.has_view(&meta.view_id).await?,
                    "op {} references unknown view",
                    id.hex(),
                );
                op_ids.insert(id.clone());
                ops.push(StoredOp {
                    id,
                    bytes: op,
                    meta,
                });
            }
            OpFrame::Done => {
                for want in wants {
                    ensure!(
                        op_ids.contains(want) || repo.has_operation(want).await?,
                        "peer did not send wanted op {}",
                        want.hex(),
                    );
                }
                let batch = OpBatch { ops, views };
                ensure_batch_reachable(wants, &batch)?;
                return Ok(batch);
            }
            OpFrame::Error { message } => {
                bail!("peer refused fetch: {}", sanitize(&message))
            }
        }
    }
    bail!("too many op frames");
}

/// Rejects batches containing ops not on any want's ancestry. An honest
/// server only sends ancestors of the wants; anything else has no business
/// in the batch, and a fabricated op claiming a local head as its parent
/// could otherwise poison the supersession computation in [`apply`].
fn ensure_batch_reachable(wants: &[OperationId], batch: &OpBatch) -> Result<()> {
    let by_id = batch.ops_by_id();
    let mut reachable: HashSet<&OperationId> = HashSet::new();
    let mut stack: Vec<&OperationId> = wants
        .iter()
        .filter(|want| by_id.contains_key(want))
        .collect();

    while let Some(id) = stack.pop() {
        if reachable.insert(id)
            && let Some(meta) = by_id.get(id)
        {
            stack.extend(
                meta.parents
                    .iter()
                    .filter(|parent| by_id.contains_key(*parent)),
            );
        }
    }

    ensure!(
        reachable.len() == batch.ops.len(),
        "batch contains {} ops unreachable from the wants",
        batch.ops.len() - reachable.len(),
    );
    Ok(())
}

/// Every commit id the batch references: view heads, all ref targets
/// (including conflict sides and legacy encodings), working copies, and
/// predecessor records.
fn referenced_commits(batch: &OpBatch) -> Vec<CommitId> {
    let mut ids: HashSet<CommitId> = HashSet::new();
    for view in &batch.views {
        ids.extend(view.meta.referenced_commits.iter().cloned());
    }
    for op in &batch.ops {
        ids.extend(op.meta.referenced_commits.iter().cloned());
    }
    ids.into_iter().collect()
}

/// Receives the git phase, hash-verifying every object and writing them
/// loose in chunks. Returns how many objects the peer sent.
async fn receive_git_objects(
    repo: &Arc<MeshRepo>,
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<usize> {
    let hash_kind = repo.git_backend().git_repo().object_hash();
    let mut chunk: Vec<(gix::ObjectId, gix::object::Kind, Vec<u8>)> = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut total = 0usize;

    // The previous chunk writes to disk while the next one streams in;
    // awaiting it only when the next chunk is full keeps the network and
    // the blocking writer pipelined.
    let mut pending: Option<tokio::task::JoinHandle<Result<()>>> = None;

    loop {
        match read_message(recv, MAX_GIT_FRAME_SIZE).await? {
            GitFrame::Object { id, kind, data } => {
                let id = gix::ObjectId::try_from(id.as_slice())
                    .map_err(|err| eyre!("bad object id: {err}"))?;
                let kind = to_gix_kind(kind);
                let computed = gix::objs::compute_hash(hash_kind, kind, &data)
                    .map_err(|err| eyre!("cannot hash object: {err}"))?;
                ensure!(
                    computed == id,
                    "object {id} does not match its content (hashes to {computed})",
                );
                chunk_bytes += data.len();
                chunk.push((id, kind, data));
                total += 1;
                if chunk.len() >= GIT_WRITE_CHUNK || chunk_bytes >= GIT_WRITE_BYTES {
                    if let Some(write) = pending.take() {
                        write.await.wrap_err("git write task failed")??;
                    }
                    pending = Some(write_git_chunk(repo, std::mem::take(&mut chunk)));
                    chunk_bytes = 0;
                }
            }
            GitFrame::Done => {
                if let Some(write) = pending.take() {
                    write.await.wrap_err("git write task failed")??;
                }
                write_git_chunk(repo, chunk)
                    .await
                    .wrap_err("git write task failed")??;
                return Ok(total);
            }
            GitFrame::Error { message } => {
                bail!("peer failed git phase: {}", sanitize(&message))
            }
        }
    }
}

/// Writes a chunk of verified objects into the loose odb on a blocking
/// thread, skipping objects already present. The ids were hash-verified
/// against the data on receipt, so the write reuses them instead of
/// hashing every object a second time.
fn write_git_chunk(
    repo: &Arc<MeshRepo>,
    chunk: Vec<(gix::ObjectId, gix::object::Kind, Vec<u8>)>,
) -> tokio::task::JoinHandle<Result<()>> {
    let repo = repo.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use gix::prelude::Write as _;
        let git = repo.git_backend().git_repo();
        for (id, kind, data) in chunk {
            if git.has_object(id) {
                continue;
            }
            git.objects
                .write_buf_with_known_id(kind, &data, id)
                .map_err(|err| eyre!("cannot write object {id}: {err}"))?;
        }
        Ok(())
    })
}

/// Samples haves for a fetch: local heads plus ancestors at exponentially
/// growing first-parent distances, bounding redundant transfer even when
/// histories diverged long ago.
async fn sample_haves(repo: &MeshRepo, heads: &[OperationId]) -> Result<Vec<OperationId>> {
    let mut haves: Vec<OperationId> = heads.to_vec();
    let mut budget = SAMPLE_BUDGET;

    for head in heads {
        let mut current = head.clone();
        let mut depth = 0usize;
        let mut next_sample = 1usize;
        while budget > 0 {
            let op = repo.read_operation(&current).await?;
            let Some(parent) = op.parents.first() else {
                break;
            };
            if parent == repo.root_operation_id() {
                break;
            }
            current = parent.clone();
            depth += 1;
            budget -= 1;
            if depth == next_sample {
                haves.push(current.clone());
                next_sample *= 2;
            }
        }
    }

    haves.sort_unstable();
    haves.dedup();
    haves.truncate(crate::net::sync::MAX_HAVES);
    Ok(haves)
}

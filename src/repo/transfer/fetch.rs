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

use super::{
    FetchOutcome, OpBatch, ProgressSink, RepoIdent, StoredOp, StoredView, TransferPhase,
    TransferProgress, apply::apply, is_virtual_root, pack, to_gix_id,
};
use crate::{
    config::sanitize,
    net::{
        sync::{
            FetchRequest, GitFrame, GitRequest, GitTransferFormat, MAX_GIT_FRAME_SIZE,
            MAX_GIT_HAVES, MAX_GIT_OBJECT_SIZE, MAX_HAVES, MAX_OP_FRAME_SIZE, MAX_WANTS, OpFrame,
            WireObjectKind, decompress_payload,
        },
        wire::{read_message, write_message},
    },
    repo::{OpenRepo, codec},
};

/// Read budget when sampling have-ancestors for a fetch request.
const SAMPLE_BUDGET: usize = 128;

/// Git objects are written in chunks, flushed once either bound is reached,
/// to amortize the blocking-thread hops without letting large blobs pile up
/// in memory: the byte bound caps resident (compressed) data whatever the
/// object sizes; decompressed bytes exist one object at a time.
const GIT_WRITE_CHUNK: usize = 256;
const GIT_WRITE_BYTES: usize = 32 << 20;

/// Upper bound on op frames accepted in one fetch, against runaway
/// streams. Far above any real op log delta.
const MAX_OP_FRAMES: usize = 1 << 20;

/// How often the pack ingest's object count is sampled for progress once
/// the stream ended and only local indexing remains.
const INGEST_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// Fetches the given op heads from a peer over the stream pair and applies
/// them locally in crash-safe order. `format` picks how git objects travel
/// (clones request a pack, incremental syncs stay loose, see
/// [`GitTransferFormat`]); `progress` receives live counters as the phases
/// advance.
pub async fn fetch(
    repo: &Arc<OpenRepo>,
    target: RepoIdent<'_>,
    wants: &[OperationId],
    format: GitTransferFormat,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<FetchOutcome> {
    ensure!(!wants.is_empty() && wants.len() <= MAX_WANTS, "bad wants");
    let local_heads = repo.op_heads().await?;
    let haves = sample_haves(repo, &local_heads).await?;

    let request = FetchRequest {
        name: target.name.to_owned(),
        id: target.id.clone(),
        wants: wants.iter().map(|id| id.as_bytes().to_vec()).collect(),
        haves: haves.iter().map(|id| id.as_bytes().to_vec()).collect(),
    };
    write_message(send, &request, MAX_OP_FRAME_SIZE).await?;

    let batch = receive_ops(repo, wants, recv, progress).await?;
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
                if is_virtual_root(&id) {
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
    // A pack of nothing is not a valid transfer; an empty want set falls
    // back to the loose exchange, which the server answers with `Done`.
    let format = if missing.is_empty() {
        GitTransferFormat::Loose
    } else {
        format
    };
    let git_request = GitRequest {
        wants: missing.iter().map(|id| id.as_bytes().to_vec()).collect(),
        haves: git_haves.iter().map(|id| id.as_bytes().to_vec()).collect(),
        format,
    };
    write_message(send, &git_request, MAX_GIT_FRAME_SIZE).await?;
    // The keep guard holds a received pack against git GC until the apply
    // writes the keep refs that take over; it is released below, and on any
    // early return or abandoned fetch by its own drop.
    let (git_objects, pack_keep) = match format {
        GitTransferFormat::Loose => (receive_git_loose(repo, recv, progress).await?, None),
        GitTransferFormat::Pack => {
            let (objects, keep) = receive_git_pack(repo, recv, progress).await?;
            (objects, Some(keep))
        }
    };

    // Nothing threw: objects are on disk, batch is closed and verified.
    progress.report(TransferProgress::start(TransferPhase::Apply));
    let published = {
        let repo = repo.clone();
        let wants = wants.to_vec();
        tokio::task::spawn_blocking(move || apply(&repo, &batch, &wants, &local_heads))
            .await
            .wrap_err("apply task failed")??
    };
    // The apply's keep refs now protect the pack's commits.
    drop(pack_keep);

    // Index the published heads, so the next jj command loads the commit
    // index instead of building it (which after a clone means indexing the
    // entire replicated history). Best-effort: jj rebuilds lazily anyway,
    // so a failure here must not fail an otherwise complete fetch.
    if !published.is_empty() {
        let repo = repo.clone();
        let build = tokio::task::spawn_blocking(move || {
            for head in &published {
                if let Err(err) = repo.build_commit_index(head) {
                    tracing::warn!("cannot index synced operation: {err:#}");
                }
            }
        });
        if let Err(err) = build.await {
            tracing::warn!("index build task failed: {err}");
        }
    }

    Ok(FetchOutcome {
        ops: ops_received,
        git_objects,
    })
}

/// Receives and validates the op phase: every frame must decode as jj's
/// proto schema, every op's parents and view must be part of the batch or
/// already stored, and every want must be covered.
async fn receive_ops(
    repo: &Arc<OpenRepo>,
    wants: &[OperationId],
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<OpBatch> {
    let id_len = repo.root_operation_id().as_bytes().len();
    let mut ops: Vec<StoredOp> = Vec::new();
    let mut views: Vec<StoredView> = Vec::new();
    let mut op_ids: HashSet<OperationId> = HashSet::new();
    let mut view_ids: HashSet<ViewId> = HashSet::new();

    let mut counters = TransferProgress::start(TransferPhase::Ops);
    progress.report(counters);

    // Protocol position, kept apart from the display counters above.
    let mut begun = false;
    let mut data_seen = false;

    for _ in 0..MAX_OP_FRAMES {
        match read_message(recv, MAX_OP_FRAME_SIZE).await? {
            OpFrame::Begin {
                ops: total_ops,
                views: total_views,
            } => {
                ensure!(!begun, "peer sent Begin twice");
                ensure!(!data_seen, "peer sent Begin after data");
                begun = true;
                counters.total = Some(total_ops.saturating_add(total_views));
                progress.report(counters);
            }
            OpFrame::View { id, view } => {
                data_seen = true;
                counters.current += 1;
                counters.bytes += view.len() as u64;
                progress.report(counters);
                ensure!(id.len() == id_len, "bad view id length ({})", id.len());
                let id = ViewId::new(id);
                let view = decompress_payload(&view, u64::from(MAX_OP_FRAME_SIZE))
                    .wrap_err_with(|| format!("view {}", id.hex()))?;
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
                data_seen = true;
                counters.current += 1;
                counters.bytes += op.len() as u64;
                progress.report(counters);
                ensure!(id.len() == id_len, "bad op id length ({})", id.len());
                let id = OperationId::new(id);
                ensure!(
                    id != *repo.root_operation_id(),
                    "peer sent an op claiming the root id",
                );
                let op = decompress_payload(&op, u64::from(MAX_OP_FRAME_SIZE))
                    .wrap_err_with(|| format!("op {}", id.hex()))?;
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

/// Receives the git phase, decompressing and hash-verifying every object
/// and writing them loose in chunks. Returns how many objects the peer
/// sent.
async fn receive_git_loose(
    repo: &Arc<OpenRepo>,
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<usize> {
    let mut chunk: Vec<(gix::ObjectId, gix::object::Kind, Vec<u8>)> = Vec::new();
    let mut chunk_bytes = 0usize;
    let mut total = 0usize;

    let mut counters = TransferProgress::start(TransferPhase::Git);
    progress.report(counters);

    // The previous chunk writes to disk while the next one streams in;
    // awaiting it only when the next chunk is full keeps the network and
    // the blocking writer pipelined. Decompression and hashing happen on
    // the writer's blocking thread, one object at a time.
    let mut pending: Option<tokio::task::JoinHandle<Result<()>>> = None;

    loop {
        match read_message(recv, MAX_GIT_FRAME_SIZE).await? {
            GitFrame::Object { id, kind, data } => {
                counters.current += 1;
                counters.bytes += data.len() as u64;
                progress.report(counters);
                let id = gix::ObjectId::try_from(id.as_slice())
                    .map_err(|err| eyre!("bad object id: {err}"))?;
                let kind = to_gix_kind(kind);
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
            GitFrame::Pack { .. } => bail!("peer sent a pack chunk in a loose transfer"),
            GitFrame::Error { message } => {
                bail!("peer failed git phase: {}", sanitize(&message))
            }
        }
    }
}

/// Receives the git phase in the pack format: frames stream one packfile
/// into a blocking ingest task, which indexes it into `objects/pack` (see
/// [`pack::ingest_pack`] for the verification done there). Returns how many
/// objects the pack carried and the `.keep` file protecting it.
///
/// Progress: the phase total is read off the pack header (its exact object
/// count), `current` tracks the ingest's objects-indexed counter, sampled
/// on every received chunk and, once the stream ends, on a timer while
/// the ingest finishes resolving.
async fn receive_git_pack(
    repo: &Arc<OpenRepo>,
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<(usize, pack::PackKeep)> {
    let mut counters = TransferProgress::start(TransferPhase::Git);
    progress.report(counters);

    let indexed = pack::SharedObjectCount::default();
    let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(8);
    let mut ingest = {
        let repo = repo.clone();
        let indexed = indexed.clone();
        tokio::task::spawn_blocking(move || {
            let git = repo.git_backend().git_repo();
            pack::ingest_pack(&git, pack::ChunkReader::new(rx), &indexed)
        })
    };

    let mut failed = None;
    loop {
        match read_message(recv, MAX_GIT_FRAME_SIZE).await? {
            GitFrame::Pack { chunk } => {
                // The first chunk starts with the pack header, whose object
                // count is the exact phase total. Display only: the ingest
                // decodes and enforces the header itself.
                if counters.bytes == 0 && chunk.len() >= 12 {
                    let mut header = [0u8; 12];
                    header.copy_from_slice(&chunk[..12]);
                    if let Ok((_version, objects)) = gix::odb::pack::data::header::decode(&header) {
                        counters.total = Some(u64::from(objects));
                    }
                }
                counters.bytes += chunk.len() as u64;
                counters.current = indexed.get();
                progress.report(counters);
                // A closed channel means the ingest died; its own error is
                // reported after the join below.
                if tx.send(chunk).await.is_err() {
                    break;
                }
            }
            GitFrame::Done => break,
            GitFrame::Error { message } => {
                failed = Some(format!("peer failed git phase: {}", sanitize(&message)));
                break;
            }
            GitFrame::Object { .. } => {
                failed = Some("peer sent a loose object in a pack transfer".to_owned());
                break;
            }
        }
    }

    // Closing the channel ends the ingest's stream; on a clean `Done` it
    // finishes the pack, otherwise it fails on the truncation. Indexing
    // (delta resolution most of all) keeps running after the last byte
    // arrived, so keep sampling its object count while waiting.
    drop(tx);
    let outcome = loop {
        tokio::select! {
            outcome = &mut ingest => break outcome.wrap_err("pack ingest task failed")?,
            () = tokio::time::sleep(INGEST_SAMPLE_INTERVAL) => {
                counters.current = indexed.get();
                progress.report(counters);
            }
        }
    };
    if let Some(message) = failed {
        bail!("{message}");
    }
    let outcome = outcome?;
    counters.current = indexed.get();
    progress.report(counters);
    Ok((outcome.objects, outcome.keep))
}

/// Decompresses, hash-verifies and writes a chunk of received objects into
/// the loose odb on a blocking thread, skipping objects already present.
/// Verifying establishes the id, so the write reuses it instead of hashing
/// a second time. An object that lies about its id aborts the fetch;
/// earlier objects of the chunk may already be written, which is harmless
/// (each was verified, and nothing references them until the apply
/// publishes).
fn write_git_chunk(
    repo: &Arc<OpenRepo>,
    chunk: Vec<(gix::ObjectId, gix::object::Kind, Vec<u8>)>,
) -> tokio::task::JoinHandle<Result<()>> {
    let repo = repo.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use gix::prelude::Write as _;
        let git = repo.git_backend().git_repo();
        let hash_kind = git.object_hash();
        for (id, kind, data) in chunk {
            let data = decompress_payload(&data, MAX_GIT_OBJECT_SIZE)
                .wrap_err_with(|| format!("object {id}"))?;
            let computed = gix::objs::compute_hash(hash_kind, kind, &data)
                .map_err(|err| eyre!("cannot hash object: {err}"))?;
            ensure!(
                computed == id,
                "object {id} does not match its content (hashes to {computed})",
            );
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
async fn sample_haves(repo: &OpenRepo, heads: &[OperationId]) -> Result<Vec<OperationId>> {
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
    haves.truncate(MAX_HAVES);
    Ok(haves)
}

/// Maps a wire object kind to its gix equivalent.
fn to_gix_kind(kind: WireObjectKind) -> gix::object::Kind {
    match kind {
        WireObjectKind::Commit => gix::object::Kind::Commit,
        WireObjectKind::Tree => gix::object::Kind::Tree,
        WireObjectKind::Blob => gix::object::Kind::Blob,
        WireObjectKind::Tag => gix::object::Kind::Tag,
    }
}

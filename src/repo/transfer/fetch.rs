//! Fetcher side of a fetch: requesting the op-log delta, validating it,
//! pulling the git objects it references, then staging it, indexing the
//! incoming heads and publishing them (see [`super::apply`]).

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OperationId, ViewId},
};
use pollster::FutureExt as _;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};

use super::{
    FetchOptions, FetchOutcome, OpBatch, ProgressSink, StoredOp, StoredView, TransferPhase,
    TransferProgress, apply, pack, to_gix_id,
};
use crate::{
    config::{RepoId, sanitize},
    net::{
        fetch::{
            FetchRequest, GitFrame, GitRequest, GitTransferFormat, MAX_GIT_FRAME_SIZE, OpFrame,
            PhaseDecompressor, WireObjectKind, decompress_object,
        },
        wire::{MAX_FRAME_SIZE, read_message, write_message},
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

/// Cap on the op haves sampled for a fetch request.
const MAX_HAVES: usize = 256;

/// Cap on the commit haves sent in the git phase (our view heads, then the
/// commits the fetched ops reference that we hold).
const MAX_GIT_HAVES: usize = 4096;

/// How often the pack ingest's object count is sampled for progress once
/// the stream ended and only local indexing remains.
const INGEST_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

/// Fetches the given op heads from a peer over the stream pair and applies
/// them locally in crash-safe order. `options.format` asks how git objects
/// travel (clones ask for the store, incremental syncs for loose objects, see
/// [`GitTransferFormat`]); `progress` receives live counters as the phases
/// advance.
///
/// `options.net_timeout` sets one deadline for all network-facing work
/// (the request writes, the op frames, the git object or pack streaming)
/// so a stalled peer cannot pin the caller past it. Local work
/// — pack delta resolution, the apply, the index build — runs unbounded,
/// since timing it out after the transfer would only waste the received
/// data.
pub async fn fetch(
    repo: &Arc<OpenRepo>,
    target: &RepoId,
    wants: &[OperationId],
    options: FetchOptions,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<FetchOutcome> {
    ensure!(!wants.is_empty(), "nothing to fetch");
    let deadline = tokio::time::Instant::now() + options.net_timeout;
    let local_heads = repo.op_heads().await?;
    let haves = sample_haves(repo, &local_heads).await?;

    let batch = bounded(deadline, "op", async {
        let request = FetchRequest {
            id: target.clone(),
            wants: wants.iter().map(|id| id.as_bytes().to_vec()).collect(),
            haves: haves.iter().map(|id| id.as_bytes().to_vec()).collect(),
        };
        write_message(send, &request, MAX_FRAME_SIZE).await?;
        receive_ops(repo, wants, recv, progress).await
    })
    .await?;
    let ops_received = batch.ops.len();

    let (missing, git_haves) = {
        let repo = repo.clone();
        let local_heads = local_heads.clone();
        let referenced = referenced_commits(&batch);
        crate::spawn_blocking(move || git_wants(&repo, &local_heads, referenced))
            .await
            .context("git check task failed")??
    };
    let git_request = GitRequest {
        wants: missing.iter().map(|id| id.as_bytes().to_vec()).collect(),
        haves: git_haves.iter().map(|id| id.as_bytes().to_vec()).collect(),
        format: options.format,
    };
    bounded(
        deadline,
        "git",
        write_message(send, &git_request, MAX_FRAME_SIZE),
    )
    .await?;
    // The keep guards hold received packs against git GC until the stage
    // writes the keep refs that take over; they are released below, and on
    // any early return or abandoned fetch by their own drop.
    let (git_objects, pack_keeps) = receive_git(repo, deadline, recv, progress).await?;

    ensure_git_wants_arrived(repo, missing).await?;

    // Nothing threw: objects are on disk, batch is closed and verified.
    progress.report(TransferProgress::start(TransferPhase::Apply));
    // Heads to publish (wants new to this repo) whose commit index must
    // be built first, so the user's next jj command finds the replicated
    // history indexed instead of building the index itself — which after
    // a clone covers the entire history, silently and for minutes.
    // Stat'd here because progress cannot be reported from the blocking
    // task below.
    let new_heads: Vec<OperationId> = wants
        .iter()
        .filter(|want| !local_heads.contains(want))
        .cloned()
        .collect();
    let to_index = repo.unindexed(&new_heads).await;
    if !to_index.is_empty() {
        progress.report(TransferProgress::start(TransferPhase::Index));
    }

    // One blocking task from staging to publication: a started blocking
    // task always runs to completion, so an abandoned fetch (daemon
    // shutdown, clone client gone) cannot persist ops without publishing
    // their heads. A crash can: `OpenRepo::missing_heads` then reports
    // the head as missing and the next fetch publishes it. The index build
    // is best-effort: op data is valid without it, and the watch-start
    // heal retries it.
    {
        let repo = repo.clone();
        let wants = wants.to_vec();
        crate::spawn_blocking(move || apply_batch(&repo, &batch, &wants, &local_heads, &to_index))
            .await
            .context("apply task failed")??;
    }
    // The stage's keep refs now protect the packs' commits.
    drop(pack_keeps);

    Ok(FetchOutcome {
        ops: ops_received,
        git_objects,
    })
}

/// Stages, indexes and publishes a fetched batch (see [`apply`]). Blocking.
fn apply_batch(
    repo: &Arc<OpenRepo>,
    batch: &OpBatch,
    wants: &[OperationId],
    local_heads: &[OperationId],
    to_index: &[OperationId],
) -> Result<()> {
    fail::fail_point!("fetch.before_stage", |_| Err(anyhow!(
        "crash point fetch.before_stage"
    )));
    let staged = apply::stage(repo, batch, wants, local_heads)?;
    repo.build_commit_indexes(to_index);
    fail::fail_point!("fetch.after_index", |_| Err(anyhow!(
        "crash point fetch.after_index"
    )));
    apply::publish(repo, &staged)
}

/// Fails the fetch when any of the `requested` commits is still absent
/// after the git phase: published, such a head could never be loaded or
/// indexed. A server stores everything its op log references, so absence
/// is a hard failure (the announcement retry machinery covers
/// transients). One sweep on a blocking thread.
async fn ensure_git_wants_arrived(repo: &Arc<OpenRepo>, requested: Vec<CommitId>) -> Result<()> {
    if requested.is_empty() {
        return Ok(());
    }
    let repo = repo.clone();
    let absent = crate::spawn_blocking(move || -> Result<Vec<CommitId>> {
        let git = repo.git_backend().git_repo();
        let mut absent = Vec::new();
        for id in requested {
            if !git.has_object(to_gix_id(&id)?) {
                absent.push(id);
            }
        }
        Ok(absent)
    })
    .await
    .context("git verify task failed")??;
    if let Some(first) = absent.first() {
        bail!(
            "peer did not send {} of the requested commits (e.g. {})",
            absent.len(),
            first.hex(),
        );
    }
    Ok(())
}

/// Runs one network-facing section under the fetch's shared deadline.
async fn bounded<T>(
    deadline: tokio::time::Instant,
    phase: &str,
    section: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout_at(deadline, section)
        .await
        .map_err(|_| anyhow!("{phase} phase timed out"))?
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

    let mut decompressor = PhaseDecompressor::new()?;

    loop {
        match read_message(recv, MAX_FRAME_SIZE).await? {
            OpFrame::Begin {
                ops: total_ops,
                views: total_views,
            } => {
                ensure!(counters.total.is_none(), "peer sent Begin twice");
                counters.total = Some(total_ops.saturating_add(total_views));
                progress.report(counters);
            }
            OpFrame::View { id, view } => {
                count_frame(&mut counters)?;
                counters.bytes += view.len() as u64;
                progress.report(counters);
                ensure!(id.len() == id_len, "bad view id length ({})", id.len());
                let id = ViewId::new(id);
                let context = || format!("view {}", id.hex());
                let view = decompressor.decompress(&view).with_context(context)?;
                let meta = codec::parse_view(&view).with_context(context)?;
                if view_ids.insert(id.clone()) {
                    views.push(StoredView {
                        id,
                        bytes: view,
                        meta,
                    });
                }
            }
            OpFrame::Op { id, op } => {
                count_frame(&mut counters)?;
                counters.bytes += op.len() as u64;
                progress.report(counters);
                ensure!(id.len() == id_len, "bad op id length ({})", id.len());
                let id = OperationId::new(id);
                ensure!(
                    id != *repo.root_operation_id(),
                    "peer sent an op claiming the root id",
                );
                let context = || format!("op {}", id.hex());
                let op = decompressor.decompress(&op).with_context(context)?;
                let meta = codec::parse_operation(&op).with_context(context)?;
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
                return Ok(OpBatch { ops, views });
            }
            OpFrame::Error { message } => {
                bail!("peer refused fetch: {}", sanitize(&message))
            }
        }
    }
}

/// Counts one data frame of a phase into `counters`, against the total
/// its `Begin` announced, which also stops a runaway stream.
fn count_frame(counters: &mut TransferProgress) -> Result<()> {
    let total = counters.total.context("peer sent data before Begin")?;
    ensure!(
        counters.current < total,
        "peer sent more frames than announced"
    );
    counters.current += 1;
    Ok(())
}

/// The commits to request from the peer and the haves to send it, from
/// the `referenced` commits of the new views (and op predecessor records):
/// those the local git store lacks are requested. The haves are our view
/// heads at `local_heads` (the server ignores those it lacks), then what
/// the views reference and we hold: the peer knows those, and the new
/// commits build on them. Blocking.
fn git_wants(
    repo: &OpenRepo,
    local_heads: &[OperationId],
    referenced: Vec<CommitId>,
) -> Result<(Vec<CommitId>, Vec<CommitId>)> {
    let git = repo.git_backend().git_repo();
    // Repos of different object hashes cannot share commits; the ids alone
    // tell, before anything is requested.
    let hash = git.object_hash();
    ensure!(
        referenced
            .iter()
            .all(|id| id.as_bytes().len() == hash.len_in_bytes()),
        "the peer's repo uses another git object hash than this one ({hash}); \
         repos of different hashes cannot sync",
    );
    let mut git_haves: Vec<CommitId> = Vec::new();
    for head in local_heads {
        let op = repo.read_operation(head).block_on()?;
        let view = repo.read_view(&op.view_id).block_on()?;
        git_haves.extend(view.head_ids.iter().cloned());
    }
    let mut missing: Vec<CommitId> = Vec::new();
    for id in referenced {
        if repo.is_root_commit(&id) {
            continue;
        }
        if git.has_object(to_gix_id(&id)?) {
            git_haves.push(id);
        } else {
            missing.push(id);
        }
    }
    let mut unique = HashSet::new();
    git_haves.retain(|id| unique.insert(id.clone()));
    git_haves.truncate(MAX_GIT_HAVES);
    Ok((missing, git_haves))
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

/// Receives the git phase in the format the server announces. Returns how
/// many objects the peer sent, and the `.keep` files protecting the
/// received packs.
async fn receive_git(
    repo: &Arc<OpenRepo>,
    deadline: tokio::time::Instant,
    recv: &mut (impl AsyncRead + Unpin),
    progress: ProgressSink<'_>,
) -> Result<(usize, Vec<pack::PackKeep>)> {
    let first = bounded(deadline, "git", read_message(recv, MAX_GIT_FRAME_SIZE)).await?;
    let (format, objects) = match first {
        GitFrame::Begin { format, objects } => (format, objects),
        GitFrame::Done => return Ok((0, Vec::new())),
        GitFrame::Error { message } => {
            bail!("peer failed git phase: {}", sanitize(&message))
        }
        GitFrame::Object { .. } | GitFrame::Pack { .. } | GitFrame::NextPack => {
            bail!("peer sent objects before Begin")
        }
    };
    let mut counters = TransferProgress::start(TransferPhase::Git);
    counters.total = Some(objects);
    progress.report(counters);
    if format == GitTransferFormat::Loose {
        let received = bounded(
            deadline,
            "git",
            receive_git_loose(repo, recv, counters, progress),
        )
        .await?;
        return Ok((received, Vec::new()));
    }
    let store = format == GitTransferFormat::Store;
    receive_git_packs(repo, deadline, recv, counters, progress, store).await
}

/// Receives the git phase, decompressing and hash-verifying every object
/// and writing them loose in chunks. Returns how many objects the peer
/// sent.
async fn receive_git_loose(
    repo: &Arc<OpenRepo>,
    recv: &mut (impl AsyncRead + Unpin),
    mut counters: TransferProgress,
    progress: ProgressSink<'_>,
) -> Result<usize> {
    let mut chunk: Vec<(gix::ObjectId, gix::object::Kind, Vec<u8>)> = Vec::new();
    let mut chunk_bytes = 0usize;

    // The previous chunk writes to disk while the next one streams in;
    // awaiting it only when the next chunk is full keeps the network and
    // the blocking writer pipelined. Decompression and hashing happen on
    // the writer's blocking thread, one object at a time.
    let mut pending: Option<tokio::task::JoinHandle<Result<()>>> = None;

    loop {
        match read_message(recv, MAX_GIT_FRAME_SIZE).await? {
            GitFrame::Object { id, kind, data } => {
                count_frame(&mut counters)?;
                counters.bytes += data.len() as u64;
                progress.report(counters);
                let id = gix::ObjectId::try_from(id.as_slice())
                    .map_err(|err| anyhow!("bad object id: {err}"))?;
                let kind = to_gix_kind(kind);
                chunk_bytes += data.len();
                chunk.push((id, kind, data));
                if chunk.len() >= GIT_WRITE_CHUNK || chunk_bytes >= GIT_WRITE_BYTES {
                    if let Some(write) = pending.take() {
                        write.await.context("git write task failed")??;
                    }
                    pending = Some(write_git_chunk(repo, std::mem::take(&mut chunk)));
                    chunk_bytes = 0;
                }
            }
            GitFrame::Done => {
                if let Some(write) = pending.take() {
                    write.await.context("git write task failed")??;
                }
                write_git_chunk(repo, chunk)
                    .await
                    .context("git write task failed")??;
                return Ok(usize::try_from(counters.current)?);
            }
            GitFrame::Begin { .. } => bail!("peer sent Begin twice"),
            GitFrame::Pack { .. } | GitFrame::NextPack => {
                bail!("peer sent a pack chunk in a loose transfer")
            }
            GitFrame::Error { message } => {
                bail!("peer failed git phase: {}", sanitize(&message))
            }
        }
    }
}

/// Receives the git phase in the pack format, or the store format when
/// `store` (several packs, separated by `NextPack`). Frames stream each
/// packfile into its blocking ingest task, which indexes it into
/// `objects/pack` (see [`pack::ingest_pack`] for the verification done
/// there), while the next pack keeps streaming: the server waits on our
/// delta resolution only when two packs are already resolving. Returns how
/// many objects the packs carried and the `.keep` files protecting them.
///
/// Only the frame reads run under the fetch's network `deadline`: delta
/// resolution is local work, and timing it out would throw away fully
/// received packs.
///
/// Progress: `current` sums the ingests' indexed objects, then their
/// resolved objects in the [`TransferPhase::Resolve`] that follows the last
/// pack byte. It is sampled on every received chunk and on a timer while
/// waiting on an ingest.
async fn receive_git_packs(
    repo: &Arc<OpenRepo>,
    deadline: tokio::time::Instant,
    recv: &mut (impl AsyncRead + Unpin),
    mut counters: TransferProgress,
    progress: ProgressSink<'_>,
    store: bool,
) -> Result<(usize, Vec<pack::PackKeep>)> {
    let mut counts: Vec<pack::IngestCounts> = Vec::new();
    let mut running: VecDeque<Ingest> = VecDeque::new();
    let mut outcomes: Vec<pack::IngestOutcome> = Vec::new();
    let mut tx = start_ingest(repo, &mut counts, &mut running);
    loop {
        match bounded(deadline, "git", read_message(recv, MAX_GIT_FRAME_SIZE)).await? {
            GitFrame::Pack { chunk } => {
                counters.bytes += chunk.len() as u64;
                sample(&counts, &mut counters);
                progress.report(counters);
                // A closed channel means the ingest died: report its error.
                if tx.send(chunk).await.is_err() {
                    let ingest = running.pop_back().expect("the ingest fed by `tx`");
                    join_ingest(ingest, &counts, &mut counters, progress).await?;
                    bail!("pack ingest stopped early");
                }
            }
            GitFrame::NextPack if store => {
                // Replacing `tx` ends the previous pack's stream.
                tx = start_ingest(repo, &mut counts, &mut running);
                if running.len() > 2 {
                    let ingest = running.pop_front().expect("several ingests");
                    outcomes.push(join_ingest(ingest, &counts, &mut counters, progress).await?);
                }
            }
            GitFrame::NextPack => bail!("peer sent several packs in a pack transfer"),
            GitFrame::Done => break,
            GitFrame::Error { message } => {
                bail!("peer failed git phase: {}", sanitize(&message))
            }
            GitFrame::Begin { .. } => bail!("peer sent Begin twice"),
            GitFrame::Object { .. } => bail!("peer sent a loose object in a pack transfer"),
        }
    }
    drop(tx);
    counters = TransferProgress {
        total: counters.total,
        ..TransferProgress::start(TransferPhase::Resolve)
    };
    sample(&counts, &mut counters);
    progress.report(counters);
    for ingest in running {
        outcomes.push(join_ingest(ingest, &counts, &mut counters, progress).await?);
    }
    sample(&counts, &mut counters);
    progress.report(counters);
    let received = outcomes.iter().map(|outcome| outcome.objects).sum();
    let keeps = outcomes.into_iter().map(|outcome| outcome.keep).collect();
    Ok((received, keeps))
}

/// A received pack being indexed.
type Ingest = tokio::task::JoinHandle<Result<pack::IngestOutcome>>;

/// Starts indexing a pack on a blocking thread, tracking its object counts
/// in `counts`. Returns the sender of its chunks; dropping it ends the
/// pack.
fn start_ingest(
    repo: &Arc<OpenRepo>,
    counts: &mut Vec<pack::IngestCounts>,
    running: &mut VecDeque<Ingest>,
) -> mpsc::Sender<Vec<u8>> {
    let (tx, rx) = mpsc::channel(8);
    let ingest_counts = pack::IngestCounts::default();
    counts.push(ingest_counts.clone());
    let repo = repo.clone();
    running.push_back(crate::spawn_blocking(move || {
        let git = repo.git_backend().git_repo();
        pack::ingest_pack(&git, pack::ChunkReader::new(rx), &ingest_counts)
    }));
    tx
}

/// Waits for an ingest, sampling its progress: delta resolution keeps
/// running after the pack's last byte arrived.
async fn join_ingest(
    mut ingest: Ingest,
    counts: &[pack::IngestCounts],
    counters: &mut TransferProgress,
    progress: ProgressSink<'_>,
) -> Result<pack::IngestOutcome> {
    loop {
        tokio::select! {
            outcome = &mut ingest => return outcome.context("pack ingest task failed")?,
            () = tokio::time::sleep(INGEST_SAMPLE_INTERVAL) => {
                sample(counts, counters);
                progress.report(*counters);
            }
        }
    }
}

/// Sets `counters.current` from the ingests: objects indexed while the
/// packs stream in, resolved after.
fn sample(counts: &[pack::IngestCounts], counters: &mut TransferProgress) {
    let count = if counters.phase == TransferPhase::Resolve {
        pack::IngestCounts::resolved
    } else {
        pack::IngestCounts::indexed
    };
    counters.current = counts.iter().map(count).sum();
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
    crate::spawn_blocking(move || -> Result<()> {
        use gix::prelude::Write as _;
        let git = repo.git_backend().git_repo();
        let hash_kind = git.object_hash();
        for (id, kind, data) in chunk {
            let data = decompress_object(&data).with_context(|| format!("object {id}"))?;
            let computed = gix::objs::compute_hash(hash_kind, kind, &data)
                .map_err(|err| anyhow!("cannot hash object: {err}"))?;
            ensure!(
                computed == id,
                "object {id} does not match its content (hashes to {computed})",
            );
            if git.has_object(id) {
                continue;
            }
            git.objects
                .write_buf_with_known_id(kind, &data, id)
                .map_err(|err| anyhow!("cannot write object {id}: {err}"))?;
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

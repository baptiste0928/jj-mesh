//! The op and git object transfer engine: serving fetches and fetching.
//!
//! Both sides run over any `AsyncRead`/`AsyncWrite` pair (QUIC streams in
//! production, in-memory duplexes in tests) and follow the phase protocol
//! described in [`crate::net::sync`]. Ops and views travel as raw stored
//! bytes and keep their sender-side ids (see the `mesh` module docs for
//! why re-hashing them is impossible). Peer-supplied data is authenticated
//! but untrusted: op and view bytes are validated structurally before
//! anything is written, git objects are hash-verified before writing, and
//! replicated bytes can never replace already-stored objects.
//!
//! The apply side follows the crash-safe write order: git objects, anti-GC
//! keep refs, views and ops (parents first), change-id extras, the
//! colocated ref mirror, and only then the op head publication. Bulk store
//! and git work runs on blocking threads (see the `mesh` module docs).

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure, eyre};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OperationId, RefTarget, View, ViewId},
};
use pollster::FutureExt as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, info, warn};

use super::{
    MeshRepo,
    codec::{self, OpMeta, ViewMeta},
};
use crate::{
    config::RepoId,
    net::{
        sync::{
            FetchRequest, GitFrame, GitRequest, MAX_GIT_FRAME_SIZE, MAX_GIT_HAVES, MAX_HAVES,
            MAX_OP_FRAME_SIZE, MAX_WANTS, OpFrame, WireObjectKind,
        },
        wire::{read_message, write_message},
    },
};

/// Read budget when sampling have-ancestors for a fetch request.
const SAMPLE_BUDGET: usize = 128;

/// Op/view frames buffered between the server's blocking walk and the
/// stream writer. Small: it only smooths the pipeline, the point is to not
/// hold the whole delta at once.
const OP_STREAM_BUFFER: usize = 16;

/// Git objects are written in chunks, flushed once either bound is reached,
/// to amortize the blocking-thread hops without letting large blobs pile up
/// in memory: the byte bound caps resident data whatever the object sizes.
const GIT_WRITE_CHUNK: usize = 256;
const GIT_WRITE_BYTES: usize = 32 << 20;

/// Upper bound on op frames accepted in one fetch, against runaway
/// streams. Far above any real op log delta.
const MAX_OP_FRAMES: usize = 1 << 20;

/// What a completed fetch did, for logging and status.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Op heads published locally (empty when already up to date).
    pub published: Vec<OperationId>,
    pub ops: usize,
    pub git_objects: usize,
}

// --- Server side ---

/// Serves one fetch request over the given stream pair. Read-only on the
/// repo; errors are reported to the peer as protocol frames where the
/// exchange allows it.
pub async fn serve(
    repo: &Arc<MeshRepo>,
    request: FetchRequest,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<()> {
    if let Err(message) = validate_request(repo, &request) {
        write_message(send, &OpFrame::Error { message }, MAX_OP_FRAME_SIZE).await?;
        return Ok(());
    }
    let wants: Vec<OperationId> = request.wants.into_iter().map(OperationId::new).collect();
    let haves: Vec<OperationId> = request.haves.into_iter().map(OperationId::new).collect();

    for want in &wants {
        if !repo.has_operation(want).await? {
            let message = format!("unknown operation {}", want.hex());
            write_message(send, &OpFrame::Error { message }, MAX_OP_FRAME_SIZE).await?;
            return Ok(());
        }
    }

    // Stream the delta through a bounded channel so the whole op-log delta
    // never sits in memory at once: a join pulls the entire log. The walk is
    // bulk store I/O, so it runs on a blocking thread; each view is sent
    // before the first op referencing it, and ops stay parents-first.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<OpFrame>>(OP_STREAM_BUFFER);
    let walk = {
        let repo = repo.clone();
        tokio::task::spawn_blocking(move || {
            let outcome = (|| -> Result<()> {
                let ops = repo.ancestors_until(&wants, &haves).block_on()?;
                let mut sent_views: HashSet<ViewId> = HashSet::new();
                for (id, op) in ops {
                    if sent_views.insert(op.view_id.clone()) {
                        let view = repo.read_view_bytes(&op.view_id)?;
                        let frame = OpFrame::View {
                            id: op.view_id.as_bytes().to_vec(),
                            view,
                        };
                        tx.blocking_send(Ok(frame))
                            .map_err(|_| eyre!("fetcher went away"))?;
                    }
                    let bytes = repo.read_operation_bytes(&id)?;
                    let frame = OpFrame::Op {
                        id: id.as_bytes().to_vec(),
                        op: bytes,
                    };
                    tx.blocking_send(Ok(frame))
                        .map_err(|_| eyre!("fetcher went away"))?;
                }
                Ok(())
            })();
            if let Err(err) = outcome {
                let _ = tx.blocking_send(Err(err));
            }
        })
    };

    let mut op_count = 0usize;
    let mut failed = None;
    while let Some(frame) = rx.recv().await {
        match frame {
            Ok(frame) => {
                if matches!(frame, OpFrame::Op { .. }) {
                    op_count += 1;
                }
                write_message(send, &frame, MAX_OP_FRAME_SIZE).await?;
            }
            Err(err) => {
                failed = Some(format!("cannot collect operations: {err:#}"));
                break;
            }
        }
    }
    walk.await.wrap_err("fetch serve task failed")?;

    if let Some(message) = failed {
        write_message(send, &OpFrame::Error { message }, MAX_OP_FRAME_SIZE).await?;
        return Ok(());
    }
    write_message(send, &OpFrame::Done, MAX_OP_FRAME_SIZE).await?;
    debug!(ops = op_count, "served op phase");

    serve_git_phase(repo, send, recv).await
}

/// Validates the shape of a fetch request.
fn validate_request(repo: &MeshRepo, request: &FetchRequest) -> Result<(), String> {
    let id_len = repo.root_operation_id().as_bytes().len();
    let ok = !request.wants.is_empty()
        && request.wants.len() <= MAX_WANTS
        && request.haves.len() <= MAX_HAVES
        && request
            .wants
            .iter()
            .chain(&request.haves)
            .all(|id| id.len() == id_len);
    if ok {
        Ok(())
    } else {
        Err("malformed fetch request".to_owned())
    }
}

/// Serves the git phase: answers the fetcher's commit wants with the raw
/// object closure, stopping at its haves.
async fn serve_git_phase(
    repo: &Arc<MeshRepo>,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<()> {
    let request: GitRequest = read_message(recv, MAX_GIT_FRAME_SIZE).await?;

    let hash_len = repo.git_backend().git_repo().object_hash().len_in_bytes();
    let ok = request.haves.len() <= MAX_GIT_HAVES
        && request
            .wants
            .iter()
            .chain(&request.haves)
            .all(|id| id.len() == hash_len);
    if !ok {
        let message = "malformed git request".to_owned();
        write_message(send, &GitFrame::Error { message }, MAX_GIT_FRAME_SIZE).await?;
        return Ok(());
    }
    if request.wants.is_empty() {
        write_message(send, &GitFrame::Done, MAX_GIT_FRAME_SIZE).await?;
        return Ok(());
    }

    // The closure walk is blocking git I/O; it streams objects through a
    // bounded channel so huge closures never sit in memory at once.
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<Result<(gix::ObjectId, WireObjectKind, Vec<u8>)>>(64);
    let walk = {
        let repo = repo.clone();
        tokio::task::spawn_blocking(move || {
            let outcome = walk_git_closure(&repo, &request, |object| {
                tx.blocking_send(Ok(object))
                    .map_err(|_| eyre!("fetcher went away"))
            });
            if let Err(err) = outcome {
                let _ = tx.blocking_send(Err(err));
            }
        })
    };

    let mut served = 0usize;
    let mut failed = None;
    while let Some(object) = rx.recv().await {
        match object {
            Ok((id, kind, data)) => {
                let frame = GitFrame::Object {
                    id: id.as_bytes().to_vec(),
                    kind,
                    data,
                };
                write_message(send, &frame, MAX_GIT_FRAME_SIZE).await?;
                served += 1;
            }
            Err(err) => {
                failed = Some(format!("cannot walk git objects: {err:#}"));
                break;
            }
        }
    }
    walk.await.wrap_err("git walk task failed")?;

    let last = match failed {
        Some(message) => GitFrame::Error { message },
        None => GitFrame::Done,
    };
    write_message(send, &last, MAX_GIT_FRAME_SIZE).await?;
    debug!(objects = served, "served git phase");
    Ok(())
}

/// Walks the object closure of the wanted commits, stopping at haves, and
/// emits every object once. Trees and blobs the fetcher already has (those
/// reachable from its have commits) are pruned, so a sync transfers only the
/// objects the change actually introduced, not the whole working tree.
fn walk_git_closure(
    repo: &MeshRepo,
    request: &GitRequest,
    mut emit: impl FnMut((gix::ObjectId, WireObjectKind, Vec<u8>)) -> Result<()>,
) -> Result<()> {
    let git = repo.git_backend().git_repo();
    let haves: HashSet<gix::ObjectId> = request
        .haves
        .iter()
        .map(|id| gix::ObjectId::try_from(id.as_slice()))
        .collect::<Result<_, _>>()?;

    // Seed the seen set with everything reachable from the haves' trees, so
    // the emit walk below skips the (often vast) part of the tree the change
    // left untouched.
    let mut seen: HashSet<gix::ObjectId> = HashSet::new();
    mark_have_trees(&git, &haves, &mut seen);

    // Pass 1: collect wanted commits, children before parents. Non-commit
    // wants (tags, arbitrary git ref targets) join the tree walk at the
    // end.
    let mut queue: VecDeque<gix::ObjectId> = VecDeque::new();
    for want in &request.wants {
        let id = gix::ObjectId::try_from(want.as_slice())?;
        if !haves.contains(&id) && seen.insert(id) {
            queue.push_back(id);
        }
    }

    let mut commits: Vec<(gix::ObjectId, gix::ObjectId)> = Vec::new();
    let mut extras: Vec<gix::ObjectId> = Vec::new();
    while let Some(id) = queue.pop_front() {
        let object = git
            .find_object(id)
            .wrap_err_with(|| format!("missing wanted object {id}"))?;
        if object.kind == gix::object::Kind::Commit {
            let commit = object.try_into_commit().map_err(|err| eyre!("{err}"))?;
            let tree = commit.tree_id().map_err(|err| eyre!("{err}"))?.detach();
            for parent in commit.parent_ids() {
                let parent = parent.detach();
                if !haves.contains(&parent) && seen.insert(parent) {
                    queue.push_back(parent);
                }
            }
            commits.push((id, tree));
        } else {
            seen.remove(&id);
            extras.push(id);
        }
    }

    // Pass 2: emit oldest-first, each commit's tree closure before the
    // commit itself. Any crash-truncated prefix of the stream then upholds
    // "a stored commit implies its trees and ancestry are stored", which
    // the fetcher's missing-commit check relies on when retrying.
    for (commit, tree) in commits.iter().rev() {
        walk_tree(&git, *tree, &mut seen, &mut emit)?;
        let object = git
            .find_object(*commit)
            .wrap_err_with(|| format!("missing object {commit}"))?;
        emit((*commit, WireObjectKind::Commit, object.detach().data))?;
    }
    for id in extras {
        walk_tree(&git, id, &mut seen, &mut emit)?;
    }
    Ok(())
}

/// Emits a tree (or tag/blob) and its transitive entries.
fn walk_tree(
    git: &gix::Repository,
    root: gix::ObjectId,
    seen: &mut HashSet<gix::ObjectId>,
    emit: &mut impl FnMut((gix::ObjectId, WireObjectKind, Vec<u8>)) -> Result<()>,
) -> Result<()> {
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let object = git
            .find_object(id)
            .wrap_err_with(|| format!("missing object {id}"))?;
        // `detach()` moves the object's buffer out instead of copying it,
        // which matters for large blobs.
        match object.kind {
            gix::object::Kind::Tree => {
                let tree = object.try_into_tree().map_err(|err| eyre!("{err}"))?;
                for entry in tree.iter() {
                    let entry = entry.map_err(|err| eyre!("{err}"))?;
                    // Gitlink (submodule) entries point at commits in a
                    // different repository; they are not ours to send.
                    if entry.mode().is_commit() {
                        continue;
                    }
                    stack.push(entry.oid().to_owned());
                }
                emit((id, WireObjectKind::Tree, tree.detach().data))?;
            }
            gix::object::Kind::Blob => emit((id, WireObjectKind::Blob, object.detach().data))?,
            gix::object::Kind::Commit => {
                // A tree entry cannot be a commit, but a tag can point at
                // one; treat it as a boundary (it was either walked as a
                // commit already or is outside the requested closure).
            }
            gix::object::Kind::Tag => {
                let tag = object.try_into_tag().map_err(|err| eyre!("{err}"))?;
                let target = tag.target_id().map_err(|err| eyre!("{err}"))?.detach();
                stack.push(target);
                emit((id, WireObjectKind::Tag, tag.detach().data))?;
            }
        }
    }
    Ok(())
}

/// Marks every tree and blob reachable from the have commits as already
/// present, so [`walk_git_closure`] does not re-send subtrees the fetcher
/// shares with its haves. The fetcher holds each have's full object closure
/// (our own emit order guarantees a stored commit implies its trees), so
/// pruning against it is sound. Best-effort: haves the server lacks, or that
/// are not commits, are skipped, at worst re-sending more.
fn mark_have_trees(
    git: &gix::Repository,
    haves: &HashSet<gix::ObjectId>,
    seen: &mut HashSet<gix::ObjectId>,
) {
    let mut stack: Vec<gix::ObjectId> = Vec::new();
    for have in haves {
        let Ok(object) = git.find_object(*have) else {
            continue;
        };
        let Ok(commit) = object.try_into_commit() else {
            continue;
        };
        if let Ok(tree) = commit.tree_id() {
            let tree = tree.detach();
            if seen.insert(tree) {
                stack.push(tree);
            }
        }
    }
    while let Some(id) = stack.pop() {
        let Ok(object) = git.find_object(id) else {
            continue;
        };
        let Ok(tree) = object.try_into_tree() else {
            continue;
        };
        for entry in tree.iter().flatten() {
            let mode = entry.mode();
            // Gitlink (submodule) entries point outside this repo and are
            // never sent, so they need no marking.
            if mode.is_commit() {
                continue;
            }
            let oid = entry.oid().to_owned();
            // Only trees need descending; blobs are leaves, mark and move on
            // (their content is not even loaded here).
            if mode.is_tree() {
                if seen.insert(oid) {
                    stack.push(oid);
                }
            } else {
                seen.insert(oid);
            }
        }
    }
}

// --- Fetcher side ---

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

/// A replicated operation: raw bytes to store under `id`, plus the
/// structural metadata extracted from them.
struct StoredOp {
    id: OperationId,
    bytes: Vec<u8>,
    meta: OpMeta,
}

/// A replicated view (see [`StoredOp`]).
struct StoredView {
    id: ViewId,
    bytes: Vec<u8>,
    meta: ViewMeta,
}

/// The validated result of a fetch's op phase.
struct OpBatch {
    /// Parents-first, as received.
    ops: Vec<StoredOp>,
    views: Vec<StoredView>,
}

impl OpBatch {
    /// Indexes the batch ops' metadata by op id.
    fn ops_by_id(&self) -> HashMap<&OperationId, &OpMeta> {
        self.ops.iter().map(|op| (&op.id, &op.meta)).collect()
    }
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
                bail!("peer refused fetch: {}", crate::config::sanitize(&message))
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
                bail!(
                    "peer failed git phase: {}",
                    crate::config::sanitize(&message)
                )
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

// --- Apply ---

/// Applies a validated batch: keep refs, views, ops, extras, the colocated
/// ref mirror, then head publication. Runs on a blocking thread.
fn apply(
    repo: &Arc<MeshRepo>,
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

    for view in &batch.views {
        repo.write_view_bytes(&view.id, &view.bytes)?;
    }
    for op in &batch.ops {
        repo.write_operation_bytes(&op.id, &op.bytes)?;
    }

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
    let fast_forward = to_publish.len() == 1
        && local_heads.len() == 1
        && all_superseded.len() == local_heads.len();
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

/// Walk budget for [`superseded_by`]: stopping early is safe (unwalked
/// local heads stay listed and jj reconciles the divergence), so deep
/// histories need not be traversed exhaustively.
const SUPERSEDE_WALK_BUDGET: usize = 1 << 16;

/// The local heads that are ancestors of `want`, walking parent links of
/// validated ops (batch first, local store as fallback).
fn superseded_by(
    repo: &MeshRepo,
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
fn write_keep_refs(repo: &MeshRepo, commit_ids: &HashSet<CommitId>) -> Result<()> {
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

/// Seeds a freshly joined repo's colocated `.git` with the fetched view's
/// refs. Join pulls are divergent by construction (the fresh init ops are
/// not ancestors of the mesh head), so [`apply`] skips its mirror; without
/// this, the first jj command in the new repo would misread the
/// replicated refs as git-side deletions.
pub async fn mirror_after_join(
    repo: &Arc<MeshRepo>,
    mesh_head: &OperationId,
    old_head: &OperationId,
) -> Result<()> {
    if !repo.is_colocated() {
        return Ok(());
    }
    let repo = repo.clone();
    let mesh_head = mesh_head.clone();
    let old_head = old_head.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let new_op = repo.read_operation(&mesh_head).block_on()?;
        let new_view = repo.read_view(&new_op.view_id).block_on()?;
        let old_op = repo.read_operation(&old_head).block_on()?;
        let old_view = repo.read_view(&old_op.view_id).block_on()?;
        mirror_git_refs(&repo, &new_view, &old_view)
    })
    .await
    .wrap_err("mirror task failed")?
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
fn mirror_git_refs(repo: &MeshRepo, view: &View, old_view: &View) -> Result<()> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};

    let git = repo.git_backend().git_repo();
    let mut edits: Vec<(String, RefEdit)> = Vec::new();

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
        edits.push((
            name.as_str().to_owned(),
            RefEdit {
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
            },
        ));
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
        edits.push((
            name.as_str().to_owned(),
            RefEdit {
                change: Change::Delete {
                    expected: PreviousValue::MustExistAndMatch(gix::refs::Target::Object(
                        to_gix_id(old_id)?,
                    )),
                    log: gix::refs::transaction::RefLog::AndReference,
                },
                name: name
                    .as_str()
                    .try_into()
                    .map_err(|err| eyre!("bad ref name: {err}"))?,
                deref: false,
            },
        ));
    }

    // Edits apply individually: one ref the user raced must not abort the
    // rest of the mirror.
    for (name, edit) in edits {
        if let Err(err) = git.edit_references(Some(edit)) {
            warn!("skipping git ref mirror of {name}: {err}");
        }
    }
    Ok(())
}

// --- Helpers ---

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
    haves.truncate(MAX_HAVES);
    Ok(haves)
}

fn to_gix_id(id: &CommitId) -> Result<gix::ObjectId> {
    gix::ObjectId::try_from(id.as_bytes()).map_err(|err| eyre!("bad commit id: {err}"))
}

fn to_gix_kind(kind: WireObjectKind) -> gix::object::Kind {
    match kind {
        WireObjectKind::Commit => gix::object::Kind::Commit,
        WireObjectKind::Tree => gix::object::Kind::Tree,
        WireObjectKind::Blob => gix::object::Kind::Blob,
        WireObjectKind::Tag => gix::object::Kind::Tag,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, process::Command};

    use super::*;
    use crate::{repo::JjRepo, testing::Fixture};

    fn open(dir: &Path) -> Arc<MeshRepo> {
        Arc::new(JjRepo::discover(dir).unwrap().open().unwrap())
    }

    /// Copies a repo directory, forking its history.
    fn fork(from: &Path, to: &Path) {
        let cp = Command::new("cp")
            .arg("-r")
            .args([from, to])
            .status()
            .unwrap();
        assert!(cp.success());
    }

    /// Runs one fetch of `wants` from `server` into `fetcher` over an
    /// in-memory stream pair, as the daemon would over QUIC.
    async fn sync_once(
        fetcher: &Arc<MeshRepo>,
        server: &Arc<MeshRepo>,
        wants: &[OperationId],
    ) -> FetchOutcome {
        let (client, remote) = tokio::io::duplex(1 << 20);
        let (mut client_rx, mut client_tx) = tokio::io::split(client);
        let (mut server_rx, mut server_tx) = tokio::io::split(remote);

        let server = server.clone();
        let serve_task = tokio::spawn(async move {
            let request: FetchRequest = read_message(&mut server_rx, MAX_OP_FRAME_SIZE)
                .await
                .unwrap();
            serve(&server, request, &mut server_tx, &mut server_rx)
                .await
                .unwrap();
        });

        let outcome = fetch(
            fetcher,
            "test",
            &crate::config::RepoId::generate(),
            wants,
            &mut client_tx,
            &mut client_rx,
        )
        .await
        .unwrap();
        serve_task.await.unwrap();
        outcome
    }

    #[tokio::test]
    async fn fast_forward_sync_transfers_ops_and_git_objects() {
        let fx = Fixture::new();
        let a = fx.init_repo("a");
        fork(&a, &fx.path().join("b"));
        let b = fx.path().join("b");

        // Real file content on a, so commits, trees and blobs must travel.
        fs::write(a.join("file.txt"), "mesh content\n").unwrap();
        fx.jj(&a, &["commit", "-m", "add file"]);
        fx.jj(&a, &["bookmark", "create", "main", "-r", "@-"]);

        let (ra, rb) = (open(&a), open(&b));
        let wants = ra.op_heads().await.unwrap();
        let outcome = sync_once(&rb, &ra, &wants).await;

        assert_eq!(outcome.published, wants);
        assert!(outcome.ops > 0);
        assert!(outcome.git_objects > 0);
        assert_eq!(rb.op_heads().await.unwrap(), wants);

        // jj itself must accept the synced repo: log walks commits, which
        // requires the git objects and the change-id extras. The fork
        // shares the workspace name with `a` (real machines never do; the
        // join flow assigns unique names), so b's working copy is
        // legitimately stale and skipped here.
        fx.jj(&b, &["op", "log", "--ignore-working-copy"]);
        fx.jj(&b, &["log", "-r", "all()", "--ignore-working-copy"]);

        // Re-fetching the same heads is a no-op.
        let again = sync_once(&rb, &ra, &wants).await;
        assert!(again.published.is_empty());
    }

    /// An incremental sync must carry only the objects the change touched,
    /// not the whole working tree: the server prunes everything reachable
    /// from the fetcher's haves.
    #[tokio::test]
    async fn incremental_sync_transfers_only_changed_objects() {
        let fx = Fixture::new();
        let a = fx.init_repo("a");
        // Enough files that a full-tree resend would be conspicuous.
        for n in 0..20 {
            fs::write(a.join(format!("file{n}.txt")), format!("content {n}\n")).unwrap();
        }
        fx.jj(&a, &["commit", "-m", "populate"]);
        fork(&a, &fx.path().join("b"));
        let b = fx.path().join("b");

        // b now has every object; a changes a single file.
        fs::write(a.join("file0.txt"), "changed\n").unwrap();
        fx.jj(&a, &["commit", "-m", "one change"]);

        let (ra, rb) = (open(&a), open(&b));
        let wants = ra.op_heads().await.unwrap();
        let outcome = sync_once(&rb, &ra, &wants).await;

        // The delta is the one changed blob, the trees on its path, and the
        // commits, nowhere near the 20-plus objects a full resend carries.
        assert!(outcome.git_objects > 0);
        assert!(
            outcome.git_objects < 10,
            "sent {} objects for a one-file change",
            outcome.git_objects,
        );
        assert_eq!(rb.op_heads().await.unwrap(), wants);
    }

    #[tokio::test]
    async fn divergent_sync_keeps_both_heads() {
        let fx = Fixture::new();
        let a = fx.init_repo("a");
        fork(&a, &fx.path().join("b"));
        let b = fx.path().join("b");

        fx.jj(&a, &["new", "-m", "from a"]);
        fx.jj(&b, &["new", "-m", "from b"]);

        let (ra, rb) = (open(&a), open(&b));
        let wants = ra.op_heads().await.unwrap();
        let b_head_before = rb.op_heads().await.unwrap();
        sync_once(&rb, &ra, &wants).await;

        // Both lines of history stay: divergence is left for jj.
        let mut expected: Vec<OperationId> = wants;
        expected.extend(b_head_before);
        expected.sort_unstable();
        let mut heads = rb.op_heads().await.unwrap();
        heads.sort_unstable();
        assert_eq!(heads, expected);
        fx.jj(&b, &["op", "log"]);
    }

    #[tokio::test]
    async fn colocated_sync_mirrors_git_branches() {
        let fx = Fixture::new();
        let dir_a = fx.path().join("a");
        fx.jj(fx.path(), &["git", "init", "--colocate", "a"]);
        fx.jj(&dir_a, &["describe", "-m", "base"]);
        fx.jj(&dir_a, &["bookmark", "create", "main", "-r", "@"]);
        // A jj command after the bookmark exports it to the colocated git.
        fx.jj(&dir_a, &["new", "-m", "past-bookmark"]);
        fork(&dir_a, &fx.path().join("b"));
        let dir_b = fx.path().join("b");

        fs::write(dir_a.join("file.txt"), "moved\n").unwrap();
        fx.jj(&dir_a, &["commit", "-m", "advance"]);
        fx.jj(&dir_a, &["bookmark", "set", "main", "-r", "@-"]);
        // Export the bookmark move to git on a.
        fx.jj(&dir_a, &["new", "-m", "trigger export"]);

        let (ra, rb) = (open(&dir_a), open(&dir_b));
        assert!(rb.is_colocated());
        let wants = ra.op_heads().await.unwrap();
        sync_once(&rb, &ra, &wants).await;

        // The git branch in b's colocated .git must match a's export.
        let expected = git_rev(&dir_a, "refs/heads/main");
        assert_eq!(git_rev(&dir_b, "refs/heads/main"), expected);
        fx.jj(&dir_b, &["log", "-r", "all()"]);
    }

    fn git_rev(dir: &Path, rev: &str) -> String {
        let out = Command::new("git")
            .current_dir(dir)
            .args(["rev-parse", rev])
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git rev-parse failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    }

    /// A hostile batch op naming a local head as its parent, without being
    /// on the want's ancestry, must be rejected: accepting it would let a
    /// peer unlist that head (op-log rollback).
    #[tokio::test]
    async fn rejects_ops_unreachable_from_wants() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        let repo = open(&dir);
        let local_head = repo.op_heads().await.unwrap().remove(0);

        let want = OperationId::new(vec![1; 64]);
        let make_op = move |parents: Vec<OperationId>| {
            use prost::Message as _;
            jj_lib::protos::simple_op_store::Operation {
                view_id: vec![9; 64],
                parents: parents.iter().map(|id| id.as_bytes().to_vec()).collect(),
                metadata: Some(jj_lib::protos::simple_op_store::OperationMetadata {
                    description: "crafted".to_owned(),
                    hostname: "evil".to_owned(),
                    username: "evil".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .encode_to_vec()
        };

        let (client, remote) = tokio::io::duplex(1 << 20);
        let (mut client_rx, mut client_tx) = tokio::io::split(client);
        let (mut server_rx, mut server_tx) = tokio::io::split(remote);

        let head_bytes = local_head.clone();
        let server = tokio::spawn(async move {
            use prost::Message as _;
            let _request: FetchRequest = read_message(&mut server_rx, MAX_OP_FRAME_SIZE)
                .await
                .unwrap();
            let view = jj_lib::protos::simple_op_store::View::default().encode_to_vec();
            let frames = [
                OpFrame::View {
                    id: vec![9; 64],
                    view,
                },
                // The wanted op, legitimately parented on the local head.
                OpFrame::Op {
                    id: vec![1; 64],
                    op: make_op(vec![head_bytes.clone()]),
                },
                // The poison: parented on the local head, but nothing
                // connects it to the want.
                OpFrame::Op {
                    id: vec![2; 64],
                    op: make_op(vec![head_bytes]),
                },
                OpFrame::Done,
            ];
            for frame in frames {
                write_message(&mut server_tx, &frame, MAX_OP_FRAME_SIZE)
                    .await
                    .unwrap();
            }
        });

        let err = fetch(
            &repo,
            "test",
            &crate::config::RepoId::generate(),
            &[want],
            &mut client_tx,
            &mut client_rx,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("unreachable"), "{err:#}");
        server.await.unwrap();

        // Nothing was published: the local head is untouched.
        assert_eq!(repo.op_heads().await.unwrap(), vec![local_head]);
    }

    /// Branches the user created directly in the colocated `.git` (unknown
    /// to any jj view) must survive the ref mirror, and upstream bookmark
    /// deletions must still propagate.
    #[tokio::test]
    async fn mirror_preserves_user_branches_and_propagates_deletions() {
        let fx = Fixture::new();
        let dir_a = fx.path().join("a");
        fx.jj(fx.path(), &["git", "init", "--colocate", "a"]);
        fx.jj(&dir_a, &["describe", "-m", "base"]);
        fx.jj(&dir_a, &["bookmark", "create", "main", "-r", "@"]);
        fx.jj(&dir_a, &["new", "-m", "export"]);
        fork(&dir_a, &fx.path().join("b"));
        let dir_b = fx.path().join("b");

        // A user-made branch in b's .git, invisible to every view.
        let user_commit = git_rev(&dir_b, "HEAD");
        let branch = Command::new("git")
            .current_dir(&dir_b)
            .args(["branch", "user-branch", &user_commit])
            .status()
            .unwrap();
        assert!(branch.success());

        // Upstream: delete the bookmark and export the deletion.
        fx.jj(&dir_a, &["bookmark", "delete", "main"]);
        fx.jj(&dir_a, &["new", "-m", "export deletion"]);

        let (ra, rb) = (open(&dir_a), open(&dir_b));
        let wants = ra.op_heads().await.unwrap();
        sync_once(&rb, &ra, &wants).await;

        // The user's branch survived; the deleted bookmark propagated.
        assert_eq!(git_rev(&dir_b, "refs/heads/user-branch"), user_commit);
        let gone = Command::new("git")
            .current_dir(&dir_b)
            .args(["rev-parse", "--verify", "refs/heads/main"])
            .output()
            .unwrap();
        assert!(!gone.status.success(), "deleted bookmark must propagate");
    }

    /// The join flow: a freshly initialized repo with a renamed workspace
    /// pulls the full mesh state; jj then merges the fresh workspace into
    /// the replicated history on the next command.
    #[tokio::test]
    async fn join_pull_into_fresh_repo() {
        let fx = Fixture::new();
        let a = fx.init_repo("a");
        fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
        fx.jj(&a, &["new", "-m", "second"]);

        let b = fx.path().join("b");
        fx.jj(fx.path(), &["git", "init", "b"]);
        fx.jj(&b, &["workspace", "rename", "machine-b"]);

        let (ra, rb) = (open(&a), open(&b));
        let wants = ra.op_heads().await.unwrap();
        let init_head = rb.op_heads().await.unwrap();
        let outcome = sync_once(&rb, &ra, &wants).await;
        assert_eq!(outcome.published, wants);

        // Divergent by construction: init ops are not mesh ancestors.
        assert_eq!(rb.op_heads().await.unwrap().len(), 2);

        // Seed the colocated git refs as the daemon's join handler does.
        mirror_after_join(&rb, &wants[0], &init_head[0])
            .await
            .unwrap();
        assert_eq!(
            git_rev(&b, "refs/heads/main"),
            git_rev(&a, "refs/heads/main")
        );

        // The next jj command merges: both workspaces coexist, and the
        // mesh history is visible from the fresh machine.
        fx.jj(&b, &["status"]);
        let list = Command::new("jj")
            .current_dir(&b)
            .env("JJ_CONFIG", "/dev/null")
            .env("JJ_USER", "Test User")
            .env("JJ_EMAIL", "test@example.com")
            .args(["workspace", "list"])
            .output()
            .unwrap();
        let list = String::from_utf8(list.stdout).unwrap();
        assert!(list.contains("machine-b:"), "{list}");
        assert!(list.contains("default:"), "{list}");
        assert_eq!(rb.op_heads().await.unwrap().len(), 1, "merged");
    }

    /// Repos containing gitlink (submodule) tree entries must sync: the
    /// linked commit lives in another repository and is not sent.
    #[tokio::test]
    async fn syncs_trees_with_gitlink_entries() {
        let fx = Fixture::new();
        let dir_a = fx.path().join("a");
        fx.jj(fx.path(), &["git", "init", "--colocate", "a"]);
        fx.jj(&dir_a, &["describe", "-m", "base"]);
        fork(&dir_a, &fx.path().join("b"));
        let dir_b = fx.path().join("b");

        // Craft a commit whose tree has a gitlink to a commit that does
        // not exist here, as a submodule checkout would.
        let script = "tree=$(printf '160000 commit 1111111111111111111111111111111111111111\\tsub\\n' \
                      | git mktree --missing) && git branch gitlink $(git commit-tree $tree -m gitlink)";
        let crafted = Command::new("sh")
            .current_dir(&dir_a)
            .args(["-ec", script])
            .status()
            .unwrap();
        assert!(crafted.success());
        // Import the crafted branch into jj's view.
        fx.jj(&dir_a, &["git", "import"]);

        let (ra, rb) = (open(&dir_a), open(&dir_b));
        let wants = ra.op_heads().await.unwrap();
        let outcome = sync_once(&rb, &ra, &wants).await;
        assert!(!outcome.published.is_empty());

        // The gitlink commit's own objects arrived; the submodule target
        // was correctly skipped.
        let gitlink_commit = git_rev(&dir_a, "refs/heads/gitlink");
        let present = Command::new("git")
            .current_dir(&dir_b)
            .args(["cat-file", "-e", &gitlink_commit])
            .status()
            .unwrap();
        assert!(present.success());
    }
}

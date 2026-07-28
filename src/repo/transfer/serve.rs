//! Server side of a fetch: streaming the op-log delta, then the git object
//! closure the fetcher lacks. Read-only on the repo throughout.

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use color_eyre::eyre::{Result, WrapErr as _, eyre};
use jj_lib::{
    object_id::ObjectId as _,
    op_store::{OperationId, ViewId},
};
use pollster::FutureExt as _;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::debug;

use super::pack;
use crate::{
    net::{
        sync::{
            FetchRequest, GitFrame, GitRequest, GitTransferFormat, MAX_GIT_FRAME_SIZE,
            MAX_GIT_HAVES, MAX_HAVES, MAX_OP_FRAME_SIZE, MAX_WANTS, OpFrame, WireObjectKind,
            compress_payload,
        },
        wire::{read_message, write_message},
    },
    repo::MeshRepo,
};

/// Op/view frames buffered between the blocking walk and the stream writer.
/// Small: it only smooths the pipeline, the point is to not hold the whole
/// delta at once.
const OP_STREAM_BUFFER: usize = 16;

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
                // The delta is fully collected, so the phase totals are
                // exact: every unique view is sent exactly once.
                let views: HashSet<&ViewId> = ops.iter().map(|(_, op)| &op.view_id).collect();
                let begin = OpFrame::Begin {
                    ops: ops.len() as u64,
                    views: views.len() as u64,
                };
                tx.blocking_send(Ok(begin))
                    .map_err(|_| eyre!("fetcher went away"))?;
                let mut sent_views: HashSet<ViewId> = HashSet::with_capacity(views.len());
                for (id, op) in ops {
                    if sent_views.insert(op.view_id.clone()) {
                        let view = repo.read_view_bytes(&op.view_id)?;
                        let frame = OpFrame::View {
                            id: op.view_id.as_bytes().to_vec(),
                            view: compress_payload(&view)?,
                        };
                        tx.blocking_send(Ok(frame))
                            .map_err(|_| eyre!("fetcher went away"))?;
                    }
                    let bytes = repo.read_operation_bytes(&id)?;
                    let frame = OpFrame::Op {
                        id: id.as_bytes().to_vec(),
                        op: compress_payload(&bytes)?,
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
/// object closure, stopping at its haves, in the requested format.
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

    match request.format {
        GitTransferFormat::Loose => serve_git_loose(repo, request, send).await,
        GitTransferFormat::Pack => serve_git_pack(repo, request, send).await,
    }
}

/// Serves the git phase in the loose format: one object per frame.
async fn serve_git_loose(
    repo: &Arc<MeshRepo>,
    request: GitRequest,
    send: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    // The closure walk is blocking git I/O; it streams objects through a
    // bounded channel so huge closures never sit in memory at once.
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<Result<(gix::ObjectId, WireObjectKind, Vec<u8>)>>(64);
    let walk = {
        let repo = repo.clone();
        tokio::task::spawn_blocking(move || {
            let git = repo.git_backend().git_repo();
            let outcome = walk_git_closure(&repo, &request, |id, kind| {
                let object = git
                    .find_object(id)
                    .wrap_err_with(|| format!("missing object {id}"))?;
                // `detach()` moves the object's buffer out instead of
                // copying it, which matters for large blobs.
                let data = compress_payload(&object.detach().data)?;
                tx.blocking_send(Ok((id, kind, data)))
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

/// Serves the git phase in the pack format: the walk collects the closure's
/// ids without loading blob contents, then the pack pipeline streams one
/// packfile in chunks.
async fn serve_git_pack(
    repo: &Arc<MeshRepo>,
    request: GitRequest,
    send: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>>>(8);
    let walk = {
        let repo = repo.clone();
        tokio::task::spawn_blocking(move || {
            let outcome = (|| -> Result<()> {
                let mut ids = Vec::new();
                walk_git_closure(&repo, &request, |id, _kind| {
                    ids.push(id);
                    Ok(())
                })?;
                let git = repo.git_backend().git_repo();
                pack::write_pack(&git, ids, |chunk| {
                    tx.blocking_send(Ok(chunk))
                        .map_err(|_| eyre!("fetcher went away"))
                })
            })();
            if let Err(err) = outcome {
                let _ = tx.blocking_send(Err(err));
            }
        })
    };

    let mut served = 0usize;
    let mut failed = None;
    while let Some(chunk) = rx.recv().await {
        match chunk {
            Ok(chunk) => {
                served += chunk.len();
                write_message(send, &GitFrame::Pack { chunk }, MAX_GIT_FRAME_SIZE).await?;
            }
            Err(err) => {
                failed = Some(format!("cannot build pack: {err:#}"));
                break;
            }
        }
    }
    walk.await.wrap_err("pack build task failed")?;

    let last = match failed {
        Some(message) => GitFrame::Error { message },
        None => GitFrame::Done,
    };
    write_message(send, &last, MAX_GIT_FRAME_SIZE).await?;
    debug!(bytes = served, "served git phase (pack)");
    Ok(())
}

/// Walks the object closure of the wanted commits, stopping at haves, and
/// emits every object's id once. Trees and blobs the fetcher already has
/// (those reachable from its have commits) are pruned, so a sync transfers
/// only the objects the change actually introduced, not the whole working
/// tree.
///
/// Only ids are emitted: blob contents are never loaded here, and each
/// format reads what it needs (the loose server per object, the pack
/// pipeline itself).
fn walk_git_closure(
    repo: &MeshRepo,
    request: &GitRequest,
    mut emit: impl FnMut(gix::ObjectId, WireObjectKind) -> Result<()>,
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
        emit(*commit, WireObjectKind::Commit)?;
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
    emit: &mut impl FnMut(gix::ObjectId, WireObjectKind) -> Result<()>,
) -> Result<()> {
    let mut stack = vec![(root, None::<WireObjectKind>)];
    while let Some((id, known_kind)) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        // Blobs are leaves: their kind is known from the tree entry that
        // named them, so they are emitted without ever being loaded.
        if known_kind == Some(WireObjectKind::Blob) {
            emit(id, WireObjectKind::Blob)?;
            continue;
        }
        let object = git
            .find_object(id)
            .wrap_err_with(|| format!("missing object {id}"))?;
        match object.kind {
            gix::object::Kind::Tree => {
                let tree = object.try_into_tree().map_err(|err| eyre!("{err}"))?;
                for entry in tree.iter() {
                    let entry = entry.map_err(|err| eyre!("{err}"))?;
                    let mode = entry.mode();
                    // Gitlink (submodule) entries point at commits in a
                    // different repository; they are not ours to send.
                    if mode.is_commit() {
                        continue;
                    }
                    let kind = (!mode.is_tree()).then_some(WireObjectKind::Blob);
                    stack.push((entry.oid().to_owned(), kind));
                }
                emit(id, WireObjectKind::Tree)?;
            }
            gix::object::Kind::Blob => emit(id, WireObjectKind::Blob)?,
            gix::object::Kind::Commit => {
                // A tree entry cannot be a commit, but a tag can point at
                // one; treat it as a boundary (it was either walked as a
                // commit already or is outside the requested closure).
            }
            gix::object::Kind::Tag => {
                let tag = object.try_into_tag().map_err(|err| eyre!("{err}"))?;
                let target = tag.target_id().map_err(|err| eyre!("{err}"))?.detach();
                stack.push((target, None));
                emit(id, WireObjectKind::Tag)?;
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

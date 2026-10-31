//! Server side of a fetch: streaming the op-log delta, then the git object
//! closure the fetcher lacks. Every phase runs through [`serve_phase`].

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context as _, Result, anyhow};
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
use tracing::debug;

use super::{pack, to_gix_id};
use crate::{
    net::{
        fetch::{
            FetchRequest, GitFrame, GitRequest, GitTransferFormat, MAX_GIT_FRAME_SIZE, OpFrame,
            PhaseCompressor, WireObjectKind, compress_object,
        },
        wire::{MAX_FRAME_SIZE, read_message, write_message},
    },
    repo::OpenRepo,
};

/// Op/view frames buffered between the blocking walk and the stream writer.
/// Small: it only smooths the pipeline, the point is to not hold the whole
/// delta at once.
const OP_STREAM_BUFFER: usize = 16;

/// Git frames (loose objects or pack chunks) buffered between the
/// blocking producer and the stream writer.
const GIT_STREAM_BUFFER: usize = 8;

/// Objects from which a loose transfer is sent as a pack instead: past a
/// few thousand files, one pack is far cheaper to write and to send.
pub(super) const PACK_MIN_OBJECTS: usize = 2048;

/// Serves one fetch request over the given stream pair. Read-only on the
/// repo; errors are reported to the peer as protocol frames where the
/// exchange allows it.
pub async fn serve(
    repo: &Arc<OpenRepo>,
    request: FetchRequest,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<()> {
    if let Err(message) = validate_request(repo, &request) {
        return refuse(send, message).await;
    }
    let ids =
        |ids: Vec<Vec<u8>>| -> Vec<OperationId> { ids.into_iter().map(OperationId::new).collect() };
    let (wants, haves) = (ids(request.wants), ids(request.haves));

    for want in &wants {
        if !repo.has_operation(want).await? {
            return refuse(send, format!("unknown operation {}", want.hex())).await;
        }
    }

    // Stream the delta through a bounded channel so the whole op-log delta
    // never sits in memory at once: a clone pulls the entire log. Each view
    // is sent before the first op referencing it, and ops stay
    // parents-first.
    let served = {
        let repo = repo.clone();
        serve_phase(
            send,
            OP_STREAM_BUFFER,
            "cannot collect operations",
            move |tx| {
                let ops = repo.ancestors_until(&wants, &haves).block_on()?;
                // The delta is fully collected, so the phase totals are
                // exact: every unique view is sent exactly once.
                let views: HashSet<&ViewId> = ops.iter().map(|(_, op)| &op.view_id).collect();
                let sent_ops = ops.len();
                produce(
                    tx,
                    OpFrame::Begin {
                        ops: ops.len() as u64,
                        views: views.len() as u64,
                    },
                )?;
                let mut compressor = PhaseCompressor::new()?;
                let mut sent_views: HashSet<ViewId> = HashSet::with_capacity(views.len());
                for (id, op) in ops {
                    if sent_views.insert(op.view_id.clone()) {
                        let view = repo.read_view_bytes(&op.view_id)?;
                        produce(
                            tx,
                            OpFrame::View {
                                id: op.view_id.as_bytes().to_vec(),
                                view: compressor.compress(&view)?,
                            },
                        )?;
                    }
                    let bytes = repo.read_operation_bytes(&id)?;
                    produce(
                        tx,
                        OpFrame::Op {
                            id: id.as_bytes().to_vec(),
                            op: compressor.compress(&bytes)?,
                        },
                    )?;
                }
                Ok(sent_ops)
            },
        )
        .await?
    };
    let Some(ops) = served else {
        return Ok(());
    };
    debug!(ops, "served op phase");

    serve_git_phase(repo, send, recv).await
}

/// Refuses a fetch, telling the fetcher why.
pub async fn refuse(send: &mut (impl AsyncWrite + Unpin), message: String) -> Result<()> {
    write_message(send, &OpFrame::Error { message }, MAX_FRAME_SIZE).await
}

/// Validates the shape of a fetch request.
fn validate_request(repo: &OpenRepo, request: &FetchRequest) -> Result<(), String> {
    let id_len = repo.root_operation_id().as_bytes().len();
    let ok = !request.wants.is_empty()
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
/// object closure it lacks (see [`walk_git_closure`]), in the requested
/// format, or as a pack past [`PACK_MIN_OBJECTS`].
async fn serve_git_phase(
    repo: &Arc<OpenRepo>,
    send: &mut (impl AsyncWrite + Unpin),
    recv: &mut (impl AsyncRead + Unpin),
) -> Result<()> {
    let request: GitRequest = read_message(recv, MAX_FRAME_SIZE).await?;
    let hash_len = repo.git_backend().git_repo().object_hash().len_in_bytes();
    let ok = request
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
    let ids = |ids: Vec<Vec<u8>>| -> Vec<CommitId> { ids.into_iter().map(CommitId::new).collect() };
    let (wants, haves) = (ids(request.wants), ids(request.haves));

    let repo = repo.clone();
    serve_phase(
        send,
        GIT_STREAM_BUFFER,
        "cannot send git objects",
        move |tx| {
            let git = repo.git_backend().git_repo();
            let objects = walk_git_closure(&repo, &haves, &wants)?;
            if objects.is_empty() {
                return Ok(());
            }
            let format = if objects.len() >= PACK_MIN_OBJECTS {
                GitTransferFormat::Pack
            } else {
                request.format
            };
            let count = objects.len();
            produce(
                tx,
                GitFrame::Begin {
                    format,
                    objects: count as u64,
                },
            )?;
            match format {
                GitTransferFormat::Loose => send_loose(&git, objects, tx)?,
                GitTransferFormat::Pack => {
                    let ids = objects.into_iter().map(|(id, _)| id).collect();
                    pack::write_pack(&git, ids, |chunk| produce(tx, GitFrame::Pack { chunk }))?;
                }
            }
            debug!(?format, objects = count, "served git phase");
            Ok(())
        },
    )
    .await?;
    Ok(())
}

/// Sends `objects` one per frame, from a phase producer.
fn send_loose(
    git: &gix::Repository,
    objects: Vec<(gix::ObjectId, WireObjectKind)>,
    tx: &mpsc::Sender<GitFrame>,
) -> Result<()> {
    for (id, kind) in objects {
        let object = git
            .find_object(id)
            .with_context(|| format!("missing object {id}"))?;
        // `detach()` moves the object's buffer out instead of copying it,
        // which matters for large blobs.
        let data = compress_object(&object.detach().data)?;
        let id = id.as_bytes().to_vec();
        produce(tx, GitFrame::Object { id, kind, data })?;
    }
    Ok(())
}

/// A phase's terminal frames: `Done`, or `Error` carrying a message.
trait PhaseFrame: serde::Serialize {
    const MAX_SIZE: u32;
    fn done() -> Self;
    fn error(message: String) -> Self;
}

impl PhaseFrame for OpFrame {
    const MAX_SIZE: u32 = MAX_FRAME_SIZE;
    fn done() -> Self {
        OpFrame::Done
    }
    fn error(message: String) -> Self {
        OpFrame::Error { message }
    }
}

impl PhaseFrame for GitFrame {
    const MAX_SIZE: u32 = MAX_GIT_FRAME_SIZE;
    fn done() -> Self {
        GitFrame::Done
    }
    fn error(message: String) -> Self {
        GitFrame::Error { message }
    }
}

/// Runs one streamed phase: a blocking producer (`work`, sending frames
/// with [`produce`]) feeds a bounded channel, the frames are relayed to
/// the wire, and the phase closes with `Done`, or with an `Error` frame
/// prefixed by `err_context` when the producer failed. Returns the
/// producer's value, `None` when it failed: the exchange must not
/// continue then.
async fn serve_phase<T: PhaseFrame + Send + 'static, R: Send + 'static>(
    send: &mut (impl AsyncWrite + Unpin),
    buffer: usize,
    err_context: &str,
    work: impl FnOnce(&mpsc::Sender<T>) -> Result<R> + Send + 'static,
) -> Result<Option<R>> {
    let (tx, mut rx) = mpsc::channel(buffer);
    let producer = crate::spawn_blocking(move || work(&tx));

    // The channel closes once the producer returned, whatever it returned.
    while let Some(frame) = rx.recv().await {
        write_message(send, &frame, T::MAX_SIZE).await?;
    }
    let (last, value) = match producer.await.context("phase producer task failed")? {
        Ok(value) => (T::done(), Some(value)),
        Err(err) => (T::error(format!("{err_context}: {err:#}")), None),
    };
    write_message(send, &last, T::MAX_SIZE).await?;
    Ok(value)
}

/// Sends one frame from a producer, failing once the relay side is gone.
fn produce<T>(tx: &mpsc::Sender<T>, frame: T) -> Result<()> {
    tx.blocking_send(frame)
        .map_err(|_| anyhow!("fetcher went away"))
}

/// Walks the object closure of the wanted commits, minus what the fetcher
/// holds, listing every object's id once.
///
/// The fetcher holds the full closure of each have (our own emit order
/// guarantees a stored commit implies its trees and ancestry), so the
/// commits sent are the `haves..wants` range of jj's commit index at our
/// op heads. Wants outside that index are refused: only what the op log
/// references is served. Each commit's tree is compared against trees the
/// fetcher holds or receives first, its bases: its parents' and those of
/// its other versions (same change id, as a snapshot, describe or rebase
/// leaves), so mostly only the paths the commit changed are read and sent.
///
/// Only ids are listed: blob contents are never loaded here, and each
/// format reads what it needs (the loose server per object, the pack
/// pipeline itself).
pub(super) fn walk_git_closure(
    repo: &OpenRepo,
    haves: &[CommitId],
    wants: &[CommitId],
) -> Result<Vec<(gix::ObjectId, WireObjectKind)>> {
    let heads = repo
        .op_heads()
        .block_on()?
        .iter()
        .map(|id| repo.load_operation(id).block_on())
        .collect::<Result<Vec<_>>>()?;
    let commits = repo.commit_range(&heads, haves, wants)?;

    let git = repo.git_backend().git_repo();
    // The trees of each change's versions the fetcher holds or received.
    let mut versions: HashMap<Vec<u8>, Vec<gix::ObjectId>> = HashMap::new();
    for have in haves.iter().filter(|have| !repo.is_root_commit(have)) {
        // Haves we lack cannot serve as bases.
        if let Ok(have) = Commit::read(&git, to_gix_id(have)?)
            && let Some(change) = have.change
        {
            versions.entry(change).or_default().push(have.tree);
        }
    }

    // Parents first, each commit's tree closure before the commit itself.
    // Any crash-truncated prefix of the stream then upholds "a stored
    // commit implies its trees and ancestry are stored", which the
    // fetcher's missing-commit check relies on when retrying.
    let mut seen: HashSet<gix::ObjectId> = HashSet::new();
    let mut objects = Vec::new();
    // jj's virtual root commit is in the index, not in git.
    for commit in commits.iter().filter(|commit| !repo.is_root_commit(commit)) {
        let id = to_gix_id(commit)?;
        let commit = Commit::read(&git, id)?;
        let mut bases = Vec::with_capacity(MAX_BASES);
        let parents = commit
            .parents
            .iter()
            .map(|parent| Commit::read(&git, *parent).map(|parent| parent.tree));
        let others = commit
            .change
            .as_ref()
            .and_then(|change| versions.get(change))
            .into_iter()
            .flatten()
            .map(|tree| Ok(*tree));
        for base in parents.chain(others) {
            let base = base?;
            if bases.len() == MAX_BASES {
                break;
            }
            if !bases.contains(&base) {
                bases.push(base);
            }
        }
        walk_tree(&git, commit.tree, &bases, &mut seen, &mut objects)?;
        objects.push((id, WireObjectKind::Commit));
        if let Some(change) = commit.change {
            versions.entry(change).or_default().push(commit.tree);
        }
    }
    Ok(objects)
}

/// Bases a tree is compared against at most: an octopus merge must not
/// multiply the walk.
const MAX_BASES: usize = 8;

/// What the closure walk reads of a commit.
struct Commit {
    tree: gix::ObjectId,
    parents: Vec<gix::ObjectId>,
    /// jj's change id header, shared by a change's versions.
    change: Option<Vec<u8>>,
}

impl Commit {
    fn read(git: &gix::Repository, id: gix::ObjectId) -> Result<Self> {
        let object = git
            .find_commit(id)
            .with_context(|| format!("missing commit {id}"))?;
        let commit = object.decode().map_err(|err| anyhow!("{err}"))?;
        Ok(Commit {
            tree: commit.tree(),
            parents: commit.parents().collect(),
            change: commit
                .extra_headers()
                .find("change-id")
                .map(|change| change.to_vec()),
        })
    }
}

/// Lists a tree and its transitive entries, skipping those equal to the
/// entry at the same path in one of `bases`, and those already listed.
fn walk_tree(
    git: &gix::Repository,
    root: gix::ObjectId,
    bases: &[gix::ObjectId],
    seen: &mut HashSet<gix::ObjectId>,
    objects: &mut Vec<(gix::ObjectId, WireObjectKind)>,
) -> Result<()> {
    if bases.contains(&root) {
        return Ok(());
    }
    let mut stack = vec![(root, bases.to_vec())];
    while let Some((id, subtree_bases)) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let find = |id: gix::ObjectId| {
            git.find_tree(id)
                .with_context(|| format!("missing tree {id}"))
        };
        let tree = find(id)?;
        let base_trees = subtree_bases
            .iter()
            .map(|base| find(*base))
            .collect::<Result<Vec<_>>>()?;
        let base_refs = base_trees
            .iter()
            .map(|base| base.decode().map_err(|err| anyhow!("{err}")))
            .collect::<Result<Vec<_>>>()?;
        for entry in tree.decode().map_err(|err| anyhow!("{err}"))?.entries {
            // Gitlinks point at commits in other repositories, never ours
            // to send.
            if entry.mode.is_commit() {
                continue;
            }
            let is_tree = entry.mode.is_tree();
            let same: Vec<gix::ObjectId> = base_refs
                .iter()
                .filter_map(|base| base.bisect_entry(entry.filename, is_tree))
                .map(|base| base.oid.to_owned())
                .collect();
            if same.contains(&entry.oid.to_owned()) {
                continue;
            }
            if is_tree {
                // jj stores a conflict's sides as `.jjconflict-*` subtrees
                // of the root, each a near-copy of a whole tree.
                let next = if same.is_empty() && entry.filename.starts_with(b".jjconflict") {
                    bases.to_vec()
                } else {
                    same
                };
                stack.push((entry.oid.to_owned(), next));
            } else if seen.insert(entry.oid.to_owned()) {
                // Blobs are leaves: listed without ever being loaded.
                objects.push((entry.oid.to_owned(), WireObjectKind::Blob));
            }
        }
        objects.push((id, WireObjectKind::Tree));
    }
    Ok(())
}

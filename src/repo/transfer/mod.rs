//! The op and git object transfer engine: serving fetches and fetching.
//!
//! Both sides run over any `AsyncRead`/`AsyncWrite` pair (QUIC streams in
//! production, in-memory duplexes in tests) and follow the phase protocol
//! described in [`crate::net::sync`]. Ops and views travel as raw stored
//! bytes and keep their sender-side ids (see the `mesh` module docs for
//! why re-hashing them is impossible). Peer-supplied data is authenticated
//! but untrusted: op and view bytes are validated structurally before
//! anything is written, git objects are hash-verified before writing
//! (loose frames against their claimed id, packed objects while indexing
//! the pack), and replicated bytes can never replace already-stored
//! objects (loose writes skip existing ids; pack ids are content hashes).
//!
//! The engine splits along its two sides and the apply step:
//! - [`serve`] answers a fetch (read-only): the op-log delta, then the git
//!   object closure the fetcher lacks, loose or as one packfile (see
//!   [`crate::net::sync::GitTransferFormat`]).
//! - [`fetch`] pulls and validates that delta, then hands it to [`apply`],
//!   which writes it in the crash-safe order: git objects, anti-GC keep
//!   refs, views and ops (parents first), change-id extras, the colocated
//!   ref mirror, and only then the op head publication.
//!
//! Bulk store and git work runs on blocking threads (see the `mesh` module
//! docs).

mod apply;
mod fetch;
mod pack;
mod serve;

use std::collections::HashMap;

use color_eyre::eyre::{Result, eyre};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OperationId, ViewId},
};

pub use fetch::fetch;
pub use serve::serve;

use super::codec::{OpMeta, ViewMeta};
use crate::net::sync::WireObjectKind;

/// What a completed fetch did, for logging and status.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Op heads published locally (empty when already up to date).
    pub published: Vec<OperationId>,
    pub ops: usize,
    pub git_objects: usize,
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

/// The validated result of a fetch's op phase: [`fetch`] builds it, [`apply`]
/// consumes it.
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
    use std::{fs, path::Path, process::Command, sync::Arc};

    use super::*;
    use crate::{
        net::{
            sync::{FetchRequest, GitTransferFormat, MAX_OP_FRAME_SIZE, OpFrame},
            wire::{read_message, write_message},
        },
        repo::{JjRepo, MeshRepo},
        testing::Fixture,
    };

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

    /// Initializes a fresh repo as `jj-mesh join` does: non-colocated,
    /// with a machine-unique workspace name.
    fn init_join_repo(fx: &Fixture, name: &str, workspace: &str) -> std::path::PathBuf {
        let dir = fx.path().join(name);
        fx.jj(fx.path(), &["git", "init", "--no-colocate", name]);
        fx.jj(&dir, &["workspace", "rename", workspace]);
        dir
    }

    /// Runs one fetch of `wants` from `server` into `fetcher` over an
    /// in-memory stream pair, as the daemon would over QUIC.
    async fn sync_once(
        fetcher: &Arc<MeshRepo>,
        server: &Arc<MeshRepo>,
        wants: &[OperationId],
    ) -> FetchOutcome {
        sync_once_as(fetcher, server, wants, GitTransferFormat::Loose).await
    }

    /// [`sync_once`] with an explicit git transfer format.
    async fn sync_once_as(
        fetcher: &Arc<MeshRepo>,
        server: &Arc<MeshRepo>,
        wants: &[OperationId],
        format: GitTransferFormat,
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
            format,
            &mut client_tx,
            &mut client_rx,
        )
        .await
        .unwrap();
        serve_task.await.unwrap();
        outcome
    }

    /// Fetches the heads `dst` lacks from `src`, as the daemon does on an
    /// announcement. Returns whether anything was fetched.
    async fn sync_missing(dst: &Arc<MeshRepo>, src: &Arc<MeshRepo>) -> bool {
        let mut wants = Vec::new();
        for head in src.op_heads().await.unwrap() {
            if !dst.has_operation(&head).await.unwrap() {
                wants.push(head);
            }
        }
        if wants.is_empty() {
            return false;
        }
        sync_once(dst, src, &wants).await;
        true
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
            GitTransferFormat::Loose,
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

    /// The join flow: a freshly initialized non-colocated repo with a
    /// renamed workspace pulls the full mesh state as a pack; jj then
    /// merges the fresh workspace into the replicated history on the next
    /// command.
    #[tokio::test]
    async fn join_pull_into_fresh_repo() {
        let fx = Fixture::new();
        let a = fx.init_repo("a");
        // Real file content so the pack carries blobs and trees.
        fs::write(a.join("file.txt"), "mesh content\n").unwrap();
        fx.jj(&a, &["commit", "-m", "add file"]);
        fx.jj(&a, &["bookmark", "create", "main", "-r", "@-"]);
        fx.jj(&a, &["new", "-m", "second"]);

        let b = init_join_repo(&fx, "b", "machine-b");

        let (ra, rb) = (open(&a), open(&b));
        let wants = ra.op_heads().await.unwrap();
        let outcome = sync_once_as(&rb, &ra, &wants, GitTransferFormat::Pack).await;
        assert_eq!(outcome.published, wants);
        assert!(outcome.git_objects > 0);

        // The objects landed as one pack, not loose, and the `.keep` file
        // protecting it was removed once the apply published.
        let pack_dir = rb
            .git_backend()
            .git_repo()
            .objects
            .store_ref()
            .path()
            .join("pack");
        let names: Vec<std::path::PathBuf> = fs::read_dir(&pack_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        let has_ext = |ext: &str| {
            names
                .iter()
                .any(|name| name.extension().is_some_and(|e| e == ext))
        };
        assert!(
            has_ext("pack") && has_ext("idx"),
            "expected a pack and its index, got {names:?}",
        );
        assert!(
            !has_ext("keep"),
            "keep file must be removed after apply: {names:?}",
        );

        // Divergent by construction: init ops are not mesh ancestors.
        assert_eq!(rb.op_heads().await.unwrap().len(), 2);

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

    /// Rewriting synced ancestors of another machine's working copy
    /// (describe, squash, sign) must arrive as a clean rebase of the tip,
    /// not as divergent changes. This holds only because joined repos are
    /// never colocated: the view's `git_head` mirrors the colocated
    /// `.git`'s machine-local HEAD, and a second colocated checkout makes
    /// jj re-import its own HEAD after every sync, resurrecting the
    /// rewritten commits as divergent changes and ping-ponging
    /// `import git head` operations between the machines forever.
    #[tokio::test]
    async fn ancestor_rewrite_syncs_without_divergence() {
        let fx = Fixture::new();
        // The adding machine keeps jj's default (colocated) layout.
        let a = fx.init_repo("a");
        let b = init_join_repo(&fx, "b", "machine-b");
        let (ra, rb) = (open(&a), open(&b));
        sync_missing(&rb, &ra).await;
        // Merge the join divergence on b and settle both sides.
        fx.jj(&b, &["status"]);
        sync_missing(&ra, &rb).await;
        fx.jj(&a, &["status"]);

        // A stack on a, with a's working copy on its tip.
        fs::write(a.join("f.txt"), "one\n").unwrap();
        fx.jj(&a, &["commit", "-m", "change-A"]);
        fs::write(a.join("f.txt"), "two\n").unwrap();
        fx.jj(&a, &["commit", "-m", "change-B"]);
        fx.jj(&a, &["describe", "-m", "change-C"]);
        sync_missing(&rb, &ra).await;

        // b rewrites the middle of the stack; jj rebases the tip.
        fx.jj(
            &b,
            &[
                "describe",
                "-r",
                "subject(\"change-B\")",
                "-m",
                "change-B-upd",
            ],
        );
        sync_missing(&ra, &rb).await;

        // a sees a fast-forward with the tip rebased onto the update: one
        // op head, no divergent changes, no stray branch.
        assert_eq!(ra.op_heads().await.unwrap().len(), 1);
        let log = fx.jj_output(&a, &["log", "-r", "all()"]);
        assert!(log.contains("change-B-upd"), "{log}");
        assert!(!log.contains("divergent"), "{log}");
        assert_eq!(log.matches("change-C").count(), 1, "{log}");

        // And the mesh settles: a's jj command produced nothing new to
        // sync, so no import ops ping-pong between the machines.
        assert!(!sync_missing(&rb, &ra).await, "unexpected op churn on a");
    }

    /// A pack header's object count must be rejected before gix sizes its
    /// delta tree from it: unchecked, the twelve bytes below name a
    /// multi-gigabyte allocation, and running out of memory aborts the
    /// process instead of failing the fetch.
    #[test]
    fn rejects_pack_header_declaring_absurd_object_count() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        let git = open(&dir).git_backend().git_repo();

        let mut header = Vec::from(*b"PACK");
        header.extend_from_slice(&2u32.to_be_bytes());
        header.extend_from_slice(&u32::MAX.to_be_bytes());

        let Err(err) = pack::ingest_pack(&git, std::io::Cursor::new(header)) else {
            panic!("an absurd object count must be refused");
        };
        assert!(err.to_string().contains("over the"), "{err:#}");
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

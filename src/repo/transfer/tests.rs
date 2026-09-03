//! End-to-end transfer tests: fetch/serve exchanges over in-memory stream
//! pairs against real jj repos, as the daemon runs them over QUIC.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use jj_lib::object_id::ObjectId as _;

use super::*;
use crate::{
    net::{
        fetch::{
            FetchRequest, GitFrame, GitRequest, GitTransferFormat, MAX_GIT_FRAME_SIZE,
            MAX_OP_FRAME_SIZE, OpFrame,
        },
        wire::{read_message, write_message},
    },
    repo::{JjRepo, OpenRepo},
    testing::Fixture,
};

/// Network deadline passed to test fetches; generous, never meant to fire.
const NET_TIMEOUT: Duration = Duration::from_mins(1);

fn open(dir: &Path) -> Arc<OpenRepo> {
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
    fetcher: &Arc<OpenRepo>,
    server: &Arc<OpenRepo>,
    wants: &[OperationId],
) -> FetchOutcome {
    sync_once_as(fetcher, server, wants, GitTransferFormat::Loose).await
}

/// [`sync_once`] with an explicit git transfer format.
async fn sync_once_as(
    fetcher: &Arc<OpenRepo>,
    server: &Arc<OpenRepo>,
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
        RepoIdent {
            name: "test",
            id: &crate::config::RepoId::generate(),
        },
        wants,
        FetchOptions {
            format,
            net_timeout: NET_TIMEOUT,
        },
        &mut client_tx,
        &mut client_rx,
        ProgressSink::default(),
    )
    .await
    .unwrap();
    serve_task.await.unwrap();
    outcome
}

/// Asserts every op head of `repo` has its commit index, so no jj command
/// pays for a rebuild after a sync.
async fn assert_heads_indexed(repo: &Arc<OpenRepo>) {
    for head in repo.op_heads().await.unwrap() {
        assert!(
            repo.has_commit_index(&head).await,
            "op head {} published without a commit index",
            head.hex(),
        );
    }
}

/// Fetches the heads `dst` lacks from `src`, as the daemon does on an
/// announcement. Returns whether anything was fetched.
async fn sync_missing(dst: &Arc<OpenRepo>, src: &Arc<OpenRepo>) -> bool {
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

    assert!(outcome.ops > 0);
    assert!(outcome.git_objects > 0);
    // The wanted heads were published: they superseded b's old head.
    assert_eq!(rb.op_heads().await.unwrap(), wants);
    // And they were indexed before publication.
    assert_heads_indexed(&rb).await;

    // jj itself must accept the synced repo: log walks commits, which
    // requires the git objects and the change-id extras. The fork
    // shares the workspace name with `a` (real machines never do; the
    // clone flow assigns unique names), so b's working copy is
    // legitimately stale and skipped here.
    fx.jj(&b, &["op", "log", "--ignore-working-copy"]);
    fx.jj(&b, &["log", "-r", "all()", "--ignore-working-copy"]);

    // Re-fetching the same heads is a no-op.
    let again = sync_once(&rb, &ra, &wants).await;
    assert_eq!(again.ops, 0);
    assert_eq!(rb.op_heads().await.unwrap(), wants);
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
    assert_heads_indexed(&rb).await;
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

/// A git HEAD symbolic to a branch the mirror moves must be detached at
/// its current commit first, as jj's export does: otherwise git's index
/// and worktree fall behind the branch, and jj's next import reads the
/// jump as a checkout.
#[tokio::test]
async fn mirror_detaches_head_before_moving_its_branch() {
    let fx = Fixture::new();
    let dir_a = fx.path().join("a");
    fx.jj(fx.path(), &["git", "init", "--colocate", "a"]);
    fx.jj(&dir_a, &["describe", "-m", "base"]);
    fx.jj(&dir_a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&dir_a, &["new", "-m", "export"]);
    fork(&dir_a, &fx.path().join("b"));
    let dir_b = fx.path().join("b");

    // The user checks the branch out in b's colocated git.
    git(
        &dir_b.join(".git"),
        &["symbolic-ref", "HEAD", "refs/heads/main"],
    );
    let checked_out = git_rev(&dir_b, "refs/heads/main");

    fs::write(dir_a.join("file.txt"), "moved\n").unwrap();
    fx.jj(&dir_a, &["commit", "-m", "advance"]);
    fx.jj(&dir_a, &["bookmark", "set", "main", "-r", "@-"]);
    fx.jj(&dir_a, &["new", "-m", "trigger export"]);

    let (ra, rb) = (open(&dir_a), open(&dir_b));
    let wants = ra.op_heads().await.unwrap();
    sync_once(&rb, &ra, &wants).await;

    assert_eq!(
        git_rev(&dir_b, "refs/heads/main"),
        git_rev(&dir_a, "refs/heads/main")
    );
    assert_eq!(git_rev(&dir_b, "HEAD"), checked_out);
    assert!(
        !git_ok(&dir_b.join(".git"), &["symbolic-ref", "-q", "HEAD"]),
        "HEAD must be detached"
    );
}

/// Resolves `rev` in the colocated `.git` of `dir`.
fn git_rev(dir: &Path, rev: &str) -> String {
    git_rev_at(&dir.join(".git"), rev)
}

/// Resolves `rev` in the git repo at `git_dir`.
fn git_rev_at(git_dir: &Path, rev: &str) -> String {
    let out = Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
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

        use crate::net::fetch::compress_payload;
        let _request: FetchRequest = read_message(&mut server_rx, MAX_OP_FRAME_SIZE)
            .await
            .unwrap();
        let view = jj_lib::protos::simple_op_store::View::default().encode_to_vec();
        let frames = [
            OpFrame::View {
                id: vec![9; 64],
                view: compress_payload(&view).unwrap(),
            },
            // The wanted op, legitimately parented on the local head.
            OpFrame::Op {
                id: vec![1; 64],
                op: compress_payload(&make_op(vec![head_bytes.clone()])).unwrap(),
            },
            // The poison: parented on the local head, but nothing
            // connects it to the want.
            OpFrame::Op {
                id: vec![2; 64],
                op: compress_payload(&make_op(vec![head_bytes])).unwrap(),
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
        RepoIdent {
            name: "test",
            id: &crate::config::RepoId::generate(),
        },
        &[want],
        FetchOptions {
            format: GitTransferFormat::Loose,
            net_timeout: NET_TIMEOUT,
        },
        &mut client_tx,
        &mut client_rx,
        ProgressSink::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("unreachable"), "{err:#}");
    server.await.unwrap();

    // Nothing was published: the local head is untouched.
    assert_eq!(repo.op_heads().await.unwrap(), vec![local_head]);
}

/// A peer that omits git objects its views reference must fail the fetch
/// before anything is staged: published, such a head could never be
/// loaded or indexed.
#[tokio::test]
async fn rejects_fetch_when_referenced_git_objects_are_missing() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let repo = open(&dir);
    let local_head = repo.op_heads().await.unwrap().remove(0);

    let (client, remote) = tokio::io::duplex(1 << 20);
    let (mut client_rx, mut client_tx) = tokio::io::split(client);
    let (mut server_rx, mut server_tx) = tokio::io::split(remote);

    let parent = local_head.clone();
    let server = tokio::spawn(async move {
        use prost::Message as _;

        use crate::net::fetch::compress_payload;
        let _request: FetchRequest = read_message(&mut server_rx, MAX_OP_FRAME_SIZE)
            .await
            .unwrap();
        // A view whose head names a commit the git phase never delivers.
        let view = jj_lib::protos::simple_op_store::View {
            head_ids: vec![vec![0xAB; 20]],
            ..Default::default()
        }
        .encode_to_vec();
        let op = jj_lib::protos::simple_op_store::Operation {
            view_id: vec![9; 64],
            parents: vec![parent.as_bytes().to_vec()],
            metadata: Some(jj_lib::protos::simple_op_store::OperationMetadata {
                description: "crafted".to_owned(),
                hostname: "evil".to_owned(),
                username: "evil".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec();
        let frames = [
            OpFrame::View {
                id: vec![9; 64],
                view: compress_payload(&view).unwrap(),
            },
            OpFrame::Op {
                id: vec![1; 64],
                op: compress_payload(&op).unwrap(),
            },
            OpFrame::Done,
        ];
        for frame in frames {
            write_message(&mut server_tx, &frame, MAX_OP_FRAME_SIZE)
                .await
                .unwrap();
        }
        // Answer the git request without sending the object it asks for.
        let _git: GitRequest = read_message(&mut server_rx, MAX_GIT_FRAME_SIZE)
            .await
            .unwrap();
        write_message(&mut server_tx, &GitFrame::Done, MAX_GIT_FRAME_SIZE)
            .await
            .unwrap();
    });

    let err = fetch(
        &repo,
        RepoIdent {
            name: "test",
            id: &crate::config::RepoId::generate(),
        },
        &[OperationId::new(vec![1; 64])],
        FetchOptions {
            format: GitTransferFormat::Loose,
            net_timeout: NET_TIMEOUT,
        },
        &mut client_tx,
        &mut client_rx,
        ProgressSink::default(),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("did not send"), "{err:#}");
    server.await.unwrap();

    // Nothing was staged or published: the crafted op was never stored
    // and the local head is untouched.
    let want = OperationId::new(vec![1; 64]);
    assert!(!repo.has_operation(&want).await.unwrap());
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

/// The clone flow: a freshly initialized non-colocated repo with a
/// renamed workspace pulls the full mesh state as a pack; jj then
/// merges the fresh workspace into the replicated history on the next
/// command.
#[tokio::test]
async fn clone_pull_into_fresh_repo() {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    // Real file content so the pack carries blobs and trees.
    fs::write(a.join("file.txt"), "mesh content\n").unwrap();
    fx.jj(&a, &["commit", "-m", "add file"]);
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@-"]);
    fx.jj(&a, &["new", "-m", "second"]);

    let b = fx.init_pull_target("b", "machine-b");

    let (ra, rb) = (open(&a), open(&b));
    let wants = ra.op_heads().await.unwrap();
    let init_heads = rb.op_heads().await.unwrap();
    let outcome = sync_once_as(&rb, &ra, &wants, GitTransferFormat::Pack).await;
    assert!(outcome.git_objects > 0);

    // The wanted head was published next to the fresh repo's init head.
    let mut expected: Vec<OperationId> = wants.clone();
    expected.extend(init_heads);
    expected.sort_unstable();
    let mut heads = rb.op_heads().await.unwrap();
    heads.sort_unstable();
    assert_eq!(heads, expected);

    // The commit index was built for the published head: the fetch, not
    // the user's next jj command, paid for indexing the pulled history.
    let op_link = b.join(".jj/repo/index/op_links").join(wants[0].hex());
    assert!(op_link.is_file(), "missing index op link {op_link:?}");

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
    let list = Command::new(crate::repo::jj_bin())
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
/// not as divergent changes, with no op churn once both sides settle.
#[tokio::test]
async fn ancestor_rewrite_syncs_without_divergence() {
    let fx = Fixture::new();
    // The adding machine keeps jj's default (colocated) layout.
    let a = fx.init_repo("a");
    let b = fx.init_pull_target("b", "machine-b");
    let (ra, rb) = (open(&a), open(&b));
    sync_missing(&rb, &ra).await;
    // Merge the clone divergence on b and settle both sides.
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

    let indexed = pack::SharedObjectCount::default();
    let Err(err) = pack::ingest_pack(&git, std::io::Cursor::new(header), &indexed) else {
        panic!("an absurd object count must be refused");
    };
    assert!(err.to_string().contains("over the"), "{err:#}");
}

/// A pack-format fetch with a progress sink must report the phases in
/// order with exact totals: the op counts the server announced, and
/// the git object count read from the pack header, both fully reached.
/// The clone scenario is the one the progress display exists for.
#[tokio::test]
async fn progress_reports_phases_and_exact_totals() {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    fs::write(a.join("file.txt"), "mesh content\n").unwrap();
    fx.jj(&a, &["commit", "-m", "add file"]);
    let b = fx.init_pull_target("b", "machine-b");

    let (ra, rb) = (open(&a), open(&b));
    let wants = ra.op_heads().await.unwrap();

    let (client, remote) = tokio::io::duplex(1 << 20);
    let (mut client_rx, mut client_tx) = tokio::io::split(client);
    let (mut server_rx, mut server_tx) = tokio::io::split(remote);
    let serve_task = tokio::spawn(async move {
        let request: FetchRequest = read_message(&mut server_rx, MAX_OP_FRAME_SIZE)
            .await
            .unwrap();
        serve(&ra, request, &mut server_tx, &mut server_rx)
            .await
            .unwrap();
    });

    let samples = std::sync::Mutex::new(Vec::<TransferProgress>::new());
    let sink = |progress: TransferProgress| samples.lock().unwrap().push(progress);
    let outcome = fetch(
        &rb,
        RepoIdent {
            name: "test",
            id: &crate::config::RepoId::generate(),
        },
        &wants,
        FetchOptions {
            format: GitTransferFormat::Pack,
            net_timeout: NET_TIMEOUT,
        },
        &mut client_tx,
        &mut client_rx,
        ProgressSink::new(&sink),
    )
    .await
    .unwrap();
    serve_task.await.unwrap();
    let samples = samples.into_inner().unwrap();

    // The phases arrive strictly in order; the pulled head is new to the
    // fresh repo, so the index phase must run before publication.
    let mut phases: Vec<TransferPhase> = samples.iter().map(|p| p.phase).collect();
    phases.dedup();
    assert_eq!(
        phases,
        [
            TransferPhase::Ops,
            TransferPhase::Git,
            TransferPhase::Apply,
            TransferPhase::Index,
        ],
    );

    // Counters are monotonic within each phase.
    for pair in samples.windows(2) {
        if pair[0].phase == pair[1].phase {
            assert!(pair[1].current >= pair[0].current, "{pair:?}");
            assert!(pair[1].bytes >= pair[0].bytes, "{pair:?}");
        }
    }

    // Op phase: the announced total is exact and reached, and covers
    // at least the ops the fetch stored.
    let last_ops = samples
        .iter()
        .rfind(|p| p.phase == TransferPhase::Ops)
        .unwrap();
    assert_eq!(last_ops.total, Some(last_ops.current));
    assert!(last_ops.current >= outcome.ops as u64);
    assert!(last_ops.bytes > 0);

    // Git phase: the total comes off the pack header on the first
    // chunk, and the indexed count reaches it once the pack landed.
    let git: Vec<&TransferProgress> = samples
        .iter()
        .filter(|p| p.phase == TransferPhase::Git)
        .collect();
    let announced = git.iter().find(|p| p.total.is_some()).unwrap();
    assert_eq!(announced.total, Some(outcome.git_objects as u64));
    let last_git = git.last().unwrap();
    assert_eq!(last_git.total, Some(last_git.current));
    assert!(last_git.current > 0);
    assert!(last_git.bytes > 0);
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
    sync_once(&rb, &ra, &wants).await;
    // The wanted heads were published: they superseded b's old head.
    assert_eq!(rb.op_heads().await.unwrap(), wants);

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

/// A colocated pull target with `main` and `feat` bookmarks pulled from
/// `a`, settled on both sides (the first pull is divergent by
/// construction, and a jj command on each side merges it).
async fn settled_colocated_pair(fx: &Fixture) -> (PathBuf, PathBuf, Arc<OpenRepo>, Arc<OpenRepo>) {
    let a = fx.init_repo("a");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&a, &["bookmark", "create", "feat", "-r", "@"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.init_colocated_pull_target("b", "machine-b");
    let (ra, rb) = (open(&a), open(&b));
    assert!(ra.is_colocated());
    assert!(rb.is_colocated());
    sync_missing(&rb, &ra).await;
    fx.jj(&b, &["status"]);
    sync_missing(&ra, &rb).await;
    fx.jj(&a, &["status"]);
    (a, b, ra, rb)
}

/// Moves `bookmark` in `dir` forward to a new commit and exports it to
/// git, returning the git id.
fn move_bookmark(fx: &Fixture, dir: &Path, bookmark: &str, file: &str) -> String {
    fx.jj(dir, &["new", bookmark]);
    fx.commit_file(dir, file, &format!("{bookmark} moved"));
    fx.jj(dir, &["bookmark", "set", bookmark, "-r", "@-"]);
    fx.jj(dir, &["new", "-m", "export"]);
    git_rev(dir, &format!("refs/heads/{bookmark}"))
}

/// A colocated clone's first pull is divergent by construction (its init
/// ops share only the root with the mesh history), so the mirror must
/// still land the mesh's git refs in its `.git`: otherwise jj's next
/// import reads the missing refs as deletions and wipes the bookmarks
/// mesh-wide.
#[tokio::test]
async fn colocated_pull_target_keeps_mesh_bookmarks() {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.init_colocated_pull_target("b", "machine-b");
    let (ra, rb) = (open(&a), open(&b));

    sync_missing(&rb, &ra).await;
    assert_eq!(
        git_rev(&b, "refs/heads/main"),
        git_rev(&a, "refs/heads/main")
    );

    // jj merges the divergent heads and imports git: nothing to import.
    fx.jj(&b, &["status"]);
    let bookmarks = fx.jj_output(&b, &["bookmark", "list"]);
    assert!(bookmarks.contains("main"), "{bookmarks}");
    sync_missing(&ra, &rb).await;
    fx.jj(&a, &["status"]);
    let bookmarks = fx.jj_output(&a, &["bookmark", "list"]);
    assert!(bookmarks.contains("main"), "{bookmarks}");
}

/// Two colocated instances changing refs concurrently: the sync is
/// divergent, and the mirror must still apply the refs only the peer
/// changed, while leaving the ones changed locally to jj's merge.
#[tokio::test]
async fn divergent_sync_mirrors_refs_the_peer_changed() {
    let fx = Fixture::new();
    let (a, b, ra, rb) = settled_colocated_pair(&fx).await;

    // a moves main and b moves feat, concurrently.
    let a_main = move_bookmark(&fx, &a, "main", "a.txt");
    let b_feat = move_bookmark(&fx, &b, "feat", "b.txt");

    sync_missing(&rb, &ra).await;
    assert_eq!(rb.op_heads().await.unwrap().len(), 2);
    // The peer's move landed in b's .git; b's own move stayed.
    assert_eq!(git_rev(&b, "refs/heads/main"), a_main);
    assert_eq!(git_rev(&b, "refs/heads/feat"), b_feat);

    // jj merges and imports: both moves hold, nothing reverts.
    fx.jj(&b, &["status"]);
    assert_eq!(git_rev(&b, "refs/heads/main"), a_main);
    assert_eq!(git_rev(&b, "refs/heads/feat"), b_feat);
    sync_missing(&ra, &rb).await;
    fx.jj(&a, &["status"]);
    assert_eq!(git_rev(&a, "refs/heads/main"), a_main);
    assert_eq!(git_rev(&a, "refs/heads/feat"), b_feat);
    assert_eq!(ra.op_heads().await.unwrap().len(), 1);
}

/// A receiver left with two op heads by a divergent sync (the daemon
/// runs no jj command while divergent) must still mirror the next peer
/// change: with the mirror skipped, jj's merge would revert it.
#[tokio::test]
async fn already_divergent_receiver_still_mirrors() {
    let fx = Fixture::new();
    let (a, b, ra, rb) = settled_colocated_pair(&fx).await;

    move_bookmark(&fx, &a, "main", "a1.txt");
    fx.jj(&b, &["new", "-m", "concurrent on b"]);
    sync_missing(&rb, &ra).await;
    assert_eq!(rb.op_heads().await.unwrap().len(), 2);

    // a moves main again while b is still divergent.
    let a_main = move_bookmark(&fx, &a, "main", "a2.txt");
    sync_missing(&rb, &ra).await;
    assert_eq!(git_rev(&b, "refs/heads/main"), a_main);

    fx.jj(&b, &["status"]);
    assert_eq!(git_rev(&b, "refs/heads/main"), a_main);
    sync_missing(&ra, &rb).await;
    fx.jj(&a, &["status"]);
    assert_eq!(git_rev(&a, "refs/heads/main"), a_main);
}

/// Both sides moving a ref along one line (b's target descends from a's,
/// with the commits reaching b through git rather than the mesh) is no
/// conflict for jj, which picks the descendant. The mirror must land it in
/// a's `.git` too, or a's next import reverts b's move.
#[tokio::test]
async fn divergent_moves_along_one_line_mirror_the_descendant() {
    let fx = Fixture::new();
    let (a, b, ra, rb) = settled_colocated_pair(&fx).await;

    let a_main = move_bookmark(&fx, &a, "main", "a.txt");
    let fetch = Command::new("git")
        .current_dir(&b)
        .arg("fetch")
        .arg(a.join(".git"))
        .arg("main:refs/heads/tmp")
        .status()
        .unwrap();
    assert!(fetch.success());
    fx.jj(&b, &["status"]);
    fx.jj(&b, &["new", "tmp"]);
    fx.commit_file(&b, "b.txt", "main moved again");
    fx.jj(&b, &["bookmark", "set", "main", "-r", "@-"]);
    fx.jj(&b, &["bookmark", "delete", "tmp"]);
    fx.jj(&b, &["new", "-m", "export"]);
    let b_main = git_rev(&b, "refs/heads/main");
    assert_ne!(a_main, b_main);

    sync_missing(&ra, &rb).await;
    assert_eq!(ra.op_heads().await.unwrap().len(), 2);
    assert_eq!(git_rev(&a, "refs/heads/main"), b_main);
    // jj reconciles the divergence and finds nothing to import.
    fx.jj(&a, &["status"]);
    assert_eq!(git_rev(&a, "refs/heads/main"), b_main);
    let main = fx.jj_output(&a, &["log", "-r", "main", "--no-graph", "-T", "commit_id"]);
    assert_eq!(main, b_main);
}

/// A non-colocated pull target must mirror refs into its backing git
/// repo too: jj only imports git when the user enables colocation, and
/// then reads every ref the replicated view lists but git lacks as a
/// deletion, abandoning the commits only those refs reached.
#[tokio::test]
async fn non_colocated_pull_target_survives_colocation_enable() {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.init_pull_target("b", "machine-b");
    let (ra, rb) = (open(&a), open(&b));
    assert!(!rb.is_colocated());

    sync_missing(&rb, &ra).await;
    let expected = git_rev(&a, "refs/heads/main");
    assert_eq!(git_rev_at(&store_git_dir(&b), "refs/heads/main"), expected);

    fx.jj(&b, &["status"]);
    fx.jj(&b, &["git", "colocation", "enable"]);
    let bookmarks = fx.jj_output(&b, &["bookmark", "list"]);
    assert!(bookmarks.contains("main"), "{bookmarks}");
    assert_eq!(git_rev(&b, "refs/heads/main"), expected);
}

/// Healing brings a stale non-colocated store in line with the merged
/// view: missing refs come back and leftover ones go.
#[tokio::test]
async fn heal_repairs_stale_refs_of_non_colocated_store() {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.init_pull_target("b", "machine-b");
    let (ra, rb) = (open(&a), open(&b));
    sync_missing(&rb, &ra).await;
    fx.jj(&b, &["status"]);

    let store = store_git_dir(&b);
    let expected = git_rev_at(&store, "refs/heads/main");
    git(&store, &["update-ref", "-d", "refs/heads/main"]);
    git(&store, &["update-ref", "refs/heads/stale", &expected]);

    let heal = rb.clone();
    tokio::task::spawn_blocking(move || mirror::heal(&heal))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(git_rev_at(&store, "refs/heads/main"), expected);
    assert!(!git_ok(
        &store,
        &["rev-parse", "--verify", "refs/heads/stale"]
    ));

    // jj sees nothing to import.
    fx.jj(&b, &["git", "colocation", "enable"]);
    let bookmarks = fx.jj_output(&b, &["bookmark", "list"]);
    assert!(bookmarks.contains("main"), "{bookmarks}");
    assert!(!bookmarks.contains("stale"), "{bookmarks}");
}

/// Healing never touches a colocated `.git`: a ref missing there may be
/// a deletion the user made in git that jj has not imported yet.
#[tokio::test]
async fn heal_leaves_colocated_git_alone() {
    let fx = Fixture::new();
    let (_a, b, _ra, rb) = settled_colocated_pair(&fx).await;
    let git_dir = b.join(".git");
    git(&git_dir, &["update-ref", "-d", "refs/heads/feat"]);

    let heal = rb.clone();
    tokio::task::spawn_blocking(move || mirror::heal(&heal))
        .await
        .unwrap()
        .unwrap();
    assert!(!git_ok(
        &git_dir,
        &["rev-parse", "--verify", "refs/heads/feat"]
    ));
}

/// The backing git repo of a non-colocated jj repo.
fn store_git_dir(dir: &Path) -> PathBuf {
    dir.join(".jj/repo/store/git")
}

/// Runs a git command against `git_dir`, panicking on failure.
fn git(git_dir: &Path, args: &[&str]) {
    assert!(git_ok(git_dir, args), "git {args:?} failed in {git_dir:?}");
}

/// Runs a git command against `git_dir`, returning whether it succeeded.
fn git_ok(git_dir: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .arg("--git-dir")
        .arg(git_dir)
        .args(args)
        .output()
        .is_ok_and(|out| out.status.success())
}

/// jj records a tag by the commit it peels to, while git stores the tag
/// object of an annotated tag: the mirror must swap against the stored
/// form, or a moved or deleted annotated tag never follows the mesh.
#[tokio::test]
async fn mirror_deletes_annotated_tags() {
    let fx = Fixture::new();
    let (a, b, ra, rb) = settled_colocated_pair(&fx).await;
    let commit = git_rev(&a, "refs/heads/main");
    git(&a.join(".git"), &["tag", "-a", "v1", "-m", "v1", &commit]);
    fx.jj(&a, &["status"]);
    sync_missing(&rb, &ra).await;
    assert_eq!(git_rev(&b, "refs/tags/v1^{commit}"), commit);

    // b stores the same tag annotated, as a git clone would.
    let b_git = b.join(".git");
    git(&b_git, &["tag", "-d", "v1"]);
    git(&b_git, &["tag", "-a", "v1", "-m", "v1", &commit]);
    fx.jj(&b, &["status"]);
    sync_missing(&ra, &rb).await;
    fx.jj(&a, &["status"]);

    git(&a.join(".git"), &["tag", "-d", "v1"]);
    fx.jj(&a, &["status"]);
    sync_missing(&rb, &ra).await;
    assert!(!git_ok(&b_git, &["rev-parse", "--verify", "refs/tags/v1"]));
}

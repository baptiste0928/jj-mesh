//! Tests running typical synchronization scenarios with both machines online.

mod harness;

use std::fs;

use harness::{TestMesh, add_and_clone, descriptions, op_heads, wait_converged};

/// Cloning a repo added on another machine pulls its full history.
#[tokio::test]
async fn clone_pulls_full_history() {
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;

    // A registers a repo with a committed file and a bookmark.
    let dir_a = mesh.jj.init_repo("proj");
    fs::write(dir_a.join("file.txt"), "from a\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "add file"]);
    mesh.jj
        .jj(&dir_a, &["bookmark", "create", "main", "-r", "@-"]);
    a.add_repo("proj", &dir_a).await;

    // B clones it by name, as `jj-mesh repo clone` does: fresh repo with a
    // machine-unique workspace, merged by the next jj command.
    b.wait_available("proj").await;
    let dir_b = mesh.jj.init_pull_target("proj-b", "machine-b");
    b.clone_repo("proj", &dir_b).await;
    mesh.jj.jj(&dir_b, &["status"]);
    wait_converged(&dir_a, &dir_b).await;

    // The replicated history is fully usable on B.
    assert!(descriptions(&mesh, &dir_b).contains("add file"));
    let file = mesh
        .jj
        .jj_output(&dir_b, &["file", "show", "-r", "main", "file.txt"]);
    assert_eq!(file, "from a\n");
}

/// Commits made on either machine replicate to the other.
#[tokio::test]
async fn commits_replicate_both_ways() {
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;
    let (dir_a, dir_b) = add_and_clone(&mesh, &a, &b, "proj").await;

    // A commit on A arrives on B.
    mesh.jj.commit_file(&dir_a, "a.txt", "from a");
    wait_converged(&dir_a, &dir_b).await;
    assert!(descriptions(&mesh, &dir_b).contains("from a"));

    // And a commit on B arrives on A.
    mesh.jj.commit_file(&dir_b, "b.txt", "from b");
    wait_converged(&dir_a, &dir_b).await;
    assert!(descriptions(&mesh, &dir_a).contains("from b"));
}

/// Concurrent commits replicate as divergent op heads and jj merges them.
#[tokio::test]
async fn concurrent_commits_reconcile() {
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;
    let (dir_a, dir_b) = add_and_clone(&mesh, &a, &b, "proj").await;

    // Both sides commit within the watch debounce, so the op logs
    // typically diverge before the daemons exchange the heads; both
    // machines must settle on the same op head set either way.
    mesh.jj.commit_file(&dir_a, "a.txt", "concurrent a");
    mesh.jj.commit_file(&dir_b, "b.txt", "concurrent b");
    wait_converged(&dir_a, &dir_b).await;

    // Any jj command merges the divergence; the merged history holding
    // both commits replicates back.
    mesh.jj.jj(&dir_a, &["log", "-r", "all()"]);
    wait_converged(&dir_a, &dir_b).await;
    assert_eq!(op_heads(&dir_a).await.len(), 1, "op heads merged");
    let log = descriptions(&mesh, &dir_b);
    assert!(log.contains("concurrent a"), "{log}");
    assert!(log.contains("concurrent b"), "{log}");
}

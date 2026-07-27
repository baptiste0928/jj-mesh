//! Tests running typical synchronization scenarios with both machines online.

mod harness;

use std::fs;

use harness::{
    TestMesh, add_and_join, connect, descriptions, init_join_repo, op_heads, wait_converged,
};

/// Joining a repo added on another machine pulls its full history.
#[tokio::test(flavor = "multi_thread")]
async fn join_pulls_full_history() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    connect(&a, &b).await;

    // A registers a repo with a committed file and a bookmark.
    let dir_a = mesh.jj.init_repo("proj");
    fs::write(dir_a.join("file.txt"), "from a\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "add file"]);
    mesh.jj
        .jj(&dir_a, &["bookmark", "create", "main", "-r", "@-"]);
    a.add_repo("proj", &dir_a).await;

    // B joins it by name, as `jj-mesh join` does: fresh repo with a
    // machine-unique workspace, merged by the next jj command.
    b.wait_available("proj").await;
    let dir_b = init_join_repo(&mesh, "proj-b", "machine-b");
    b.join_repo("proj", &dir_b).await;
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
#[tokio::test(flavor = "multi_thread")]
async fn commits_replicate_both_ways() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    connect(&a, &b).await;
    let dir_a = mesh.jj.init_repo("proj");
    let dir_b = add_and_join(&mesh, &a, &b, &dir_a, "proj").await;

    // A commit on A arrives on B.
    fs::write(dir_a.join("a.txt"), "a\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "from a"]);
    wait_converged(&dir_a, &dir_b).await;
    assert!(descriptions(&mesh, &dir_b).contains("from a"));

    // And a commit on B arrives on A.
    fs::write(dir_b.join("b.txt"), "b\n").unwrap();
    mesh.jj.jj(&dir_b, &["commit", "-m", "from b"]);
    wait_converged(&dir_a, &dir_b).await;
    assert!(descriptions(&mesh, &dir_a).contains("from b"));
}

/// Concurrent commits replicate as divergent op heads and jj merges them.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_commits_reconcile() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    connect(&a, &b).await;
    let dir_a = mesh.jj.init_repo("proj");
    let dir_b = add_and_join(&mesh, &a, &b, &dir_a, "proj").await;

    // Both sides commit within the watch debounce, so the op logs
    // typically diverge before the daemons exchange the heads; both
    // machines must settle on the same op head set either way.
    fs::write(dir_a.join("a.txt"), "a\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "concurrent a"]);
    fs::write(dir_b.join("b.txt"), "b\n").unwrap();
    mesh.jj.jj(&dir_b, &["commit", "-m", "concurrent b"]);
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

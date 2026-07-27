//! Tests covering sync recovery for machines that were offline.

mod harness;

use std::fs;

use harness::{TestMesh, add_and_join, connect, descriptions, op_heads, wait_converged};

/// Commits made while a machine's daemon was down are exchanged after
/// restart: the histories diverged offline, converge on reconnect, and
/// jj merges them.
#[tokio::test(flavor = "multi_thread")]
async fn offline_machine_catches_up() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let mut b = mesh.machine("machine-b").await;
    connect(&a, &b).await;
    let dir_a = mesh.jj.init_repo("proj");
    let dir_b = add_and_join(&mesh, &a, &b, &dir_a, "proj").await;

    // Commits land on both sides while B's daemon is down, so the op
    // logs genuinely diverge.
    b.stop().await;
    fs::write(dir_a.join("a.txt"), "a\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "while b was down"]);
    fs::write(dir_b.join("b.txt"), "b\n").unwrap();
    mesh.jj.jj(&dir_b, &["commit", "-m", "offline on b"]);
    assert_ne!(op_heads(&dir_a).await, op_heads(&dir_b).await);

    // B restarts with the same identity and state, reconnects on its
    // own, and the missed operations are exchanged both ways.
    b.start().await;
    wait_converged(&dir_a, &dir_b).await;

    // jj merges the divergence; the merged history holding both offline
    // commits replicates back.
    mesh.jj.jj(&dir_b, &["log", "-r", "all()"]);
    wait_converged(&dir_a, &dir_b).await;
    assert_eq!(op_heads(&dir_a).await.len(), 1, "op heads merged");
    let log = descriptions(&mesh, &dir_a);
    assert!(log.contains("while b was down"), "{log}");
    assert!(log.contains("offline on b"), "{log}");
}

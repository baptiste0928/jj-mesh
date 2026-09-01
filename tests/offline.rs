//! Tests covering sync recovery for machines that were offline.

mod harness;

use harness::{TestMesh, add_and_clone, descriptions, op_heads, wait_converged};

/// Commits made while a machine's daemon was down are exchanged after
/// restart: the histories diverged offline, converge on reconnect, and
/// jj merges them.
#[tokio::test]
async fn offline_machine_catches_up() {
    let mesh = TestMesh::new();
    let (a, mut b) = mesh.connected_pair().await;
    let (dir_a, dir_b) = add_and_clone(&mesh, &a, &b, "proj").await;

    // Commits land on both sides while B's daemon is down, so the op
    // logs genuinely diverge.
    b.stop().await;
    mesh.jj.commit_file(&dir_a, "a.txt", "while b was down");
    mesh.jj.commit_file(&dir_b, "b.txt", "offline on b");
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

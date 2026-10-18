//! Tests covering server mode.

mod harness;

use harness::TestMesh;

/// The server role reaches the peers through the gossip, both ways.
#[tokio::test]
async fn server_role_reaches_the_peers() {
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;
    let is_server = |s: &jj_mesh::daemon::control::Status| {
        s.peers.iter().any(|p| p.name == "machine-a" && p.server)
    };

    a.set_server(true).await;
    assert!(a.status().await.server);
    b.wait("machine-a as server", is_server).await;

    a.set_server(false).await;
    b.wait("machine-a as regular machine", |s| !is_server(s))
        .await;
}

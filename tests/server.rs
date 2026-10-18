//! Tests covering server mode.

mod harness;

use harness::{TestMesh, descriptions, wait_converged, wait_until};
use jj_mesh::{
    config::ObjectHash,
    daemon::control::{Request, Response, Status},
    repo::{Identity, JjRepo},
};

/// A server announces its role, replicates every mesh repo, the ones added
/// before server mode and after, whatever their object hash, keeps them in
/// sync, refuses to forget them, and deletes them once removed from the
/// mesh.
#[tokio::test]
async fn server_replicates_every_repo() {
    let mesh = TestMesh::new();
    let (a, server) = mesh.connected_pair().await;

    let early = mesh.jj.init_repo("early");
    a.add_repo("early", &early).await;
    server.wait_available("early").await;
    server.set_server(true).await;
    assert!(server.status().await.server);
    a.wait("machine-b as server", |s| is_server(s, "machine-b"))
        .await;
    // A SHA-256 repo gets a SHA-256 replica.
    let late = mesh.jj.init_repo_hashed("late", ObjectHash::Sha256);
    a.add_repo("late", &late).await;

    let early_replica = server.wait_repo("early").await;
    let late_replica = server.wait_repo("late").await;
    wait_converged(&early, &early_replica).await;
    wait_converged(&late, &late_replica).await;

    mesh.jj.commit_file(&late, "file.txt", "after replication");
    wait_converged(&late, &late_replica).await;

    let forgotten = server
        .try_request(&Request::ForgetRepo {
            name: "late".to_owned(),
        })
        .await;
    assert!(matches!(forgotten, Response::Error(_)), "{forgotten:?}");

    let removed = a
        .request(&Request::RemoveRepo {
            name: "late".to_owned(),
        })
        .await;
    assert!(
        matches!(removed, Response::RepoRemoved { .. }),
        "{removed:?}"
    );
    server
        .wait("late removed", |s| s.repos.iter().all(|r| r.name != "late"))
        .await;
    wait_until("late replica deleted", || !late_replica.exists()).await;
    assert!(early_replica.exists());
}

/// A workspace added to a replica syncs like any other, and keeps its
/// replica when server mode is disabled and the repo removed from the
/// mesh, unlike the replicas no workspace uses.
#[tokio::test]
async fn disabling_server_mode_keeps_replicas_in_use() {
    let mesh = TestMesh::new();
    let (a, server) = mesh.connected_pair().await;
    server.set_server(true).await;
    let used = mesh.jj.init_repo("used");
    a.add_repo("used", &used).await;
    let unused = mesh.jj.init_repo("unused");
    a.add_repo("unused", &unused).await;
    let used_replica = server.wait_repo("used").await;
    let unused_replica = server.wait_repo("unused").await;
    wait_converged(&used, &used_replica).await;

    // A workspace of the replica, as `jj-mesh clone` adds on a server.
    let workspace = mesh.jj.path().join("used-on-server");
    JjRepo::at(&used_replica)
        .unwrap()
        .add_workspace(&workspace, "machine-b", &Identity::default())
        .unwrap();
    mesh.jj
        .commit_file(&workspace, "file.txt", "from the server");
    wait_converged(&used, &used_replica).await;
    assert!(descriptions(&mesh, &used).contains("from the server"));

    server.set_server(false).await;
    a.wait("machine-b as regular machine", |s| {
        !is_server(s, "machine-b")
    })
    .await;
    server
        .wait("unused dropped", |s| {
            s.repos.iter().all(|r| r.name != "unused")
        })
        .await;
    wait_until("unused replica deleted", || !unused_replica.exists()).await;
    assert_eq!(server.wait_repo("used").await, used_replica);

    // A removal unregisters the replica in use, which the replica task
    // keeps when it reconciles right after.
    let removed = a
        .request(&Request::RemoveRepo {
            name: "used".to_owned(),
        })
        .await;
    assert!(
        matches!(removed, Response::RepoRemoved { .. }),
        "{removed:?}"
    );
    server.wait("used removed", |s| s.repos.is_empty()).await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(used_replica.exists());
}

/// Whether `status` lists the peer `name` as a server.
fn is_server(status: &Status, name: &str) -> bool {
    status.peers.iter().any(|p| p.name == name && p.server)
}

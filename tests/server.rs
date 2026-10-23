//! Tests covering server mode.

mod harness;

use harness::{TestMesh, descriptions, wait_converged, wait_until};
use jj_mesh::{
    config::ObjectHash,
    daemon::control::{Request, Response, Status},
    repo::{Identity, JjRepo},
};

/// A server announces its role, pulls a bare repo of every mesh repo, the
/// ones added before server mode and after, whatever their object hash,
/// keeps them in sync, refuses to forget them, and deletes them once
/// removed from the mesh.
#[tokio::test]
async fn server_keeps_every_repo() {
    let mesh = TestMesh::new();
    let (a, server) = mesh.connected_pair().await;

    let early = mesh.jj.init_repo("early");
    a.add_repo("early", &early).await;
    server.wait_available("early").await;
    server.set_server(true).await;
    assert!(server.status().await.server);
    a.wait("machine-b as server", |s| is_server(s, "machine-b"))
        .await;
    // A SHA-256 repo gets a SHA-256 bare repo.
    let late = mesh.jj.init_repo_hashed("late", ObjectHash::Sha256);
    a.add_repo("late", &late).await;

    let early_bare = server.wait_repo("early").await;
    let late_bare = server.wait_repo("late").await;
    wait_converged(&early, &early_bare).await;
    wait_converged(&late, &late_bare).await;

    mesh.jj.commit_file(&late, "file.txt", "after the pull");
    wait_converged(&late, &late_bare).await;

    let forgotten = server
        .try_request(&Request::ForgetRepo {
            name: "late".to_owned(),
        })
        .await;
    assert!(matches!(forgotten, Response::Error(_)), "{forgotten:?}");

    a.remove_repo("late").await;
    server
        .wait("late removed", |s| s.repos.iter().all(|r| r.name != "late"))
        .await;
    wait_until("late bare repo deleted", || !late_bare.exists()).await;
    assert!(early_bare.exists());
}

/// A workspace added to a bare repo syncs like any other, and keeps its
/// bare repo when server mode is disabled and the repo removed from the
/// mesh, unlike the bare repos no workspace uses.
#[tokio::test]
async fn disabling_server_mode_keeps_bare_repos_in_use() {
    let mesh = TestMesh::new();
    let (a, server) = mesh.connected_pair().await;
    server.set_server(true).await;
    let used = mesh.jj.init_repo("used");
    a.add_repo("used", &used).await;
    let unused = mesh.jj.init_repo("unused");
    a.add_repo("unused", &unused).await;
    let used_bare = server.wait_repo("used").await;
    let unused_bare = server.wait_repo("unused").await;
    wait_converged(&used, &used_bare).await;

    // A workspace of the bare repo, as `jj-mesh clone` adds on a server.
    let workspace = mesh.jj.path().join("used-on-server");
    JjRepo::at(&used_bare)
        .unwrap()
        .add_workspace(&workspace, "machine-b", &Identity::default())
        .unwrap();
    mesh.jj
        .commit_file(&workspace, "file.txt", "from the server");
    wait_converged(&used, &used_bare).await;
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
    wait_until("unused bare repo deleted", || !unused_bare.exists()).await;
    assert_eq!(server.wait_repo("used").await, used_bare);

    // Enabled again, the server pulls the deleted bare repo back.
    server.set_server(true).await;
    assert_eq!(server.wait_repo("unused").await, unused_bare);

    // Once removed from the mesh, the bare repo in use survives the sweep
    // that deletes the unused one, removed afterwards: a reconcile pulls
    // only after its sweep is done.
    a.remove_repo("used").await;
    server
        .wait("used removed", |s| s.repos.iter().all(|r| r.name != "used"))
        .await;
    a.remove_repo("unused").await;
    wait_until("unused bare repo deleted", || !unused_bare.exists()).await;
    let late = mesh.jj.init_repo("late");
    a.add_repo("late", &late).await;
    server.wait_repo("late").await;
    assert!(used_bare.exists());
}

/// Whether `status` lists the peer `name` as a server.
fn is_server(status: &Status, name: &str) -> bool {
    status.peers.iter().any(|p| p.name == name && p.server)
}

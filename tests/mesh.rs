//! Tests covering mesh membership: pairing machines, gossiping peers and repos.

mod harness;

use harness::{TestMesh, add_and_clone, connect, descriptions, wait_converged};
use jj_mesh::daemon::control::{Request, Response};

/// Machines that never paired directly learn about each other through
/// gossip: they connect on their own, repos added anywhere become
/// clonable everywhere, and removing a repo retires it mesh-wide.
#[tokio::test]
async fn membership_gossips_transitively() {
    // Only A-B and B-C are paired.
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;
    let c = mesh.machine("machine-c").await;
    connect(&b, &c).await;

    // A and C discover each other through B and connect directly.
    c.wait_peer_connected("machine-a").await;
    a.wait_peer_connected("machine-c").await;

    // C clones a repo added on A and receives its commits.
    let (dir_a, dir_c) = add_and_clone(&mesh, &a, &c, "proj").await;
    mesh.jj.commit_file(&dir_a, "file.txt", "from a");
    wait_converged(&dir_a, &dir_c).await;
    assert!(descriptions(&mesh, &dir_c).contains("from a"));

    // Removing the repo on A gossips a tombstone that unregisters it
    // everywhere, including on C where it is registered.
    let removed = a
        .request(&Request::RemoveRepo {
            name: "proj".to_owned(),
        })
        .await;
    assert!(
        matches!(removed, Response::RepoRemoved { was_local: true }),
        "{removed:?}",
    );
    assert!(a.status().await.repos.is_empty());
    c.wait("proj removed", |s| {
        s.repos.is_empty() && s.available.is_empty()
    })
    .await;
}

/// Forgetting a repo locally unregisters it on that machine only: the
/// mesh keeps the repo, it can be cloned again right away (the daemon
/// keeps the peers' announcements), and sync resumes in both directions
/// afterwards (announcement sequences survive the re-registration).
#[tokio::test]
async fn forgetting_locally_keeps_the_repo_clonable() {
    let mesh = TestMesh::new();
    let (a, b) = mesh.connected_pair().await;
    let (dir_a, dir_b) = add_and_clone(&mesh, &a, &b, "proj").await;

    // Sync some traffic first, so B's announcement sequence has advanced
    // by the time it forgets the repo.
    mesh.jj.commit_file(&dir_b, "file.txt", "before forget");
    wait_converged(&dir_a, &dir_b).await;

    let forgotten = b
        .request(&Request::ForgetRepo {
            name: "proj".to_owned(),
        })
        .await;
    assert!(
        matches!(forgotten, Response::RepoForgotten { .. }),
        "{forgotten:?}",
    );

    // B no longer syncs the repo (and forgetting again is an error)...
    b.wait("proj forgotten locally", |s| {
        s.repos.is_empty() && s.available == ["proj"]
    })
    .await;
    assert_eq!(a.status().await.repos.len(), 1);
    let again = b
        .try_request(&Request::ForgetRepo {
            name: "proj".to_owned(),
        })
        .await;
    assert!(matches!(again, Response::Error(_)), "{again:?}");

    // ...but the repo clones again into a fresh directory and catches up...
    let dir_b2 = mesh.jj.init_clone_repo("proj-again", "machine-b-again");
    b.clone_repo("proj", &dir_b2).await;
    wait_converged(&dir_a, &dir_b2).await;
    assert!(descriptions(&mesh, &dir_b2).contains("before forget"));

    // ...and changes flow in both directions afterwards.
    mesh.jj.commit_file(&dir_b2, "after.txt", "after forget");
    wait_converged(&dir_a, &dir_b2).await;
    assert!(descriptions(&mesh, &dir_a).contains("after forget"));
}

/// Pairing requires the ticket secret, and a failed attempt never
/// persists a peer nor burns the host's ticket.
#[tokio::test]
async fn pairing_requires_the_ticket_secret() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;

    let ticket = a.host_pairing().await;

    // A join with a tampered secret is rejected; nobody is paired.
    let rejected = b.try_join_pairing(tamper_secret(&ticket)).await;
    assert!(matches!(rejected, Response::Error(_)), "{rejected:?}");
    assert!(a.status().await.peers.is_empty());
    assert!(b.status().await.peers.is_empty());

    // The ticket survives the bad attempt: the original one works.
    b.join_pairing(ticket).await;
    a.wait_peer_connected("machine-b").await;
    b.wait_peer_connected("machine-a").await;
}

/// A pairing ticket stops working once its pairing has concluded.
#[tokio::test]
async fn pairing_ticket_is_single_use() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    let c = mesh.machine("machine-c").await;

    let ticket = a.host_pairing().await;

    // B pairs with the ticket, which redeems it.
    b.join_pairing(ticket.clone()).await;
    a.wait_peer_connected("machine-b").await;

    // Reusing the ticket is refused; the third machine is not paired.
    let reused = c.try_join_pairing(ticket).await;
    assert!(matches!(reused, Response::Error(_)), "{reused:?}");
    assert!(c.status().await.peers.is_empty());
    assert_eq!(a.status().await.peers.len(), 1);
}

/// A failed attempt by the ticket holder (here: announcing an unacceptable
/// name) does not burn the ticket: fixing the problem and retrying with
/// the same ticket works.
#[tokio::test]
async fn failed_attempt_keeps_the_ticket_valid() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let mut b = mesh.machine("machine-b").await;

    // B carries a name the host refuses (a control character); only a
    // hand-edited state can hold one.
    b.stop().await;
    b.edit_state(|state| state.machine.name = "machine\u{7}b".to_owned());
    b.start().await;

    let ticket = a.host_pairing().await;

    let rejected = b.try_join_pairing(ticket.clone()).await;
    assert!(matches!(rejected, Response::Error(_)), "{rejected:?}");
    assert!(a.status().await.peers.is_empty());

    b.rename("machine-b").await;
    b.join_pairing(ticket).await;
    a.wait_peer_connected("machine-b").await;
}

/// A renamed machine keeps its connections, and its peers learn the new
/// name through the gossip; the name survives a restart.
#[tokio::test]
async fn rename_reaches_the_peers() {
    let mesh = TestMesh::new();
    let (mut a, b) = mesh.connected_pair().await;

    a.rename("desk").await;
    assert_eq!(a.status().await.name, "desk");
    b.wait_peer_connected("desk").await;

    a.stop().await;
    a.start().await;
    assert_eq!(a.status().await.name, "desk");
}

/// Hosting again replaces the outstanding ticket: only the newest one is
/// valid.
#[tokio::test]
async fn rehosting_replaces_the_ticket() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;

    let stale = a.host_pairing().await;
    let fresh = a.host_pairing().await;

    // The replaced ticket is dead, and rejecting it does not burn the
    // fresh one...
    let rejected = b.try_join_pairing(stale).await;
    assert!(matches!(rejected, Response::Error(_)), "{rejected:?}");
    assert!(a.status().await.peers.is_empty());

    // ...while the fresh one pairs.
    b.join_pairing(fresh).await;
    a.wait_peer_connected("machine-b").await;
}

/// Flips a bit in the ticket's trailing bytes, which hold the secret (the
/// last field of the encoded ticket), keeping the host address intact.
fn tamper_secret(ticket: &str) -> String {
    let encoded = ticket.strip_prefix("jjmesh-pair-").unwrap();
    let mut bytes = data_encoding::BASE32_NOPAD
        .decode(encoded.to_ascii_uppercase().as_bytes())
        .unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    let encoded = data_encoding::BASE32_NOPAD
        .encode(&bytes)
        .to_ascii_lowercase();
    format!("jjmesh-pair-{encoded}")
}

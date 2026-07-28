//! Tests covering mesh membership: pairing machines, gossiping peers and repos.

mod harness;

use std::fs;

use harness::{TestMesh, add_and_join, connect, descriptions, wait_converged};
use jj_mesh::daemon::control::{Request, Response};

/// Machines that never paired directly learn about each other through
/// gossip: they connect on their own, repos added anywhere become
/// joinable everywhere, and forgetting a repo retires it mesh-wide.
#[tokio::test(flavor = "multi_thread")]
async fn membership_gossips_transitively() {
    // Only A-B and B-C are paired.
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    let c = mesh.machine("machine-c").await;
    connect(&a, &b).await;
    connect(&b, &c).await;

    // A and C discover each other through B and connect directly.
    c.wait_peer_connected("machine-a").await;
    a.wait_peer_connected("machine-c").await;

    // C joins a repo added on A and receives its commits.
    let dir_a = mesh.jj.init_repo("proj");
    let dir_c = add_and_join(&mesh, &a, &c, &dir_a, "proj").await;
    fs::write(dir_a.join("file.txt"), "hello\n").unwrap();
    mesh.jj.jj(&dir_a, &["commit", "-m", "from a"]);
    wait_converged(&dir_a, &dir_c).await;
    assert!(descriptions(&mesh, &dir_c).contains("from a"));

    // Forgetting the repo on A gossips a tombstone that unregisters it
    // everywhere, including on C where it is registered.
    let forgotten = a
        .request(&Request::ForgetRepo {
            name: "proj".to_owned(),
        })
        .await;
    assert!(
        matches!(forgotten, Response::RepoForgotten { was_local: true }),
        "{forgotten:?}",
    );
    assert!(a.status().await.repos.is_empty());
    c.wait("proj forgotten", |s| {
        s.repos.is_empty() && s.available.is_empty()
    })
    .await;
}

/// Pairing requires the ticket secret, and a failed attempt never
/// persists a peer nor burns the host's ticket.
#[tokio::test(flavor = "multi_thread")]
async fn pairing_requires_the_ticket_secret() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;

    let ticket = a.host_pairing().await;

    // A join with a tampered secret is rejected; nobody is paired.
    let rejected = b
        .try_request(&Request::PairJoin {
            ticket: tamper_secret(&ticket),
            name: "machine-b".to_owned(),
        })
        .await;
    assert!(matches!(rejected, Response::Error(_)), "{rejected:?}");
    assert!(a.status().await.peers.is_empty());
    assert!(b.status().await.peers.is_empty());

    // The ticket survives the bad attempt: the original one works.
    let joined = b
        .request(&Request::PairJoin {
            ticket,
            name: "machine-b".to_owned(),
        })
        .await;
    assert!(matches!(joined, Response::Paired { .. }), "{joined:?}");
    a.wait_peer_connected("machine-b").await;
    b.wait_peer_connected("machine-a").await;
}

/// A pairing ticket stops working once its pairing has concluded.
#[tokio::test(flavor = "multi_thread")]
async fn pairing_ticket_is_single_use() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;
    let c = mesh.machine("machine-c").await;

    let ticket = a.host_pairing().await;

    // B pairs with the ticket, which redeems it.
    let joined = b
        .request(&Request::PairJoin {
            ticket: ticket.clone(),
            name: "machine-b".to_owned(),
        })
        .await;
    assert!(matches!(joined, Response::Paired { .. }), "{joined:?}");
    a.wait_peer_connected("machine-b").await;

    // Reusing the ticket is refused; the third machine is not paired.
    let reused = c
        .try_request(&Request::PairJoin {
            ticket,
            name: "machine-c".to_owned(),
        })
        .await;
    assert!(matches!(reused, Response::Error(_)), "{reused:?}");
    assert!(c.status().await.peers.is_empty());
    assert_eq!(a.status().await.peers.len(), 1);
}

/// A failed attempt by the ticket holder (here: announcing an unacceptable
/// name) does not burn the ticket: fixing the problem and retrying with
/// the same ticket works.
#[tokio::test(flavor = "multi_thread")]
async fn failed_attempt_keeps_the_ticket_valid() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;

    let ticket = a.host_pairing().await;

    let rejected = b
        .try_request(&Request::PairJoin {
            ticket: ticket.clone(),
            name: String::new(),
        })
        .await;
    assert!(matches!(rejected, Response::Error(_)), "{rejected:?}");
    assert!(a.status().await.peers.is_empty());

    let joined = b
        .request(&Request::PairJoin {
            ticket,
            name: "machine-b".to_owned(),
        })
        .await;
    assert!(matches!(joined, Response::Paired { .. }), "{joined:?}");
    a.wait_peer_connected("machine-b").await;
}

/// Hosting again replaces the outstanding ticket: only the newest one is
/// valid.
#[tokio::test(flavor = "multi_thread")]
async fn rehosting_replaces_the_ticket() {
    let mesh = TestMesh::new();
    let a = mesh.machine("machine-a").await;
    let b = mesh.machine("machine-b").await;

    let stale = a.host_pairing().await;
    let fresh = a.host_pairing().await;

    // The replaced ticket is dead, and rejecting it does not burn the
    // fresh one...
    let rejected = b
        .try_request(&Request::PairJoin {
            ticket: stale,
            name: "machine-b".to_owned(),
        })
        .await;
    assert!(matches!(rejected, Response::Error(_)), "{rejected:?}");
    assert!(a.status().await.peers.is_empty());

    // ...while the fresh one pairs.
    let joined = b
        .request(&Request::PairJoin {
            ticket: fresh,
            name: "machine-b".to_owned(),
        })
        .await;
    assert!(matches!(joined, Response::Paired { .. }), "{joined:?}");
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

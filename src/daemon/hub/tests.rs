use super::*;
use crate::{
    config::MAX_MESH_REPOS,
    net::sync::{self, UniMessage},
};

fn announce(id: &RepoId, seq: u64, heads: Vec<Vec<u8>>) -> Announce {
    Announce {
        id: id.clone(),
        seq,
        heads,
    }
}

/// A connection between two local endpoints, which must outlive it.
async fn connection() -> (Connection, [iroh::Endpoint; 2]) {
    let [dialer, server] = crate::testing::local_endpoints(sync::ALPN).await;
    let (conn, _) = tokio::join!(dialer.connect(server.id(), sync::ALPN), async {
        server.accept().await.unwrap().await.unwrap()
    });
    (conn.unwrap(), [dialer, server])
}

#[tokio::test]
async fn routes_to_registered_repo() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());

    let peer = iroh::SecretKey::generate().public();
    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));

    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].peer, peer);
    assert_eq!(drained[0].heads, vec![vec![1; 64]]);
    assert!(inbox.drain().is_empty());

    // Malformed announcements are dropped.
    hub.route(peer, announce(&id, 2, vec![vec![1; MAX_HEAD_LEN + 1]]));
    let heads = vec![vec![1; 64]; MAX_ANNOUNCED_HEADS + 1];
    hub.route(peer, announce(&id, 3, heads));
    assert!(inbox.drain().is_empty());
}

#[tokio::test]
async fn discards_reordered_announcements() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce(&id, 2, vec![vec![2; 64]]));
    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));

    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].heads, vec![vec![2; 64]]);

    // The watermark survives draining: the stale announcement stays
    // rejected even when it arrives afterwards.
    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
}

#[tokio::test]
async fn requeue_retries_failed_heads_until_superseded() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);

    // A failed fetch requeues the drained heads; the next drain retries.
    inbox.requeue(peer, drained[0].seq, drained[0].heads.clone());
    let retried = inbox.drain();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].heads, vec![vec![1; 64]]);

    // A newer announcement supersedes a stale requeue.
    hub.route(peer, announce(&id, 2, vec![vec![2; 64]]));
    inbox.requeue(peer, 1, vec![vec![1; 64]]);
    let latest = inbox.drain();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].heads, vec![vec![2; 64]]);
}

#[tokio::test]
async fn ignores_unregistered_repo() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());
    hub.unregister_repo(&id);

    let peer = iroh::SecretKey::generate().public();
    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
}

#[tokio::test]
async fn remembers_unregistered_announcements_for_clone() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let peer = iroh::SecretKey::generate().public();

    // Not offered as a clone source while the peer is not connected.
    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    assert!(hub.clone_sources(&id, "a").is_err());

    // Registering the repo claims the orphan entry.
    let inbox = hub.register_repo(id.clone());
    assert!(hub.clone_sources(&id, "a").is_err());
    hub.route(peer, announce(&id, 2, vec![vec![2; 64]]));
    assert_eq!(inbox.drain().len(), 1);
}

/// Registering a repo seeds its inbox from the orphan announcements, so
/// an idle peer's heads are not lost.
#[tokio::test]
async fn register_seeds_the_inbox_from_orphans() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    let inbox = hub.register_repo(id);
    assert_eq!(inbox.drain().len(), 1);
}

#[tokio::test]
async fn orphans_are_bounded_per_peer() {
    let hub = SyncHub::new();
    let flooder = iroh::SecretKey::generate().public();
    let honest = iroh::SecretKey::generate().public();

    for _ in 0..MAX_MESH_REPOS {
        hub.route(flooder, announce(&RepoId::generate(), 1, vec![vec![1; 64]]));
    }
    let over = RepoId::generate();
    hub.route(flooder, announce(&over, 1, vec![vec![1; 64]]));
    let work = RepoId::generate();
    hub.route(honest, announce(&work, 1, vec![vec![2; 64]]));

    // The honest orphan survived the flood, the one over the cap did not.
    assert_eq!(hub.register_repo(work).drain().len(), 1);
    assert!(hub.register_repo(over).drain().is_empty());

    // Disconnecting the flooder frees its slots for new repos.
    hub.peer_disconnected(&flooder);
    let fresh = RepoId::generate();
    hub.route(flooder, announce(&fresh, 1, vec![vec![3; 64]]));
    assert_eq!(hub.register_repo(fresh).drain().len(), 1);
}

/// A repo unregistered locally keeps its peers' last announcements as
/// orphans, so it stays clonable without waiting for the (idle) peers to
/// announce again.
#[tokio::test]
async fn unregister_demotes_announcements_to_orphans() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    // Draining (a completed fetch) must not lose the heads.
    assert_eq!(inbox.drain().len(), 1);
    hub.unregister_repo(&id);

    assert_eq!(hub.register_repo(id).drain().len(), 1);
}

/// A retraction releases everything attributed to the peer for the repo,
/// while reordered pre-retraction announcements stay rejected.
#[tokio::test]
async fn retraction_releases_peer_state() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce(&id, 2, vec![]));
    // A stale pre-retraction announcement stays rejected...
    hub.route(peer, announce(&id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
    // ...while a later one (a re-clone on the peer) comes through.
    hub.route(peer, announce(&id, 3, vec![vec![2; 64]]));
    assert_eq!(inbox.drain().len(), 1);

    // A retraction clears heads offered but not drained yet: they do not
    // come back as orphans.
    hub.route(peer, announce(&id, 4, vec![vec![3; 64]]));
    hub.route(peer, announce(&id, 5, vec![]));
    hub.unregister_repo(&id);
    assert!(hub.register_repo(id).drain().is_empty());

    // A retraction of an unregistered repo clears its orphan entry.
    let other = RepoId::generate();
    hub.route(peer, announce(&other, 6, vec![vec![1; 64]]));
    hub.route(peer, announce(&other, 7, vec![]));
    assert!(hub.register_repo(other).drain().is_empty());
}

#[tokio::test]
async fn reconnection_resets_sequence_tracking() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo(id.clone());
    let (conn, _endpoints) = connection().await;
    let peer = conn.remote_id();

    hub.peer_connected(peer, &conn);
    hub.route(peer, announce(&id, 5, vec![vec![1; 64]]));
    assert_eq!(inbox.drain().len(), 1);

    hub.peer_connected(peer, &conn);
    hub.route(peer, announce(&id, 1, vec![vec![2; 64]]));
    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].heads, vec![vec![2; 64]]);
}

#[test]
fn outbox_coalesces_per_repo_with_membership_first() {
    let outbox = Outbox::default();
    let id = RepoId::generate();
    let other = RepoId::generate();

    outbox.push_announce(announce(&id, 1, vec![vec![1; 64]]));
    outbox.push_membership(Membership::default());
    outbox.push_announce(announce(&other, 1, vec![vec![3; 64]]));
    outbox.push_announce(announce(&id, 2, vec![vec![2; 64]]));
    outbox.push_membership(Membership::default());

    let sent: Vec<UniMessage> = std::iter::from_fn(|| outbox.pop()).collect();
    assert_eq!(sent.len(), 3);
    assert!(matches!(sent[0], UniMessage::Membership(_)));
    let seqs: Vec<(&RepoId, u64)> = sent
        .iter()
        .filter_map(|message| match message {
            UniMessage::Announce(a) => Some((&a.id, a.seq)),
            UniMessage::Membership(_) | UniMessage::Status(_) => None,
        })
        .collect();
    assert!(seqs.contains(&(&id, 2)) && seqs.contains(&(&other, 1)));
}

#[test]
fn names_managed_peers() {
    let hub = SyncHub::new();
    let peer = iroh::SecretKey::generate().public();
    assert_eq!(hub.peer_name(&peer), peer.fmt_short().to_string());

    hub.set_peer_names(BTreeMap::from([(peer, "laptop".to_owned())]));
    assert_eq!(hub.peer_name(&peer), "laptop");
}

use super::orphans::MAX_ORPHAN_REPOS_PER_PEER;
use super::*;
use crate::net::sync::UniMessage;

fn announce(name: &str, id: &RepoId, seq: u64, heads: Vec<Vec<u8>>) -> Announce {
    Announce {
        name: name.to_owned(),
        id: id.clone(),
        seq,
        heads,
        colocated: false,
    }
}

#[tokio::test]
async fn routes_to_registered_repo() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());

    let peer = iroh::SecretKey::generate().public();
    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));

    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].peer, peer);
    assert_eq!(drained[0].heads, vec![vec![1; 64]]);
    assert!(inbox.drain().is_empty());
}

#[tokio::test]
async fn discards_reordered_announcements() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));

    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);
    assert_eq!(drained[0].heads, vec![vec![2; 64]]);

    // The watermark survives draining: the stale announcement stays
    // rejected even when it arrives afterwards.
    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
}

#[tokio::test]
async fn requeue_retries_failed_heads_until_superseded() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
    let drained = inbox.drain();
    assert_eq!(drained.len(), 1);

    // A failed fetch requeues the drained heads; the next drain retries.
    inbox.requeue(peer, drained[0].seq, drained[0].heads.clone());
    let retried = inbox.drain();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].heads, vec![vec![1; 64]]);

    // A newer announcement supersedes a stale requeue.
    hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
    inbox.requeue(peer, 1, vec![vec![1; 64]]);
    let latest = inbox.drain();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].heads, vec![vec![2; 64]]);
}

#[tokio::test]
async fn ignores_unregistered_repo() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    hub.unregister_repo("a");

    let peer = iroh::SecretKey::generate().public();
    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
}

#[tokio::test]
async fn remembers_unregistered_announcements_for_clone() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let peer = iroh::SecretKey::generate().public();

    // Not offered as a clone source while the peer is not connected.
    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
    assert!(hub.clone_sources("a").is_err());

    // Registering the repo claims the orphan entry.
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    assert!(hub.clone_sources("a").is_err());
    hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
    assert_eq!(inbox.drain().len(), 1);
}

#[tokio::test]
async fn conflicting_id_is_surfaced_and_never_synced() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    let peer = iroh::SecretKey::generate().public();

    let foreign = RepoId::generate();
    hub.route(peer, announce("a", &foreign, 1, vec![vec![1; 64]]));

    assert!(inbox.drain().is_empty(), "conflicts must not be synced");
    assert_eq!(hub.conflicts(), vec![("a".to_owned(), peer)]);

    // A matching announcement resumes sync and resolves the conflict.
    hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
    assert_eq!(inbox.drain().len(), 1);
    assert!(hub.conflicts().is_empty());
}

#[tokio::test]
async fn tracks_conflicts_per_peer() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    hub.register_repo("a".to_owned(), id.clone());
    let (b, c) = (
        iroh::SecretKey::generate().public(),
        iroh::SecretKey::generate().public(),
    );

    hub.route(b, announce("a", &RepoId::generate(), 1, vec![vec![1; 64]]));
    hub.route(c, announce("a", &RepoId::generate(), 1, vec![vec![2; 64]]));
    assert_eq!(hub.conflicts().len(), 2);

    // One peer leaving must not hide the other's live conflict.
    hub.peer_disconnected(&c);
    assert_eq!(hub.conflicts(), vec![("a".to_owned(), b)]);
}

#[tokio::test]
async fn register_seeds_conflicts_from_orphans() {
    let hub = SyncHub::new();
    let peer = iroh::SecretKey::generate().public();

    // The peer announced first; adding a different local repo under
    // the same name must surface the conflict without waiting for the
    // (idle) peer to announce again.
    hub.route(
        peer,
        announce("a", &RepoId::generate(), 1, vec![vec![1; 64]]),
    );
    hub.register_repo("a".to_owned(), RepoId::generate());
    assert_eq!(hub.conflicts(), vec![("a".to_owned(), peer)]);
}

#[tokio::test]
async fn orphans_are_bounded_per_peer() {
    let hub = SyncHub::new();
    let flooder = iroh::SecretKey::generate().public();
    let honest = iroh::SecretKey::generate().public();

    for n in 0..MAX_ORPHAN_REPOS_PER_PEER {
        hub.route(
            flooder,
            announce(
                &format!("junk{n}"),
                &RepoId::generate(),
                1,
                vec![vec![1; 64]],
            ),
        );
    }
    hub.route(
        honest,
        announce("work", &RepoId::generate(), 1, vec![vec![2; 64]]),
    );

    // The honest orphan survived the flood: registering `work` with a
    // different id seeds its conflict, proving the entry was kept.
    hub.register_repo("work".to_owned(), RepoId::generate());
    assert_eq!(hub.conflicts(), vec![("work".to_owned(), honest)]);

    // Disconnecting the flooder frees its slots for new names.
    hub.peer_disconnected(&flooder);
    hub.route(
        flooder,
        announce("fresh", &RepoId::generate(), 1, vec![vec![3; 64]]),
    );
    hub.register_repo("fresh".to_owned(), RepoId::generate());
    assert!(hub.conflicts().iter().any(|(name, _)| name == "fresh"));
}

/// A peer announcing a colocated instance while ours is colocated too
/// pauses the repo, and the pause lifts when either side stops being
/// colocated.
#[tokio::test]
async fn colocation_conflict_pauses_and_resumes() {
    use crate::{repo::JjRepo, testing::Fixture};

    // `jj git init` colocates by default, so the opened repo reports
    // `is_colocated`.
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let repo = Arc::new(JjRepo::discover(&dir).unwrap().open().unwrap());
    assert!(repo.is_colocated());

    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    hub.repo_opened("a", &id, repo);
    let peer = iroh::SecretKey::generate().public();

    let colocated = |seq| Announce {
        colocated: true,
        ..announce("a", &id, seq, vec![vec![1; 64]])
    };
    hub.route(peer, colocated(1));
    assert!(hub.is_paused("a"));
    assert_eq!(hub.paused_repos().get("a"), Some(&vec![peer]));
    // The heads still land in the inbox: the repo task requeues them
    // while paused, so they are fetched once the pause lifts instead
    // of being lost.
    assert_eq!(inbox.drain().len(), 1);

    // The peer de-colocating resumes sync with its own announcement.
    hub.route(peer, announce("a", &id, 2, vec![vec![2; 64]]));
    assert!(!hub.is_paused("a"));
    assert_eq!(inbox.drain().len(), 1);

    // So does the peer disconnecting.
    hub.route(peer, colocated(3));
    assert!(hub.is_paused("a"));
    hub.peer_disconnected(&peer);
    assert!(!hub.is_paused("a"));

    // And so does a retraction (the peer forgot its instance).
    hub.route(peer, colocated(1));
    assert!(hub.is_paused("a"));
    hub.route(peer, announce("a", &id, 2, vec![]));
    assert!(!hub.is_paused("a"));
}

/// A repo unregistered locally keeps its peers' last announcements as
/// orphans, so the name stays clonable without waiting for the (idle)
/// peers to announce again.
#[tokio::test]
async fn unregister_demotes_announcements_to_orphans() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
    // Draining (a completed fetch) must not lose the heads.
    assert_eq!(inbox.drain().len(), 1);
    hub.unregister_repo("a");

    // Re-registering under a different id seeds a conflict from the
    // demoted entry, proving the announcement survived.
    hub.register_repo("a".to_owned(), RepoId::generate());
    assert_eq!(hub.conflicts(), vec![("a".to_owned(), peer)]);
}

/// A retraction releases everything attributed to the peer for the
/// name, while reordered pre-retraction announcements stay rejected.
#[tokio::test]
async fn retraction_releases_peer_state() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a".to_owned(), id.clone());
    let peer = iroh::SecretKey::generate().public();

    // A retraction clears even a conflicting peer's entry.
    hub.route(
        peer,
        announce("a", &RepoId::generate(), 1, vec![vec![1; 64]]),
    );
    assert_eq!(hub.conflicts().len(), 1);
    hub.route(peer, announce("a", &RepoId::generate(), 2, vec![]));
    assert!(hub.conflicts().is_empty());

    // A stale pre-retraction announcement stays rejected...
    hub.route(peer, announce("a", &id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
    // ...while a later one (a re-clone on the peer) comes through.
    hub.route(peer, announce("a", &id, 3, vec![vec![2; 64]]));
    assert_eq!(inbox.drain().len(), 1);

    // A retraction of an unregistered name clears the orphan entry:
    // registering the name no longer seeds a conflict from it.
    hub.route(
        peer,
        announce("b", &RepoId::generate(), 4, vec![vec![1; 64]]),
    );
    hub.route(peer, announce("b", &RepoId::generate(), 5, vec![]));
    hub.register_repo("b".to_owned(), RepoId::generate());
    assert!(hub.conflicts().is_empty());
}

#[tokio::test]
async fn rejects_invalid_announced_names() {
    let hub = SyncHub::new();
    let id = RepoId::generate();
    let inbox = hub.register_repo("a\u{202E}b".to_owned(), id.clone());
    let peer = iroh::SecretKey::generate().public();

    hub.route(peer, announce("a\u{202E}b", &id, 1, vec![vec![1; 64]]));
    assert!(inbox.drain().is_empty());
    hub.route(peer, announce("", &id, 1, vec![vec![1; 64]]));
    assert!(hub.clone_sources("").is_err());
}

#[test]
fn outbox_coalesces_per_repo_with_membership_first() {
    let outbox = Outbox::default();
    let id = RepoId::generate();
    let other = RepoId::generate();

    outbox.push_announce(announce("a", &id, 1, vec![vec![1; 64]]));
    outbox.push_membership(Membership::default());
    outbox.push_announce(announce("b", &other, 1, vec![vec![3; 64]]));
    outbox.push_announce(announce("a", &id, 2, vec![vec![2; 64]]));
    outbox.push_membership(Membership::default());

    let sent: Vec<UniMessage> = std::iter::from_fn(|| outbox.pop()).collect();
    assert_eq!(sent.len(), 3);
    assert!(matches!(sent[0], UniMessage::Membership(_)));
    let seqs: Vec<(&str, u64)> = sent
        .iter()
        .filter_map(|message| match message {
            UniMessage::Announce(a) => Some((a.name.as_str(), a.seq)),
            UniMessage::Membership(_) | UniMessage::Status(_) => None,
        })
        .collect();
    assert!(seqs.contains(&("a", 2)) && seqs.contains(&("b", 1)));
}

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use super::RepoSet;
use crate::{
    config::{MeshState, Repo, RepoId},
    daemon::{
        control::{self, WatchStatus},
        hub::SyncHub,
    },
    repo::JjRepo,
    testing::Fixture,
};

/// A repo set with the given `config.toml` contents.
fn repo_set(config: &str) -> RepoSet {
    let settings = Arc::new(toml::from_str(config).unwrap());
    RepoSet::new(Arc::new(SyncHub::new()), settings)
}

/// A repo set with auto-snapshot and update-stale disabled: hermetic
/// (the daemon spawns no jj, which would read the user's real config).
fn quiet_repo_set() -> RepoSet {
    repo_set("snapshot-interval = 0\nupdate-stale = false")
}

/// Polls until `pred` holds on the statuses, panicking after 10s.
async fn wait_for(set: &RepoSet, pred: impl Fn(&[control::RepoStatus]) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if pred(&set.statuses()) {
            return;
        }
        assert!(Instant::now() < deadline, "condition not reached in time");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls until the set holds a single repo whose watch matches `pred`.
async fn wait_watch(set: &RepoSet, pred: impl Fn(&WatchStatus) -> bool) {
    wait_for(set, |s| matches!(s, [status] if pred(&status.watch))).await;
}

/// Polls until the single repo's watch is up.
async fn wait_watching(set: &RepoSet) {
    wait_watch(set, |w| matches!(w, WatchStatus::Watching { .. })).await;
}

/// Polls until the single repo's watch has recorded a change.
async fn wait_changed(set: &RepoSet) {
    wait_watch(set, |w| {
        matches!(
            w,
            WatchStatus::Watching {
                last_change_secs: Some(_),
                ..
            }
        )
    })
    .await;
}

fn state_with(name: &str, path: &std::path::Path) -> MeshState {
    let mut state = MeshState::default();
    state.repos.insert(
        name.to_owned(),
        Repo {
            id: RepoId::generate(),
            path: path.to_owned(),
        },
    );
    state
}

#[tokio::test]
async fn watches_and_detects_head_changes() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");

    let set = quiet_repo_set();
    set.sync(&state_with("a", &dir));

    wait_watch(&set, |w| {
        matches!(
            w,
            WatchStatus::Watching {
                op_heads: 1,
                last_change_secs: None,
                ..
            }
        )
    })
    .await;

    fx.jj(&dir, &["new", "-m", "change"]);

    wait_changed(&set).await;

    set.sync(&MeshState::default());
    assert!(set.statuses().is_empty());
}

/// Removing and recreating a watched repo must not leave a dead watch:
/// the task reopens and keeps detecting changes at the same path.
#[tokio::test]
async fn recovers_after_repo_recreation() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");

    let set = quiet_repo_set();
    set.sync(&state_with("a", &dir));
    wait_watching(&set).await;

    std::fs::remove_dir_all(&dir).unwrap();
    fx.init_repo("a");

    // The dead watch must be noticed (Failed, or Missing when the
    // failure lands in the removed-not-yet-recreated window), then
    // rebuilt (Watching); both states persist long enough for the 50ms
    // polling to see them.
    wait_watch(&set, |w| {
        matches!(w, WatchStatus::Failed { .. } | WatchStatus::Missing { .. })
    })
    .await;
    wait_watching(&set).await;

    fx.jj(&dir, &["new", "-m", "after-recreation"]);
    wait_changed(&set).await;
}

/// A path with no repo directory at all is `Missing` (the state that
/// suggests `jj-mesh repo forget`), not a generic failure.
#[tokio::test]
async fn reports_missing_for_absent_repo_dir() {
    let fx = Fixture::new();
    let set = quiet_repo_set();
    set.sync(&state_with("ghost", &fx.path().join("missing")));

    wait_watch(&set, |w| matches!(w, WatchStatus::Missing { .. })).await;
}

/// A directory that exists but is not a usable repo is a `Failed`
/// watch, with the open error preserved.
#[tokio::test]
async fn reports_failure_for_invalid_repo() {
    let fx = Fixture::new();
    let dir = fx.path().join("broken");
    std::fs::create_dir_all(dir.join(".jj")).unwrap();

    let set = quiet_repo_set();
    set.sync(&state_with("broken", &dir));

    wait_watch(&set, |w| matches!(w, WatchStatus::Failed { .. })).await;
}

/// An edit to a working-copy file must produce a snapshot operation
/// one interval later, visible as an op-heads change.
#[tokio::test]
async fn auto_snapshots_working_copy_edits() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");

    let set = repo_set("snapshot-interval = 1\nupdate-stale = false");
    set.sync(&state_with("a", &dir));
    wait_watch(&set, |w| {
        matches!(
            w,
            WatchStatus::Watching {
                last_change_secs: None,
                ..
            }
        )
    })
    .await;

    std::fs::write(dir.join("edited.txt"), "content").unwrap();

    wait_changed(&set).await;
}

/// A stale working copy (op head advanced without updating it, which
/// is what applying synced operations does) is healed on watch start.
#[tokio::test]
async fn updates_stale_working_copy() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    // Change the working-copy commit's tree without updating the
    // working copy: jj only considers it stale when the trees differ.
    std::fs::write(dir.join("f.txt"), "content").unwrap();
    fx.jj(&dir, &["status"]);
    fx.jj(&dir, &["--ignore-working-copy", "abandon", "@"]);
    assert!(
        !fx.jj_ok(&dir, &["status"]),
        "the working copy must start stale for this test to mean anything",
    );

    let set = repo_set("snapshot-interval = 0\nupdate-stale = true");
    set.sync(&state_with("a", &dir));

    let deadline = Instant::now() + Duration::from_secs(10);
    while !fx.jj_ok(&dir, &["status"]) {
        assert!(Instant::now() < deadline, "working copy still stale");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// An op head without a commit index (published by a fetch whose index
/// build failed, or by an older jj-mesh) is reindexed on watch start,
/// before any jj command pays for the rebuild itself.
#[tokio::test]
async fn heals_missing_commit_index_on_watch_start() {
    use jj_lib::object_id::ObjectId as _;

    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let repo = JjRepo::discover(&dir).unwrap().open().unwrap();
    let head = repo.op_heads().await.unwrap().remove(0);
    let op_link = dir.join(".jj/repo/index/op_links").join(head.hex());
    assert!(op_link.is_file(), "jj indexes its own operations");
    std::fs::remove_file(&op_link).unwrap();
    assert!(!repo.has_commit_index(&head).await);

    let set = quiet_repo_set();
    set.sync(&state_with("a", &dir));
    // The heal runs before the watch reports itself up.
    wait_watching(&set).await;

    assert!(
        repo.has_commit_index(&head).await,
        "op link must be rebuilt"
    );
}

/// Changing a watched repo's store configuration must be detected on
/// the next wake and reopen the repo against the new configuration
/// instead of continuing on stale stores.
#[tokio::test]
async fn reopens_when_store_configuration_changes() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let repo = JjRepo::discover(&dir).unwrap();
    let before = repo.fingerprint().unwrap();

    let set = quiet_repo_set();
    set.sync(&state_with("a", &dir));
    wait_watching(&set).await;

    // Point git_target somewhere unusable, then wake the watch with a
    // transient file in the watched op-heads directory (jj itself can
    // no longer run against the broken configuration): the fingerprint
    // change must force a reopen, which fails against the broken
    // configuration. Watching on stale stores would sail right past
    // this.
    let target = dir.join(".jj/repo/store/git_target");
    let original = std::fs::read_to_string(&target).unwrap();
    std::fs::write(&target, "does-not-exist").unwrap();
    assert_ne!(repo.fingerprint().unwrap(), before);
    let wake = dir.join(".jj/repo/op_heads/heads/.wake");
    std::fs::write(&wake, "").unwrap();
    std::fs::remove_file(&wake).unwrap();
    wait_watch(&set, |w| matches!(w, WatchStatus::Failed { .. })).await;

    // Restoring the configuration heals the repo on the next retry.
    std::fs::write(&target, original).unwrap();
    wait_watching(&set).await;
}

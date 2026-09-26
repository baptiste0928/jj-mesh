use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::mpsc;

use super::{ClaimUpdate, RepoSet};
use crate::{
    config::{MeshState, Repo, RepoId, WorkspaceClaims},
    daemon::{
        control::{self, WatchStatus, WorkspacePlace, WorkspaceState},
        hub::SyncHub,
    },
    repo::JjRepo,
    testing::Fixture,
};

/// A repo set with the given `config.toml` contents, and the claims its
/// repo tasks send.
fn claiming_repo_set(config: &str) -> (RepoSet, mpsc::UnboundedReceiver<ClaimUpdate>) {
    let settings = Arc::new(toml::from_str(config).unwrap());
    let (claim, claims) = mpsc::unbounded_channel();
    (
        RepoSet::new(Arc::new(SyncHub::new()), settings, claim),
        claims,
    )
}

/// Settings with auto-snapshot and update-stale disabled: hermetic (the
/// daemon spawns no jj, which would read the user's real config).
const QUIET: &str = "snapshot-interval = 0\nupdate-stale = false";

/// A repo set with the given `config.toml` contents, synced to `state`
/// and recording the claims its repo tasks make like the daemon's store.
fn start(config: &str, state: MeshState) -> Arc<RepoSet> {
    let (set, mut claims) = claiming_repo_set(config);
    let set = Arc::new(set);
    set.sync(&state);
    let weak = Arc::downgrade(&set);
    tokio::spawn(async move {
        let mut state = state;
        while let Some(update) = claims.recv().await {
            let Some(set) = weak.upgrade() else {
                return;
            };
            state.set_claims(&update.repo, update.names).unwrap();
            set.sync(&state);
        }
    });
    set
}

/// Polls until `pred` holds on the statuses, panicking after 10s.
async fn wait_for(set: &RepoSet, pred: impl Fn(&[control::RepoStatus]) -> bool) {
    assert!(
        wait_for_within(set, Duration::from_secs(10), pred).await,
        "condition not reached in time"
    );
}

/// Polls until `pred` holds on the statuses, giving up after `timeout`.
async fn wait_for_within(
    set: &RepoSet,
    timeout: Duration,
    pred: impl Fn(&[control::RepoStatus]) -> bool,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if pred(&set.statuses()) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls until the set holds a single repo whose watch matches `pred`.
async fn wait_watch(set: &RepoSet, pred: impl Fn(&WatchStatus) -> bool) {
    wait_for(set, |s| matches!(s, [status] if pred(&status.watch))).await;
}

/// Polls until `cond` holds, panicking with `what` after 10s.
async fn eventually(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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

fn state_with(name: &str, path: &Path) -> MeshState {
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

    let set = start(QUIET, state_with("a", &dir));

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

    let set = start(QUIET, state_with("a", &dir));
    wait_watching(&set).await;

    std::fs::remove_dir_all(&dir).unwrap();
    fx.init_repo("a");

    // With inotify the dead watch must be noticed (Failed, or Missing when
    // the failure lands in the removed-not-yet-recreated window), then
    // rebuilt (Watching); both states persist long enough for the 50ms
    // polling to see them.
    if cfg!(target_os = "linux") {
        wait_watch(&set, |w| {
            matches!(w, WatchStatus::Failed { .. } | WatchStatus::Missing { .. })
        })
        .await;
    }
    wait_watching(&set).await;

    // FSEvents follows the path, so on macOS the old watch stays up until
    // the next wake, which then rebuilds it with the heads found at that
    // point as its baseline. Nothing observable marks the rebuild, so
    // keep changing the repo until a change is seen.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        fx.jj(&dir, &["new", "-m", "after-recreation"]);
        let changed = wait_for_within(&set, Duration::from_secs(2), |s| {
            matches!(
                s,
                [status] if matches!(
                    status.watch,
                    WatchStatus::Watching { last_change_secs: Some(_), .. }
                )
            )
        })
        .await;
        if changed {
            break;
        }
        assert!(Instant::now() < deadline, "change not observed in time");
    }
}

/// A path with no repo directory at all is `Missing` (the state that
/// suggests `jj-mesh repo forget`), not a generic failure.
#[tokio::test]
async fn reports_missing_for_absent_repo_dir() {
    let fx = Fixture::new();
    let set = start(QUIET, state_with("ghost", &fx.path().join("missing")));

    wait_watch(&set, |w| matches!(w, WatchStatus::Missing { .. })).await;
}

/// A directory that exists but is not a usable repo is a `Failed`
/// watch, with the open error preserved.
#[tokio::test]
async fn reports_failure_for_invalid_repo() {
    let fx = Fixture::new();
    let dir = fx.path().join("broken");
    std::fs::create_dir_all(dir.join(".jj")).unwrap();

    let set = start(QUIET, state_with("broken", &dir));

    wait_watch(&set, |w| matches!(w, WatchStatus::Failed { .. })).await;
}

/// An edit to a working-copy file must produce a snapshot operation
/// one interval later, visible as an op-heads change.
#[tokio::test]
async fn auto_snapshots_working_copy_edits() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");

    let set = start(
        "snapshot-interval = 1\nupdate-stale = false",
        state_with("a", &dir),
    );
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

    let _set = start(
        "snapshot-interval = 0\nupdate-stale = true",
        state_with("a", &dir),
    );

    eventually("working copy still stale", || fx.jj_ok(&dir, &["status"])).await;
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

    let set = start(QUIET, state_with("a", &dir));
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

    let set = start(QUIET, state_with("a", &dir));
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

/// The staleness check gates update-stale: a head that moved the
/// working-copy commit without updating the working copy reads as
/// stale, one that only touched other state does not.
#[tokio::test]
async fn staleness_follows_the_working_copy_commit() {
    use super::workspace::may_be_stale;

    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let jj = JjRepo::discover(&dir).unwrap();
    let workspace = jj.workspace().unwrap();
    let repo = jj.open().unwrap();
    let head = |repo: &crate::repo::OpenRepo| {
        let heads = pollster::block_on(repo.op_heads()).unwrap();
        assert_eq!(heads.len(), 1);
        heads[0].clone()
    };

    assert!(!may_be_stale(&workspace, &repo, &head(&repo)).await.unwrap());

    // A new operation leaving the working-copy commit alone.
    fx.jj(
        &dir,
        &[
            "--ignore-working-copy",
            "bookmark",
            "create",
            "b",
            "-r",
            "@",
        ],
    );
    assert!(!may_be_stale(&workspace, &repo, &head(&repo)).await.unwrap());

    // One rewriting it behind the working copy's back.
    fx.jj(&dir, &["--ignore-working-copy", "describe", "-m", "moved"]);
    assert!(may_be_stale(&workspace, &repo, &head(&repo)).await.unwrap());

    // Updating the working copy settles it.
    fx.jj(&dir, &["status"]);
    assert!(!may_be_stale(&workspace, &repo, &head(&repo)).await.unwrap());
}

/// The workspace names the single repo's status lists.
fn workspace_names(statuses: &[control::RepoStatus]) -> Vec<&str> {
    match statuses {
        [status] => status.workspaces.iter().map(|w| w.name.as_str()).collect(),
        _ => Vec::new(),
    }
}

/// Workspaces added and forgotten while the repo is watched are picked up
/// and dropped, and edits in a secondary one are snapshotted.
#[tokio::test]
async fn tracks_and_snapshots_secondary_workspaces() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");

    let set = start(
        "snapshot-interval = 1\nupdate-stale = false",
        state_with("a", &dir),
    );
    wait_for(&set, |s| workspace_names(s) == ["default"]).await;

    fx.jj(&dir, &["workspace", "add", "../child"]);
    wait_for(&set, |s| {
        let mut names = workspace_names(s);
        names.sort_unstable();
        names == ["child", "default"]
    })
    .await;

    let child = fx.path().join("child");
    std::fs::write(child.join("edited.txt"), "content").unwrap();
    eventually("secondary workspace not snapshotted", || {
        let files = fx.jj_output(
            &dir,
            &["--ignore-working-copy", "file", "list", "-r", "child@"],
        );
        files.contains("edited.txt")
    })
    .await;

    fx.jj(&dir, &["workspace", "forget", "child"]);
    wait_for(&set, |s| workspace_names(s) == ["default"]).await;
}

/// A stale claimed secondary working copy is healed like the main one.
#[tokio::test]
async fn updates_stale_secondary_workspace() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let child = stale_child(&fx, &dir);

    let mut state = state_with("a", &dir);
    claim(&mut state, &["default", "child"]);
    let _set = start("snapshot-interval = 0\nupdate-stale = true", state);

    eventually("working copy still stale", || fx.jj_ok(&child, &["status"])).await;
}

/// Adds a secondary workspace `child` to the repo at `dir`, left stale:
/// its working-copy commit is rewritten to another tree behind its back.
fn stale_child(fx: &Fixture, dir: &Path) -> PathBuf {
    fx.jj(dir, &["workspace", "add", "../child"]);
    let child = fx.path().join("child");
    std::fs::write(child.join("f.txt"), "content").unwrap();
    fx.jj(&child, &["status"]);
    fx.jj(dir, &["--ignore-working-copy", "abandon", "child@"]);
    assert!(
        !fx.jj_ok(&child, &["status"]),
        "the working copy must start stale for this test to mean anything",
    );
    child
}

/// Claims the named workspaces of the single repo in `state`.
fn claim(state: &mut MeshState, names: &[&str]) {
    let id = state.repos.values().next().unwrap().id.clone();
    let names = names.iter().map(|name| (*name).to_owned()).collect();
    state.set_claims(&id, names).unwrap();
}

/// The state of the single repo's local workspace `name`, if listed.
fn local_state(statuses: &[control::RepoStatus], name: &str) -> Option<WorkspaceState> {
    let [status] = statuses else {
        return None;
    };
    status.workspaces.iter().find_map(|w| match &w.place {
        WorkspacePlace::Local { state, .. } if w.name == name => Some(state.clone()),
        _ => None,
    })
}

/// Fresh workspaces are claimed, the main one whatever its freshness.
#[tokio::test]
async fn claims_fresh_workspaces() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    fx.jj(&dir, &["workspace", "add", "../child"]);

    let (set, mut claims) = claiming_repo_set("snapshot-interval = 0\nupdate-stale = false");
    set.sync(&state_with("a", &dir));

    let update = tokio::time::timeout(Duration::from_secs(10), claims.recv())
        .await
        .unwrap()
        .unwrap();
    let expected: BTreeSet<String> = ["child", "default"].map(str::to_owned).into();
    assert_eq!(update.names, expected);
}

/// A workspace found stale is not claimed: it may be a forgotten one that
/// a peer reusing its name brought back into the view.
#[tokio::test]
async fn leaves_stale_unclaimed_workspaces_alone() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let child = stale_child(&fx, &dir);

    let set = start(
        "snapshot-interval = 0\nupdate-stale = true",
        state_with("a", &dir),
    );

    wait_for(&set, |s| {
        local_state(s, "child") == Some(WorkspaceState::Stale)
    })
    .await;
    assert_eq!(
        local_state(&set.statuses(), "default"),
        Some(WorkspaceState::Claimed)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !fx.jj_ok(&child, &["status"]),
        "an unclaimed workspace must stay untouched"
    );
}

/// A name claimed here and by a peer is contested and left alone; the
/// peer's claims show up in the status.
#[tokio::test]
async fn leaves_contested_workspaces_alone() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    let child = stale_child(&fx, &dir);

    let mut state = state_with("a", &dir);
    claim(&mut state, &["default", "child"]);
    let peer = iroh::SecretKey::generate().public();
    state.add_peer(peer, "desktop".to_owned()).unwrap();
    let id = state.repos["a"].id.clone();
    let names = BTreeSet::from(["child".to_owned(), "desktop".to_owned()]);
    state.peer_claims.insert(
        peer,
        WorkspaceClaims {
            version: 1,
            repos: [(id, names)].into(),
        },
    );

    let set = start("snapshot-interval = 0\nupdate-stale = true", state);

    wait_for(&set, |s| {
        matches!(
            local_state(s, "child"),
            Some(WorkspaceState::Contested { .. })
        )
    })
    .await;
    let [status] = &set.statuses()[..] else {
        panic!("one repo expected");
    };
    let peers: Vec<&str> = status
        .workspaces
        .iter()
        .filter(|w| matches!(&w.place, WorkspacePlace::Peer { machine } if machine == "desktop"))
        .map(|w| w.name.as_str())
        .collect();
    assert_eq!(peers, ["child", "desktop"]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !fx.jj_ok(&child, &["status"]),
        "a contested workspace must stay untouched"
    );
}

/// A claimed workspace whose directory goes missing keeps its claim while
/// the view names it, and shows as missing.
#[tokio::test]
async fn keeps_claims_of_missing_workspaces() {
    let fx = Fixture::new();
    let dir = fx.init_repo("a");
    fx.jj(&dir, &["workspace", "add", "../child"]);
    let mut state = state_with("a", &dir);
    claim(&mut state, &["default", "child"]);
    let parked = fx.path().join("parked");
    std::fs::rename(fx.path().join("child"), &parked).unwrap();

    let (set, mut claims) = claiming_repo_set(QUIET);
    set.sync(&state);

    wait_for(&set, |s| {
        matches!(s, [status] if status.workspaces.iter().any(|w| {
            w.name == "child" && matches!(w.place, WorkspacePlace::Missing)
        }))
    })
    .await;
    assert!(claims.try_recv().is_err(), "the claim must be kept");
}

//! Crash-point tests of the sync apply: a fetch is interrupted at each
//! step of the apply, the repo is reopened like the daemon on watch
//! start, and the sync is retried with the heads the daemon would fetch.
//! The repo must stay consistent at every step and end identical to the
//! server's.
//!
//! The crash points are the `fail_point!` sites in the crate, enabled
//! through `fail`. Failpoints are process-global, so this is a separate
//! test binary and `fail`'s scenario guard serializes the cases.

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::MetadataExt as _,
    path::{Path, PathBuf},
    sync::Arc,
};

use jj_lib::{object_id::ObjectId as _, op_store::OperationId};
use jj_mesh::{repo::OpenRepo, testing::*};

/// The last apply step completed before a crash point, in apply order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Landed {
    /// The git objects, written before the apply.
    Objects,
    KeepRefs,
    /// All ops and views.
    Persist,
    Extras,
    Index,
    Mirror,
    /// The first of the wants.
    Head,
}

/// A crash point: its `fail` name and action, and the last step completed before it.
#[derive(Clone, Copy)]
struct Point {
    name: &'static str,
    action: &'static str,
    landed: Landed,
}

const BEFORE_STAGE: Point = Point {
    name: "fetch.before_stage",
    action: "return",
    landed: Landed::Objects,
};
const AFTER_KEEP_REFS: Point = Point {
    name: "stage.after_keep_refs",
    action: "return",
    landed: Landed::KeepRefs,
};
/// After the first view file is renamed into place: a partial persist.
const AFTER_VIEW_RENAME: Point = Point {
    name: "persist.after_rename",
    action: "return",
    landed: Landed::KeepRefs,
};
/// After the fourth rename, the first op: needs three ops or more.
const AFTER_OP_RENAME: Point = Point {
    name: "persist.after_rename",
    action: "3*off->return",
    landed: Landed::KeepRefs,
};
const AFTER_PERSIST: Point = Point {
    name: "stage.after_persist",
    action: "return",
    landed: Landed::Persist,
};
const AFTER_EXTRAS: Point = Point {
    name: "stage.after_extras",
    action: "return",
    landed: Landed::Extras,
};
const AFTER_INDEX: Point = Point {
    name: "fetch.after_index",
    action: "return",
    landed: Landed::Index,
};
const AFTER_MIRROR: Point = Point {
    name: "publish.after_mirror",
    action: "return",
    landed: Landed::Mirror,
};
/// After the first of several wants is published.
const AFTER_HEAD: Point = Point {
    name: "publish.after_head",
    action: "return",
    landed: Landed::Head,
};

/// A server and a fetcher, and the expected fetcher state after the sync.
struct Scenario {
    fx: Fixture,
    server: PathBuf,
    fetcher: PathBuf,
    format: GitTransferFormat,
    /// Whether the fetcher's own heads remain after the sync.
    union: bool,
    /// Expected branch targets in the fetcher's git.
    refs: Vec<(String, String)>,
}

impl Scenario {
    fn new(fx: Fixture, server: PathBuf, fetcher: PathBuf) -> Self {
        Scenario {
            fx,
            server,
            fetcher,
            format: GitTransferFormat::Loose,
            union: false,
            refs: vec![],
        }
    }

    fn pack(mut self) -> Self {
        self.format = GitTransferFormat::Pack;
        self
    }

    fn union(mut self) -> Self {
        self.union = true;
        self
    }

    /// Expects `branch` at the same commit as in the server's git.
    fn mirrored(mut self, branch: &str) -> Self {
        let sha = git_rev_at(
            open(&self.server).git_repo_path(),
            &format!("refs/heads/{branch}"),
        );
        self.refs.push((branch.to_owned(), sha));
        self
    }
}

/// Fast-forward into a colocated fetcher: the mirror updates its `.git`.
async fn fast_forward() -> Scenario {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    let b = fx.path().join("b");
    fork(&a, &b);
    fx.commit_file(&a, "file.txt", "add file");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@-"]);
    fx.jj(&a, &["new", "-m", "export"]);
    Scenario::new(fx, a, b).mirrored("main")
}

/// Divergent histories: the fetcher keeps its own head.
async fn divergent() -> Scenario {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    let b = fx.path().join("b");
    fork(&a, &b);
    fx.commit_file(&a, "a.txt", "from a");
    fx.commit_file(&b, "b.txt", "from b");
    Scenario::new(fx, a, b).union()
}

/// Clone into a fresh non-colocated repo, as a pack.
async fn clone_pack() -> Scenario {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    fx.commit_file(&a, "file.txt", "add file");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@-"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.init_pull_target("b", "machine-b");
    Scenario::new(fx, a, b).mirrored("main").pack().union()
}

/// Two wants: the local head is kept after both.
async fn multi_want() -> Scenario {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    let b = fx.path().join("b");
    let c = fx.path().join("c");
    fork(&a, &b);
    fork(&a, &c);
    fx.commit_file(&a, "a.txt", "from a");
    fx.commit_file(&b, "b.txt", "from b");
    fx.commit_file(&c, "c.txt", "from c");
    let (ra, rc) = (open(&a), open(&c));
    sync_once(&ra, &rc, &rc.op_heads().await.unwrap()).await;
    assert_eq!(ra.op_heads().await.unwrap().len(), 2);
    Scenario::new(fx, a, b).union()
}

/// A branch moved by the user in the colocated `.git` is not overwritten.
async fn colocated_user_branch() -> Scenario {
    let fx = Fixture::new();
    let a = fx.path().join("a");
    fx.jj(fx.path(), &["git", "init", "--colocate", "a"]);
    fx.jj(&a, &["describe", "-m", "base"]);
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.path().join("b");
    fork(&a, &b);

    let b_git = b.join(".git");
    let user = git_output(&b_git, &["commit-tree", "HEAD^{tree}", "-m", "user"]);
    git(&b_git, &["update-ref", "refs/heads/main", &user]);

    fx.commit_file(&a, "file.txt", "advance");
    fx.jj(&a, &["bookmark", "set", "main", "-r", "@-"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let mut scenario = Scenario::new(fx, a, b);
    scenario.refs.push(("main".to_owned(), user));
    scenario
}

/// Non-colocated fetcher: the mirror writes the git store, healed on reopen.
async fn non_colocated() -> Scenario {
    let fx = Fixture::new();
    let a = fx.init_repo("a");
    fx.jj(&a, &["bookmark", "create", "main", "-r", "@"]);
    fx.jj(&a, &["new", "-m", "export"]);
    let b = fx.init_pull_target("b", "machine-b");
    let (ra, rb) = (open(&a), open(&b));
    sync_missing(&rb, &ra).await;
    fx.jj(&b, &["status"]);
    sync_missing(&ra, &rb).await;
    fx.jj(&a, &["status"]);

    fx.commit_file(&a, "file.txt", "advance");
    fx.jj(&a, &["bookmark", "set", "main", "-r", "@-"]);
    fx.jj(&a, &["new", "-m", "export"]);
    Scenario::new(fx, a, b).mirrored("main")
}

/// Interrupts a sync at `point`, reopens, retries and checks the repo.
async fn run(build: impl AsyncFnOnce() -> Scenario, point: Point) {
    let _guard = fail::FailScenario::setup();
    let scenario = build().await;
    let server = open(&scenario.server);
    let server_heads = server.op_heads().await.unwrap();
    let fetcher = open(&scenario.fetcher);
    let before = fetcher.op_heads().await.unwrap();
    let mut expected = server_heads.clone();
    if scenario.union {
        expected.extend(before.iter().cloned());
    }
    expected.sort_unstable();

    let wants = fetcher.missing_heads(&server_heads).unwrap();
    assert!(!wants.is_empty());
    if point.landed == Landed::Head {
        assert!(wants.len() > 1, "the point needs several wants");
    }
    fail::cfg(point.name, point.action).unwrap();
    let err = try_sync(&fetcher, &server, &wants, scenario.format)
        .await
        .unwrap_err();
    fail::remove(point.name);
    let expected_err = format!("crash point {}", point.name);
    assert!(format!("{err:#}").contains(&expected_err), "{err:#}");

    drop(fetcher);
    let fetcher = open_healed(&scenario.fetcher).await;

    let heads = fetcher.op_heads().await.unwrap();
    if point.landed < Landed::Head {
        assert_eq!(heads, before, "heads moved before any publication");
    }
    for head in &heads {
        assert!(
            before.contains(head) || expected.contains(head),
            "unexpected head {}",
            head.hex(),
        );
    }
    if scenario.union {
        for head in &before {
            assert!(heads.contains(head), "local head {} unlisted", head.hex());
        }
    }
    assert_heads_load(&fetcher).await;
    assert_refs_resolve(&fetcher);
    // One head at a time: loading them together merges them.
    for head in &heads {
        scenario.fx.jj(
            &scenario.fetcher,
            &["op", "log", "--ignore-working-copy", "--at-op", &head.hex()],
        );
    }
    if point.landed >= Landed::KeepRefs {
        assert_keep_refs(&fetcher, &server, &wants).await;
    }
    for want in &wants {
        assert_eq!(
            fetcher.has_operation(want).await.unwrap(),
            point.landed >= Landed::Persist,
            "want {} stored",
            want.hex(),
        );
    }
    if point.landed >= Landed::Index {
        for want in &wants {
            assert!(fetcher.has_commit_index(want).await);
        }
    }
    // In a store only jj writes, the heal resets the refs to the published
    // heads and the retry moves them again.
    if point.landed >= Landed::Mirror && !fetcher.owns_git_refs() {
        assert_refs(&scenario, &fetcher);
    }
    let landed = snapshot_files(&fetcher);

    let wants = fetcher.missing_heads(&server_heads).unwrap();
    assert!(!wants.is_empty(), "the retry would fetch nothing");
    try_sync(&fetcher, &server, &wants, scenario.format)
        .await
        .unwrap();

    let mut heads = fetcher.op_heads().await.unwrap();
    heads.sort_unstable();
    assert_eq!(heads, expected);
    assert_heads_indexed(&fetcher).await;
    assert_refs(&scenario, &fetcher);
    assert_same_ops(&fetcher, &server, &server_heads).await;
    // A rewrite changes the inode or the mtime.
    let after = snapshot_files(&fetcher);
    for (path, meta) in &landed {
        assert_eq!(
            after.get(path),
            Some(meta),
            "{} was rewritten",
            path.display()
        );
    }
    scenario
        .fx
        .jj(&scenario.fetcher, &["op", "log", "--ignore-working-copy"]);
    scenario.fx.jj(
        &scenario.fetcher,
        &["log", "-r", "all()", "--ignore-working-copy"],
    );
}

/// Every op head and its view can be read.
async fn assert_heads_load(repo: &Arc<OpenRepo>) {
    for head in repo.op_heads().await.unwrap() {
        let op = repo.read_operation(&head).await.unwrap();
        repo.read_view(&op.view_id).await.unwrap();
    }
}

/// Every direct git ref points at a stored object.
fn assert_refs_resolve(repo: &Arc<OpenRepo>) {
    let git = repo.git_backend().git_repo();
    for reference in git.references().unwrap().all().unwrap() {
        let reference = reference.unwrap();
        if let Some(id) = reference.target().try_id() {
            assert!(
                git.has_object(id),
                "{} points at missing object {id}",
                reference.name().as_bstr(),
            );
        }
    }
}

/// The keep refs cover the view heads of every want.
async fn assert_keep_refs(fetcher: &Arc<OpenRepo>, server: &Arc<OpenRepo>, wants: &[OperationId]) {
    for want in wants {
        let op = server.read_operation(want).await.unwrap();
        let view = server.read_view(&op.view_id).await.unwrap();
        for commit in &view.head_ids {
            let keep = format!("refs/jj/keep/{}", commit.hex());
            assert!(
                git_ok(
                    fetcher.git_repo_path(),
                    &["rev-parse", "--verify", "-q", &keep]
                ),
                "missing {keep}"
            );
        }
    }
}

/// The scenario's branches have the expected targets in the fetcher's git.
fn assert_refs(scenario: &Scenario, fetcher: &Arc<OpenRepo>) {
    for (branch, sha) in &scenario.refs {
        assert_eq!(
            &git_rev_at(fetcher.git_repo_path(), &format!("refs/heads/{branch}")),
            sha,
            "{branch}"
        );
    }
}

/// Every op and view reachable from `heads` is byte-identical on the fetcher.
async fn assert_same_ops(fetcher: &Arc<OpenRepo>, server: &Arc<OpenRepo>, heads: &[OperationId]) {
    for (id, op) in server.ancestors_until(heads, &[]).await.unwrap() {
        assert_eq!(
            fetcher.read_operation_bytes(&id).unwrap(),
            server.read_operation_bytes(&id).unwrap(),
            "op {}",
            id.hex(),
        );
        assert_eq!(
            fetcher.read_view_bytes(&op.view_id).unwrap(),
            server.read_view_bytes(&op.view_id).unwrap(),
            "view {}",
            op.view_id.hex(),
        );
    }
}

/// Inode and mtime of every op, view and git object file.
fn snapshot_files(repo: &Arc<OpenRepo>) -> BTreeMap<PathBuf, (u64, i64, i64)> {
    let mut files = BTreeMap::new();
    for dir in [
        repo.op_store_dir().join("operations"),
        repo.op_store_dir().join("views"),
        repo.git_repo_path().join("objects"),
    ] {
        walk(&dir, &mut files);
    }
    files
}

fn walk(dir: &Path, files: &mut BTreeMap<PathBuf, (u64, i64, i64)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let meta = fs::metadata(&path).unwrap();
        if meta.is_dir() {
            walk(&path, files);
        } else if path.extension().is_none_or(|ext| ext != "keep") {
            files.insert(path, (meta.ino(), meta.mtime(), meta.mtime_nsec()));
        }
    }
}

/// One test per scenario and crash point: the common points plus the listed extras.
macro_rules! cases {
    ($($scenario:ident $(: $($extra:ident $extra_name:ident),+)?;)+) => {$(
        mod $scenario {
            cases!(@ $scenario: BEFORE_STAGE before_stage, AFTER_KEEP_REFS after_keep_refs,
                AFTER_VIEW_RENAME after_view_rename, AFTER_PERSIST after_persist,
                AFTER_EXTRAS after_extras, AFTER_INDEX after_index, AFTER_MIRROR after_mirror);
            $(cases!(@ $scenario: $($extra $extra_name),+);)?
        }
    )+};
    (@ $scenario:ident: $($point:ident $name:ident),+) => {$(
        #[tokio::test]
        async fn $name() {
            super::run(super::$scenario, super::$point).await;
        }
    )+};
}

cases! {
    fast_forward: AFTER_OP_RENAME after_op_rename;
    divergent;
    clone_pack: AFTER_OP_RENAME after_op_rename;
    multi_want: AFTER_HEAD after_head;
    colocated_user_branch: AFTER_OP_RENAME after_op_rename;
    non_colocated: AFTER_OP_RENAME after_op_rename;
}

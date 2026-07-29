//! Ignore-aware watching of a whole working copy.
//!
//! Watching a working copy recursively would register a kernel watch on
//! every directory, ignored build trees included: tens of thousands of
//! watches and a constant event stream to discard. Instead the tree is
//! walked with gitignore semantics and only non-ignored directories get a
//! watch, one non-recursive watch each. The same rules filter events for
//! ignored files inside watched directories, so the walk and the event
//! path can never disagree.
//!
//! This is a change signal for snapshot scheduling, not an exact tracker:
//! a spurious signal only costs a no-op snapshot, and a missed one is
//! absorbed by the next edit. The invariants that do matter:
//! - `.jj` and `.git` never signal (the daemon's own syncs, and jj's and
//!   the watcher's consumers' state churn, must not feed back);
//! - a directory that appears non-ignored, however it appeared (created,
//!   renamed in, unignored), starts being watched;
//! - the watcher's own cost stays bounded whatever the tree contains:
//!   rule files are read defensively, the event queue is bounded, and the
//!   walks rebuilding the watched set are coalesced and run off-thread.

use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::Read as _,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure, eyre};
use ignore::{
    Match,
    gitignore::{Gitignore, GitignoreBuilder},
};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Cap on watched directories. Each kernel watch pins an inode and costs
/// around a kilobyte of unswappable kernel memory, so this bounds what
/// one pathological tree takes from the machine (and from the other repos
/// sharing that budget). Changes under directories past the cap go
/// unnoticed until something else triggers a snapshot.
const MAX_WATCHED_DIRS: usize = 10_000;

/// Cap on a rule file's size. Rule files come from synced working copies,
/// so their size is not ours to trust; a megabyte is orders of magnitude
/// above any real one.
const MAX_RULES_BYTES: u64 = 1024 * 1024;

/// Bound on queued filesystem events. A build writing into watched
/// directories emits thousands per second while the consumer can be busy
/// for minutes (a fetch, a snapshot), so the queue must not grow with it.
/// Overflow degrades exactly like the kernel's own queue overflow: one
/// re-derivation and one change signal.
const SIGNAL_QUEUE: usize = 4096;

/// Name of the per-directory ignore file. jj reads gitignore files
/// whether or not the repo is colocated.
const GITIGNORE: &str = ".gitignore";

/// What the notify callback reports to the async side.
#[derive(Debug)]
enum Signal {
    Event(Event),
    Failed(String),
}

/// An ignore-aware watch on one working copy.
///
/// Dropping it stops notify's background threads.
pub struct TreeWatcher {
    root: PathBuf,
    watcher: RecommendedWatcher,
    signals: mpsc::Receiver<Signal>,
    /// Set by the notify callback when the queue is full, so events lost
    /// to backpressure are handled like a kernel queue overflow.
    overflowed: Arc<AtomicBool>,
    /// Directories currently holding a watch.
    watched: BTreeSet<PathBuf>,
    /// Ignore evaluation, shared with the walk that runs off-thread.
    rules: Arc<Mutex<Rules>>,
    /// Whether the watched set must be re-derived. Set while handling
    /// events and acted on once per batch: one walk is expensive, and a
    /// checkout can touch thousands of paths that each ask for one.
    stale: bool,
}

impl TreeWatcher {
    /// Starts watching the working copy rooted at `root` (a canonical
    /// path; event paths are matched against it by prefix).
    ///
    /// The initial walk plus one watch registration per directory is
    /// seconds of blocking syscalls on a large tree, so it runs on a
    /// blocking thread.
    pub async fn new(root: &Path) -> Result<Self> {
        let root = root.to_owned();
        tokio::task::spawn_blocking(move || Self::build(&root))
            .await
            .wrap_err("working copy watch task failed")?
    }

    /// Builds the watcher and its initial watched set. Blocking.
    fn build(root: &Path) -> Result<Self> {
        let (tx, signals) = mpsc::channel(SIGNAL_QUEUE);
        let overflowed = Arc::new(AtomicBool::new(false));
        let full = overflowed.clone();

        let backend = notify::recommended_watcher(move |event: notify::Result<Event>| {
            let signal = match event {
                Err(err) => Some(Signal::Failed(format!("watch backend error: {err}"))),
                // Access events never count: reads (the consumer's own
                // snapshots included) must not feed back into the watch.
                Ok(event) if event.kind.is_access() => None,
                Ok(mut event) => {
                    event.paths.retain(|path| !is_internal(path));
                    (!event.paths.is_empty() || event.need_rescan()).then_some(Signal::Event(event))
                }
            };
            if let Some(signal) = signal
                && tx.try_send(signal).is_err()
            {
                full.store(true, Ordering::Relaxed);
            }
        })
        .wrap_err("cannot create filesystem watcher")?;

        let mut watch = TreeWatcher {
            root: root.to_owned(),
            watcher: backend,
            signals,
            overflowed,
            watched: BTreeSet::new(),
            rules: Arc::new(Mutex::new(Rules::new(root))),
            stale: false,
        };
        let dirs = walk_dirs(&watch.root, &watch.rules)?;
        watch.apply(dirs);
        Ok(watch)
    }

    /// Waits for the next change to a non-ignored file or directory.
    /// Errors when the watch is dead (root gone, backend failed) and the
    /// whole watcher must be rebuilt.
    pub async fn changed(&mut self) -> Result<()> {
        loop {
            let signal = self
                .signals
                .recv()
                .await
                .ok_or_else(|| eyre!("filesystem watcher stopped"))?;
            let mut relevant = self.handle(signal)?;

            // Whatever is already queued is handled in the same batch, so
            // an event burst costs at most one walk.
            while let Ok(signal) = self.signals.try_recv() {
                relevant |= self.handle(signal)?;
            }
            relevant |= self.take_overflow();
            self.settle().await?;

            if relevant {
                return Ok(());
            }
        }
    }

    /// Drops the events queued so far without reporting them as changes,
    /// while keeping the watched set correct. Lets the caller swallow the
    /// events its own working-copy writes caused, which would otherwise
    /// schedule a snapshot of that very work.
    pub async fn discard_queued(&mut self) -> Result<()> {
        while let Ok(signal) = self.signals.try_recv() {
            self.handle(signal)?;
        }
        self.take_overflow();
        self.settle().await
    }

    /// Re-derives the watched set if anything in this batch invalidated
    /// it.
    async fn settle(&mut self) -> Result<()> {
        if std::mem::take(&mut self.stale) {
            let root = self.root.clone();
            let rules = self.rules.clone();
            // On a large tree the walk is hundreds of milliseconds of
            // syscalls: it must not sit on a runtime worker.
            let dirs = tokio::task::spawn_blocking(move || walk_dirs(&root, &rules))
                .await
                .wrap_err("working copy walk task failed")??;
            self.apply(dirs);
        }
        Ok(())
    }

    /// Consumes the overflow flag, treating lost events as a full
    /// re-derivation plus one change.
    fn take_overflow(&mut self) -> bool {
        if self.overflowed.swap(false, Ordering::Relaxed) {
            debug!(root = %self.root.display(), "working copy event queue overflowed");
            self.stale = true;
            return true;
        }
        false
    }

    /// Interprets one signal, returning whether it is a real change.
    fn handle(&mut self, signal: Signal) -> Result<bool> {
        let event = match signal {
            Signal::Event(event) => event,
            Signal::Failed(msg) => bail!(msg),
        };

        // The kernel dropped events: the only safe answer is to
        // re-derive everything and report a change.
        if event.need_rescan() {
            self.stale = true;
            return Ok(true);
        }

        let mut relevant = false;
        for path in &event.paths {
            relevant |= self.handle_path(path)?;
        }
        Ok(relevant)
    }

    /// Interprets one changed path. What the path *is* now decides how it
    /// is treated, not what the event claims happened to it: a rename
    /// reports the same kind whether the path appeared or disappeared,
    /// and only the filesystem knows which.
    fn handle_path(&mut self, path: &Path) -> Result<bool> {
        if path == self.root {
            // The root's own disappearance is the one event its children
            // cannot report; anything else about it (its mtime moving as
            // children come and go) is their business, not its own.
            ensure!(
                path.exists(),
                "the watched working copy root was removed or moved",
            );
            return Ok(false);
        }

        // An ignore-rule change moves the tracked/ignored boundary, and
        // is an edit in itself.
        if path.file_name().is_some_and(|name| name == GITIGNORE) {
            self.rules.lock().unwrap().forget(path.parent());
            self.stale = true;
            return Ok(true);
        }

        match fs::symlink_metadata(path) {
            // Symlinks are content to jj, never directories to descend.
            Ok(meta) if meta.is_dir() => {
                // Already watched: a metadata event on the directory
                // itself, whose children report themselves.
                if self.watched.contains(path) || self.ignored(path, true) {
                    return Ok(false);
                }
                // It needs watches, and whatever landed inside it before
                // they existed was never seen: re-derive and signal.
                self.stale = true;
                Ok(true)
            }
            Ok(_) => Ok(!self.ignored(path, false)),
            // Gone: removed, or renamed away. A watched directory loses
            // its kernel watch with it, so re-derive (a recreation must
            // be seen as new).
            Err(_) => {
                if self.watched.contains(path) {
                    self.stale = true;
                    return Ok(true);
                }
                // Directory-only rules cannot be evaluated against
                // something that no longer exists; treating it as a file
                // at worst reports a change costing a no-op snapshot.
                Ok(!self.ignored(path, false))
            }
        }
    }

    fn ignored(&self, path: &Path, is_dir: bool) -> bool {
        self.rules.lock().unwrap().is_ignored(path, is_dir)
    }

    /// Moves the kernel watches onto a freshly derived directory set.
    fn apply(&mut self, dirs: BTreeSet<PathBuf>) {
        for gone in self.watched.difference(&dirs) {
            // Failures are fine: the kernel drops watches of removed
            // directories on its own.
            let _ = self.watcher.unwatch(gone);
        }
        for new in dirs.difference(&self.watched) {
            if let Err(err) = self.watcher.watch(new, RecursiveMode::NonRecursive) {
                // Racing a deletion is normal; anything else (a watch
                // limit) degrades that subtree, not the watcher.
                debug!(dir = %new.display(), "cannot watch directory: {err}");
            }
        }
        self.watched = dirs;
    }
}

impl std::fmt::Debug for TreeWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TreeWatcher")
            .field("root", &self.root)
            .field("watched", &self.watched.len())
            .finish_non_exhaustive()
    }
}

/// Walks the tree and returns the directories deserving a watch. The
/// crate's own ignore handling is off: [`Rules`] is the single source of
/// truth, and the only one reading rule files defensively.
fn walk_dirs(root: &Path, rules: &Arc<Mutex<Rules>>) -> Result<BTreeSet<PathBuf>> {
    let filter = rules.clone();
    let walk = ignore::WalkBuilder::new(root)
        .hidden(false) // Dotfiles are regular files to jj.
        .ignore(false) // jj reads gitignore files, not .ignore.
        .parents(false) // The root is the repo root; nothing above applies.
        .git_ignore(false)
        .git_global(false)
        .git_exclude(false)
        .require_git(false)
        .follow_links(false)
        .filter_entry(move |entry| {
            if matches!(entry.file_name().to_str(), Some(".jj" | ".git")) {
                return false;
            }
            // Only directories are collected, so files are pruned before
            // they cost an ignore evaluation.
            entry.file_type().is_some_and(|ty| ty.is_dir())
                && !filter.lock().unwrap().is_ignored(entry.path(), true)
        })
        .build();

    let mut dirs = BTreeSet::new();
    for entry in walk {
        match entry {
            Ok(entry) => {
                if dirs.len() >= MAX_WATCHED_DIRS {
                    warn!(
                        root = %root.display(),
                        "working copy exceeds {MAX_WATCHED_DIRS} watchable directories; \
                         changes past the cap will not trigger snapshots",
                    );
                    break;
                }
                dirs.insert(entry.into_path());
            }
            // Unreadable subtrees are skipped, not fatal: their contents
            // cannot be snapshotted either.
            Err(err) => debug!(root = %root.display(), "walk error: {err}"),
        }
    }
    if !dirs.contains(root) {
        bail!("the working copy root is gone or unreadable");
    }
    Ok(dirs)
}

/// Gitignore evaluation for one working copy: per-directory matchers,
/// built lazily from rule files and cached until those files change.
struct Rules {
    root: PathBuf,
    /// The user's global gitignore, lowest precedence.
    global: Gitignore,
    per_dir: HashMap<PathBuf, Option<Gitignore>>,
}

impl Rules {
    fn new(root: &Path) -> Self {
        Rules {
            root: root.to_owned(),
            global: Gitignore::global().0,
            per_dir: HashMap::new(),
        }
    }

    /// Whether `path` is ignored, evaluated deepest-first: the closest
    /// rule file with an opinion decides, and the global gitignore only
    /// speaks when none of them does.
    fn is_ignored(&mut self, path: &Path, is_dir: bool) -> bool {
        // The root is the working copy itself: never ignorable, and with
        // no in-tree ancestor to consult.
        if path == self.root {
            return false;
        }
        // Outside the root: not this working copy's business.
        if !path.starts_with(&self.root) {
            return true;
        }

        for dir in path.ancestors().skip(1) {
            if let Some(matcher) = self.matcher(dir) {
                match matcher.matched(path, is_dir) {
                    Match::Ignore(_) => return true,
                    Match::Whitelist(_) => return false,
                    Match::None => {}
                }
            }
            if dir == self.root {
                break;
            }
        }
        self.global.matched(path, is_dir).is_ignore()
    }

    /// The matcher for one directory's own rule files.
    fn matcher(&mut self, dir: &Path) -> Option<&Gitignore> {
        let is_root = dir == self.root;
        self.per_dir
            .entry(dir.to_owned())
            .or_insert_with(|| build_matcher(dir, is_root))
            .as_ref()
    }

    /// Drops one directory's cached matcher, after its rules changed.
    fn forget(&mut self, dir: Option<&Path>) {
        if let Some(dir) = dir {
            self.per_dir.remove(dir);
        }
    }
}

/// Builds the matcher for one directory: its `.gitignore`, plus
/// `.git/info/exclude` at the root of colocated repos. `None` when the
/// directory has no rules at all, which keeps the cache cheap.
fn build_matcher(dir: &Path, is_root: bool) -> Option<Gitignore> {
    let mut builder = GitignoreBuilder::new(dir);
    let mut any = false;
    // Lowest precedence first: within one matcher the last matching rule
    // wins, and git ranks a directory's own `.gitignore` above
    // `.git/info/exclude`.
    let files = [
        is_root.then(|| dir.join(".git").join("info").join("exclude")),
        Some(dir.join(GITIGNORE)),
    ];
    for rules in files.into_iter().flatten() {
        let Some(text) = read_rules(&rules) else {
            continue;
        };
        for line in text.lines() {
            // A malformed line is skipped and the valid remainder still
            // applies, like git.
            let _ = builder.add_line(None, line);
        }
        any = true;
    }
    any.then(|| builder.build().ok()).flatten()
}

/// Reads a rule file, refusing anything that is not a plain regular file
/// of sane size. Rule files arrive with synced working copies: a
/// `.gitignore` symlinked to `/dev/zero` would otherwise be read until
/// the daemon is killed for its memory use, and an oversized one would
/// compile into a matcher just as large. Symlinked rule files are skipped
/// outright, as git does.
fn read_rules(path: &Path) -> Option<String> {
    if !fs::symlink_metadata(path).is_ok_and(|meta| meta.is_file()) {
        return None;
    }
    let file = fs::File::open(path).ok()?;
    // Re-checked on the open handle: the path may have been swapped
    // between the two calls.
    let meta = file.metadata().ok()?;
    if !meta.is_file() || meta.len() > MAX_RULES_BYTES {
        return None;
    }

    let mut text = String::new();
    file.take(MAX_RULES_BYTES).read_to_string(&mut text).ok()?;
    Some(text)
}

/// Whether a path is inside jj's or git's own state: never a working-copy
/// change, and the bulk of self-inflicted event traffic.
fn is_internal(path: &Path) -> bool {
    path.components()
        .any(|c| matches!(c.as_os_str().to_str(), Some(".jj" | ".git")))
}

#[cfg(test)]
mod tests {
    use std::{os::unix::fs::symlink, time::Duration};

    use super::*;

    const QUIET: Duration = Duration::from_millis(300);
    const WAIT: Duration = Duration::from_secs(5);

    async fn assert_changed(watch: &mut TreeWatcher) {
        tokio::time::timeout(WAIT, watch.changed())
            .await
            .expect("no change detected")
            .unwrap();
    }

    /// Consumes queued signals until the watcher stays quiet, so earlier
    /// activity cannot leak into a negative assertion.
    async fn drain(watch: &mut TreeWatcher) {
        while let Ok(changed) = tokio::time::timeout(QUIET, watch.changed()).await {
            changed.unwrap();
        }
    }

    async fn assert_quiet(watch: &mut TreeWatcher) {
        let outcome = tokio::time::timeout(QUIET, watch.changed()).await;
        assert!(outcome.is_err(), "unexpected change reported");
    }

    /// A working copy root, canonicalized like the daemon's.
    fn workdir(tmp: &tempfile::TempDir) -> PathBuf {
        let root = tmp.path().join("wc");
        fs::create_dir(&root).unwrap();
        root.canonicalize().unwrap()
    }

    #[tokio::test]
    async fn reports_edits_to_tracked_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::write(root.join("file.rs"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    #[tokio::test]
    async fn skips_jj_and_git_internals() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::create_dir_all(root.join(".jj/repo")).unwrap();
        fs::create_dir_all(root.join(".git/objects")).unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::write(root.join(".jj/repo/op"), "x").unwrap();
        fs::write(root.join(".git/objects/pack"), "x").unwrap();
        assert_quiet(&mut watch).await;

        // .gitignore is not ".git": dotfiles still count.
        fs::write(root.join(".env"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    #[tokio::test]
    async fn skips_ignored_directories_and_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::write(root.join(GITIGNORE), "/target/\n*.log\n").unwrap();
        fs::create_dir(root.join("target")).unwrap();
        fs::create_dir(root.join("src")).unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        // The ignored directory holds no watch: churn inside is invisible.
        fs::create_dir(root.join("target/debug")).unwrap();
        fs::write(root.join("target/debug/out"), "x").unwrap();
        // An ignored file in a watched directory is filtered by matching.
        fs::write(root.join("src/build.log"), "x").unwrap();
        assert_quiet(&mut watch).await;

        fs::write(root.join("src/main.rs"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    #[tokio::test]
    async fn nested_gitignore_and_negations_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::write(root.join(GITIGNORE), "*.log\n").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub").join(GITIGNORE), "!keep.log\n").unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::write(root.join("sub/noise.log"), "x").unwrap();
        assert_quiet(&mut watch).await;

        // The deeper negation wins over the root-level ignore.
        fs::write(root.join("sub/keep.log"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    /// git ranks a directory's own `.gitignore` above `.git/info/exclude`;
    /// so must the watcher, or it filters out edits jj tracks.
    #[tokio::test]
    async fn gitignore_outranks_git_exclude() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::create_dir_all(root.join(".git/info")).unwrap();
        fs::write(root.join(".git/info/exclude"), "*.log\n").unwrap();
        fs::write(root.join(GITIGNORE), "!important.log\n").unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::write(root.join("boring.log"), "x").unwrap();
        assert_quiet(&mut watch).await;

        fs::write(root.join("important.log"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    #[tokio::test]
    async fn new_directories_get_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        // The creation itself is a change (and triggers the walk that
        // watches the new directory).
        fs::create_dir(root.join("newdir")).unwrap();
        assert_changed(&mut watch).await;
        drain(&mut watch).await;

        fs::write(root.join("newdir/file"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    /// A directory renamed within the tree must keep being watched under
    /// its new name: renames are how editors and refactors move code, and
    /// the destination is not a removal however notify labels the event.
    #[tokio::test]
    async fn renamed_directories_get_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::create_dir(root.join("before")).unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::rename(root.join("before"), root.join("after")).unwrap();
        assert_changed(&mut watch).await;
        drain(&mut watch).await;

        fs::write(root.join("after/file"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    /// The same for a directory moved in from outside the working copy.
    #[tokio::test]
    async fn directories_moved_in_get_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        let outside = tmp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::rename(&outside, root.join("moved")).unwrap();
        assert_changed(&mut watch).await;
        drain(&mut watch).await;

        fs::write(root.join("moved/file"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    #[tokio::test]
    async fn gitignore_change_rescans() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::create_dir(root.join("logs")).unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::write(root.join(GITIGNORE), "/logs/\n").unwrap();
        // The rule edit is itself a change...
        assert_changed(&mut watch).await;
        drain(&mut watch).await;

        // ...after which the newly ignored directory goes quiet.
        fs::write(root.join("logs/app"), "x").unwrap();
        assert_quiet(&mut watch).await;
    }

    /// A rule file that is a symlink, a device or oversized must not be
    /// read: a `.gitignore` pointing at `/dev/zero` in a synced working
    /// copy would otherwise be read until the daemon dies of it.
    #[tokio::test]
    async fn refuses_unreasonable_rule_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        symlink("/dev/zero", root.join(GITIGNORE)).unwrap();

        // Both the walk and the event path must survive it.
        let mut watch = tokio::time::timeout(WAIT, TreeWatcher::new(&root))
            .await
            .expect("building the watcher must not hang on /dev/zero")
            .unwrap();
        fs::write(root.join("file.rs"), "x").unwrap();
        assert_changed(&mut watch).await;

        // Oversized rules are skipped rather than compiled.
        let big = tmp.path().join("big");
        fs::write(
            &big,
            vec![b'a'; usize::try_from(MAX_RULES_BYTES).unwrap() + 1],
        )
        .unwrap();
        assert_eq!(read_rules(&big), None);
    }

    /// Symlinks are content, not directories to descend: treating one as
    /// a directory would re-derive the watched set on its every event.
    #[tokio::test]
    async fn symlinked_directories_are_not_watched() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        fs::create_dir(root.join("real")).unwrap();
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        symlink(root.join("real"), root.join("link")).unwrap();
        assert_changed(&mut watch).await;
        drain(&mut watch).await;

        assert!(
            !watch.watched.contains(&root.join("link")),
            "a symlink must never hold a watch",
        );
    }

    /// Events queued while the consumer was away can be dropped on the
    /// floor without losing the watched set.
    #[tokio::test]
    async fn discards_queued_events() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::create_dir(root.join("dir")).unwrap();
        fs::write(root.join("file"), "x").unwrap();
        tokio::time::sleep(QUIET).await;
        watch.discard_queued().await.unwrap();
        assert_quiet(&mut watch).await;

        // The directory that appeared meanwhile is watched all the same.
        fs::write(root.join("dir/inner"), "x").unwrap();
        assert_changed(&mut watch).await;
    }

    #[tokio::test]
    async fn root_removal_kills_the_watch() {
        let tmp = tempfile::tempdir().unwrap();
        let root = workdir(&tmp);
        let mut watch = TreeWatcher::new(&root).await.unwrap();

        fs::remove_dir_all(&root).unwrap();
        let outcome = tokio::time::timeout(WAIT, watch.changed()).await;
        assert!(
            matches!(outcome, Ok(Err(_))),
            "root removal must surface as a watch error",
        );
    }
}

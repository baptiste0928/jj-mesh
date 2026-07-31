//! Gitignore evaluation for one working copy: per-directory matchers,
//! built lazily from rule files and cached until those files change.
//!
//! This is the single source of truth for ignore decisions, shared by the
//! tree walk and the event path so they can never disagree. Rule files
//! come from synced working copies, so they are read defensively: sizes
//! are capped and symlinks refused.

use std::{
    collections::HashMap,
    fs,
    io::Read as _,
    path::{Path, PathBuf},
};

use ignore::{
    Match,
    gitignore::{Gitignore, GitignoreBuilder},
};

/// Name of the per-directory ignore file. jj reads gitignore files
/// whether or not the repo is colocated.
pub(super) const GITIGNORE: &str = ".gitignore";

/// Cap on a rule file's size; a megabyte is orders of magnitude above any
/// real one.
const MAX_RULES_BYTES: u64 = 1024 * 1024;

/// Gitignore evaluation state for one working copy.
pub(super) struct Rules {
    root: PathBuf,
    /// The user's global gitignore, lowest precedence.
    global: Gitignore,
    per_dir: HashMap<PathBuf, Option<Gitignore>>,
}

impl Rules {
    pub(super) fn new(root: &Path) -> Self {
        Rules {
            root: root.to_owned(),
            global: Gitignore::global().0,
            per_dir: HashMap::new(),
        }
    }

    /// Whether `path` is ignored, evaluated deepest-first: the closest
    /// rule file with an opinion decides, and the global gitignore only
    /// speaks when none of them does.
    pub(super) fn is_ignored(&mut self, path: &Path, is_dir: bool) -> bool {
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
    pub(super) fn forget(&mut self, dir: Option<&Path>) {
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
/// of sane size: a `.gitignore` symlinked to `/dev/zero` would otherwise
/// be read until the daemon is killed for its memory use, and an
/// oversized one would compile into a matcher just as large. Symlinked
/// rule files are skipped outright, as git does.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Oversized rule files must be skipped rather than compiled.
    #[test]
    fn oversized_rule_files_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let big = tmp.path().join("big");
        fs::write(
            &big,
            vec![b'a'; usize::try_from(MAX_RULES_BYTES).unwrap() + 1],
        )
        .unwrap();
        assert_eq!(read_rules(&big), None);
    }
}

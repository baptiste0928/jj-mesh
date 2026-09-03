//! The git ref mirror: before op heads get published, moves the git
//! repo's refs to what jj's merged view of the new heads carries, so
//! jj's next import finds nothing to undo (see the sync docs).
//!
//! The merge replays jj's own view merge on the git refs, with jj's code:
//! the op heads fold pairwise, each pair three-way merged against its
//! closest common ancestors, and each ref merged by `merge_ref_targets`
//! over the commit index (which picks the descendant when both sides moved
//! a ref along one line). Each ref then moves in git from a "before" value
//! to the merged "after" value by compare-and-swap. The "before" depends
//! on who else writes the git repo:
//!
//! - Git repo inside `.jj`: only jj writes there, so "before" is what the
//!   repo actually holds and any difference from the merged view is
//!   staleness. [`heal`] runs the same pass on watch start.
//! - Colocated `.git` or an external git repo: the user may have moved
//!   refs there that jj has not imported yet, so "before" is the merged
//!   view of the heads before the publication, and a ref moved by the
//!   user fails its swap and stays for jj to import.
//!
//! Only refs jj imports are touched, whichever names the replicated views
//! carry. Refs that conflict on either side are left alone: a conflict
//! has no representation in git, and jj's import resolves it to the local
//! git value on every machine.
//!
//! ```text
//!   read heads ──► merge refs ──► merge refs ──► swap ──► publish
//!                  (after)        (before)       refs     op heads
//! ```

use std::{
    cell::OnceCell,
    collections::{BTreeMap, BTreeSet, HashSet},
};

use color_eyre::eyre::{Result, eyre};
use jj_lib::{
    backend::CommitId,
    git::{RESERVED_REMOTE_REF_NAMESPACE, parse_git_ref},
    index::{Index, MutableIndex},
    op_store::{OperationId, RefTarget},
    op_walk,
    operation::Operation,
    ref_name::{GitRefName, GitRefNameBuf},
    refs::merge_ref_targets,
};
use pollster::FutureExt as _;
use tracing::warn;

use super::to_gix_id;
use crate::repo::OpenRepo;

type GitRefs = BTreeMap<GitRefNameBuf, RefTarget>;

/// The ref namespaces jj imports; [`is_imported`] narrows them further.
const NAMESPACES: [&str; 3] = ["refs/heads/", "refs/remotes/", "refs/tags/"];

/// Mirrors what publishing `to_publish` (each want with the local heads
/// it supersedes) changes in jj's merged git refs. The heads are re-read
/// here, not taken from the fetch's snapshot: an operation the user ran
/// during the transfer keeps its refs. A ref whose swap fails is logged
/// and left to jj's import. A no-op when nothing is published.
pub(super) fn run(repo: &OpenRepo, to_publish: &[(OperationId, Vec<OperationId>)]) -> Result<()> {
    if to_publish.is_empty() {
        return Ok(());
    }
    let heads = repo.op_heads().block_on()?;
    let superseded: HashSet<&OperationId> = to_publish.iter().flat_map(|(_, s)| s).collect();
    let after: Vec<OperationId> = heads
        .iter()
        .filter(|head| !superseded.contains(head))
        .chain(to_publish.iter().map(|(want, _)| want))
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let merger = Merger::new(repo, heads.iter().chain(&after))?;
    let after = merger.merged_refs(&after)?;
    let before = if repo.owns_git_refs() {
        stored_refs(repo, &after)?
    } else {
        merger.merged_refs(&heads)?
    };
    swap(repo, &before, &after)
}

/// Brings a git repo only jj writes to in line with the merged refs of
/// the current op heads. A no-op for a colocated or external git repo.
pub fn heal(repo: &OpenRepo) -> Result<()> {
    if !repo.owns_git_refs() {
        return Ok(());
    }
    let heads = repo.op_heads().block_on()?;
    let after = Merger::new(repo, heads.iter())?.merged_refs(&heads)?;
    swap(repo, &stored_refs(repo, &after)?, &after)
}

/// Merges the git refs of sets of operations, over a commit index that
/// covers `ops` and is built on first use: a single head needs no merge,
/// and a divergence with no ref touched on both sides needs no index.
struct Merger<'a> {
    repo: &'a OpenRepo,
    ops: Vec<Operation>,
    index: OnceCell<Box<dyn MutableIndex>>,
}

impl<'a> Merger<'a> {
    fn new(repo: &'a OpenRepo, ops: impl Iterator<Item = &'a OperationId>) -> Result<Self> {
        let ids: BTreeSet<&OperationId> = ops.collect();
        let ops = ids
            .into_iter()
            .map(|id| repo.load_operation(id).block_on())
            .collect::<Result<_>>()?;
        Ok(Self {
            repo,
            ops,
            index: OnceCell::new(),
        })
    }

    /// The git refs of jj's merged view of the operations `ids`.
    fn merged_refs(&self, ids: &[OperationId]) -> Result<GitRefs> {
        let ops: Vec<Operation> = ids
            .iter()
            .map(|id| self.repo.load_operation(id).block_on())
            .collect::<Result<_>>()?;
        self.merge(&ops)
    }

    /// Follows `RepoLoader::merge_operations`: the ops fold left to
    /// right, each merged into the accumulated view against the closest
    /// common ancestors of the ops folded so far and itself (themselves
    /// merged first when several).
    fn merge(&self, ops: &[Operation]) -> Result<GitRefs> {
        let (first, rest) = ops
            .split_first()
            .ok_or_else(|| eyre!("no op head to merge"))?;
        let mut refs = view_refs(first)?;
        let mut folded = vec![first.clone()];
        for other in rest {
            let ancestors =
                op_walk::closest_common_ancestors(folded.clone(), [other.clone()]).block_on()?;
            let base = self.merge(&ancestors)?;
            let theirs = view_refs(other)?;
            let names: BTreeSet<&GitRefName> = [&refs, &base, &theirs]
                .into_iter()
                .flat_map(|refs| refs.keys().map(GitRefNameBuf::as_ref))
                .collect();
            let mut result = GitRefs::new();
            for name in names {
                let merged = merge_ref_targets(
                    self.index()?,
                    target(&refs, name),
                    target(&base, name),
                    target(&theirs, name),
                )
                .block_on()?;
                if merged.is_present() {
                    result.insert(name.to_owned(), merged);
                }
            }
            refs = result;
            folded.push(other.clone());
        }
        Ok(refs)
    }

    /// The commit index covering every op, as jj builds one to merge
    /// their views.
    fn index(&self) -> Result<&dyn Index> {
        if self.index.get().is_none() {
            let (first, rest) = self
                .ops
                .split_first()
                .ok_or_else(|| eyre!("no op head to index"))?;
            let mut index = self.repo.index_at(first)?.start_modification();
            for op in rest {
                index.merge_in(self.repo.index_at(op)?.as_ref())?;
            }
            self.index.set(index).ok();
        }
        Ok(self.index.get().expect("set above").as_index())
    }
}

/// The git refs of an op's view.
fn view_refs(op: &Operation) -> Result<GitRefs> {
    Ok(op.view().block_on()?.git_refs().clone())
}

/// Whether jj's import records the ref in `git_refs`.
fn is_imported(name: &GitRefName) -> bool {
    !name.as_str().starts_with(RESERVED_REMOTE_REF_NAMESPACE) && parse_git_ref(name).is_some()
}

/// The imported refs the git repo holds, resolved to the commit jj
/// records for them. A ref is read as stored when that is the commit
/// `hint` gives it, and peeled (through symbolic refs and annotated
/// tags) otherwise; refs not reaching a commit are skipped, as jj's
/// import skips them.
fn stored_refs(repo: &OpenRepo, hint: &GitRefs) -> Result<GitRefs> {
    let git = repo.git_backend().git_repo();
    let platform = git.references()?;
    let mut refs = GitRefs::new();
    for namespace in NAMESPACES {
        for reference in platform.prefixed(namespace)? {
            let mut reference = reference.map_err(|err| eyre!("cannot read git ref: {err}"))?;
            let name = GitRefNameBuf::from(reference.name().as_bstr().to_string());
            if !is_imported(&name) {
                continue;
            }
            let hinted = target(hint, &name).as_normal().map(to_gix_id).transpose()?;
            let commit = match reference.target().try_id() {
                Some(id) if Some(id.to_owned()) == hinted => id.to_owned(),
                _ => match reference.peel_to_commit() {
                    Ok(commit) => commit.id,
                    Err(_) => continue,
                },
            };
            refs.insert(
                name,
                RefTarget::normal(CommitId::from_bytes(commit.as_bytes())),
            );
        }
    }
    Ok(refs)
}

/// The ref's target in a view, absent when unlisted.
fn target<'a>(refs: &'a GitRefs, name: &GitRefName) -> &'a RefTarget {
    refs.get(name).unwrap_or(RefTarget::absent_ref())
}

/// Moves each imported ref in the git repo from its `before` value to
/// its `after` value, skipping unchanged refs and conflicts on either
/// side.
fn swap(repo: &OpenRepo, before: &GitRefs, after: &GitRefs) -> Result<()> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog};

    let git = repo.git_backend().git_repo();
    let names: BTreeSet<&GitRefName> = [before, after]
        .into_iter()
        .flat_map(|refs| refs.keys().map(GitRefNameBuf::as_ref))
        .filter(|name| is_imported(name))
        .collect();
    for name in names {
        let (Some(before_id), Some(after_id)) = (
            target(before, name).as_resolved(),
            target(after, name).as_resolved(),
        ) else {
            continue;
        };
        if before_id == after_id {
            continue;
        }
        let Ok(full_name) = name.as_str().try_into() else {
            warn!(
                "skipping git ref mirror of {}: invalid ref name",
                name.as_symbol()
            );
            continue;
        };
        let expected = match before_id {
            Some(id) => PreviousValue::MustExistAndMatch(stored_target(&git, name, id)?),
            None => PreviousValue::MustNotExist,
        };
        let change = match after_id {
            Some(id) => Change::Update {
                log: LogChange::default(),
                expected,
                new: gix::refs::Target::Object(to_gix_id(id)?),
            },
            None => Change::Delete {
                expected,
                log: RefLog::AndReference,
            },
        };
        let edit = RefEdit {
            change,
            name: full_name,
            deref: false,
        };
        // Edits apply individually: one ref the user raced must not abort
        // the rest of the mirror.
        let moved = detach_head(&git, name).and_then(|()| {
            git.edit_references(Some(edit))
                .map_err(|err| eyre!("{err}"))
        });
        if let Err(err) = moved {
            warn!("skipping git ref mirror of {}: {err}", name.as_symbol());
        }
    }
    Ok(())
}

/// Detaches HEAD at its current commit when it is symbolic to `name`, as
/// jj's export does before moving a ref: a branch moved under HEAD leaves
/// git's index and worktree behind, and jj's next import would read the
/// jump as a checkout. A no-op when HEAD is detached, unborn, or on
/// another ref.
fn detach_head(git: &gix::Repository, name: &GitRefName) -> Result<()> {
    use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};

    let Some(mut head) = git.try_find_reference("HEAD")? else {
        return Ok(());
    };
    let stored = head.target().into_owned();
    if stored.try_name().map(gix::refs::FullNameRef::as_bstr) != Some(name.as_str().into()) {
        return Ok(());
    }
    let Ok(commit) = head.peel_to_commit() else {
        return Ok(());
    };
    let edit = RefEdit {
        change: Change::Update {
            log: LogChange::default(),
            expected: PreviousValue::MustExistAndMatch(stored),
            new: gix::refs::Target::Object(commit.id),
        },
        name: "HEAD".try_into().expect("valid ref name"),
        deref: false,
    };
    git.edit_references(Some(edit))
        .map_err(|err| eyre!("cannot detach HEAD: {err}"))?;
    Ok(())
}

/// What the git repo stores for a ref jj records at `commit`: the
/// commit itself, or the annotated tag or symbolic ref that peels to it.
/// Git compares stored targets, not peeled ones, so a swap must expect
/// the stored form.
fn stored_target(
    git: &gix::Repository,
    name: &GitRefName,
    commit: &CommitId,
) -> Result<gix::refs::Target> {
    let commit = to_gix_id(commit)?;
    let stored = git
        .try_find_reference(name.as_str())?
        .and_then(|mut reference| {
            let stored = reference.target().into_owned();
            (stored.try_id() != Some(&commit) && reference.peel_to_commit().ok()?.id == commit)
                .then_some(stored)
        });
    Ok(stored.unwrap_or(gix::refs::Target::Object(commit)))
}

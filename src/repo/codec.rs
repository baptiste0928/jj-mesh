//! Structural validation of replicated op store bytes.
//!
//! Ops and views replicate as the raw proto bytes stored on the sender
//! (see [`crate::net::fetch::OpFrame`]). This module decodes them with jj's
//! own proto schema and extracts what sync validation needs: the op DAG
//! shape and every commit id the object references, including legacy proto
//! forms still present in repos written by older jj versions. The bytes
//! themselves are stored verbatim, so fields this build does not know
//! about survive replication untouched.
//!
//! Parsing must reject every shape jj's own readers reject (or panic on):
//! once stored and reachable from a published op head, an unreadable
//! object would break every jj command in the repo. jj 0.44 requires a
//! ref target's value oneof to be set, native ref conflicts to have one
//! more add than removes, remote ref merges to have odd arity, and remote
//! ref states to be known enum values.

use std::collections::HashSet;

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};
use jj_lib::{
    backend::CommitId,
    op_store::{OperationId, ViewId},
    protos::simple_op_store as proto,
};
use prost::Message as _;

/// Id length of the simple op store (BLAKE2b-512), the only op store
/// backend jj-mesh supports (checked in [`crate::repo::JjRepo`]).
const STORE_ID_LEN: usize = 64;

/// The validated DAG-relevant fields of a replicated operation.
#[derive(Debug)]
pub struct OpMeta {
    pub view_id: ViewId,
    pub parents: Vec<OperationId>,
    /// Commits referenced by the op's predecessor records.
    pub referenced_commits: Vec<CommitId>,
}

/// Decodes a replicated operation's bytes and validates their structure.
pub fn parse_operation(bytes: &[u8]) -> Result<OpMeta> {
    let op = proto::Operation::decode(bytes).wrap_err("malformed operation bytes")?;
    ensure!(
        op.view_id.len() == STORE_ID_LEN,
        "operation has a bad view id length ({})",
        op.view_id.len(),
    );
    // Only the root operation is parentless, and it is never replicated;
    // jj asserts on reading any other parentless op.
    ensure!(!op.parents.is_empty(), "operation has no parents");
    for parent in &op.parents {
        ensure!(
            parent.len() == STORE_ID_LEN,
            "operation has a bad parent id length ({})",
            parent.len(),
        );
    }

    let mut ids = IdSet::default();
    for entry in &op.commit_predecessors {
        ids.add(&entry.commit_id);
        entry.predecessor_ids.iter().for_each(|id| ids.add(id));
    }

    Ok(OpMeta {
        view_id: ViewId::new(op.view_id),
        parents: op.parents.into_iter().map(OperationId::new).collect(),
        referenced_commits: ids.into_commit_ids(),
    })
}

/// The validated commit references of a replicated view.
#[derive(Debug)]
pub struct ViewMeta {
    /// The view's head commits.
    pub head_ids: Vec<CommitId>,
    /// Every commit id the view references: the heads, all ref targets
    /// (every conflict side, legacy encodings included) and working-copy
    /// commits.
    pub referenced_commits: Vec<CommitId>,
}

/// Decodes a replicated view's bytes and extracts its commit references.
// Repos written by older jj versions still contain the deprecated proto
// forms; reading them is the point.
#[expect(deprecated)]
pub fn parse_view(bytes: &[u8]) -> Result<ViewMeta> {
    let view = proto::View::decode(bytes).wrap_err("malformed view bytes")?;

    let mut heads = IdSet::default();
    view.head_ids.iter().for_each(|id| heads.add(id));

    let mut ids = heads.clone();
    ids.add(&view.wc_commit_id); // Legacy single-workspace form.
    view.wc_commit_ids.values().for_each(|id| ids.add(id));
    for bookmark in &view.bookmarks {
        ids.add_target(bookmark.local_target.as_ref())
            .wrap_err_with(|| format!("bookmark {}", bookmark.name))?;
        // Legacy per-bookmark remote refs, pre remote_views.
        for remote in &bookmark.remote_bookmarks {
            ids.add_target(remote.target.as_ref())
                .wrap_err_with(|| format!("bookmark {}@{}", bookmark.name, remote.remote_name))?;
            if let Some(state) = remote.state {
                check_remote_ref_state(state).wrap_err_with(|| {
                    format!("bookmark {}@{}", bookmark.name, remote.remote_name)
                })?;
            }
        }
    }
    for tag in &view.local_tags {
        ids.add_target(tag.target.as_ref())
            .wrap_err_with(|| format!("tag {}", tag.name))?;
    }
    for remote in &view.remote_views {
        for remote_ref in remote.bookmarks.iter().chain(&remote.tags) {
            ids.add_remote_ref(remote_ref)
                .wrap_err_with(|| format!("remote ref {}@{}", remote_ref.name, remote.name))?;
        }
    }
    for git_ref in &view.git_refs {
        // jj's tag migration for pre-0.34 views asserts on the empty tag
        // name this ref would produce.
        ensure!(
            git_ref.name != "refs/tags/",
            "git ref named exactly refs/tags/",
        );
        ids.add(&git_ref.commit_id); // Legacy pre-RefTarget form.
        ids.add_target(git_ref.target.as_ref())
            .wrap_err_with(|| format!("git ref {}", git_ref.name))?;
    }
    // Reading an unmigrated view (one an honest jj never writes, since it
    // always marks views migrated) runs jj's pre-0.34 Git-tracking tag
    // migration, which asserts the "git" remote holds no tags yet before
    // moving `refs/tags/*` there. A peer could otherwise craft a view that
    // panics jj's reader once stored.
    if !view.has_git_refs_migrated_to_remote_tags {
        let has_tag_refs = view.git_refs.iter().any(|git_ref| {
            git_ref
                .name
                .strip_prefix("refs/tags/")
                .is_some_and(|name| !name.is_empty())
        });
        let git_remote_has_tags = view
            .remote_views
            .iter()
            .any(|remote| remote.name == "git" && !remote.tags.is_empty());
        ensure!(
            !(has_tag_refs && git_remote_has_tags),
            "unmigrated view collides with jj's Git-tracking tag migration",
        );
    }
    ids.add(&view.git_head_legacy);
    ids.add_target(view.git_head.as_ref())
        .wrap_err("git head")?;

    Ok(ViewMeta {
        head_ids: heads.into_commit_ids(),
        referenced_commits: ids.into_commit_ids(),
    })
}

/// Deduplicating commit id collector. Empty ids are proto3 defaults of
/// absent legacy fields, not references, and are dropped.
#[derive(Clone, Default)]
struct IdSet(HashSet<Vec<u8>>);

impl IdSet {
    fn add(&mut self, id: &[u8]) {
        if !id.is_empty() {
            self.0.insert(id.to_vec());
        }
    }

    /// Adds every commit a ref target references, in any of its proto
    /// encodings, deprecated ones included. Rejects the shapes jj's reader
    /// panics on: a missing value oneof and native conflicts without one
    /// more add than removes.
    #[expect(deprecated)]
    fn add_target(&mut self, target: Option<&proto::RefTarget>) -> Result<()> {
        use proto::ref_target::Value;
        // A missing message is jj's legacy encoding of an absent target; a
        // present message must carry a value.
        let Some(target) = target else {
            return Ok(());
        };
        match &target.value {
            Some(Value::CommitId(id)) => self.add(id),
            Some(Value::ConflictLegacy(conflict)) => {
                for id in conflict.removes.iter().chain(&conflict.adds) {
                    self.add(id);
                }
            }
            Some(Value::Conflict(conflict)) => {
                ensure!(
                    conflict.adds.len() == conflict.removes.len() + 1,
                    "ref conflict with {} adds and {} removes",
                    conflict.adds.len(),
                    conflict.removes.len(),
                );
                for term in conflict.removes.iter().chain(&conflict.adds) {
                    term.value.iter().for_each(|id| self.add(id));
                }
            }
            None => bail!("ref target without a value"),
        }
        Ok(())
    }

    /// Adds a remote ref's commits, rejecting even merge arity and unknown
    /// states, which jj's reader refuses.
    fn add_remote_ref(&mut self, remote_ref: &proto::RemoteRef) -> Result<()> {
        ensure!(
            remote_ref.target_terms.len() % 2 == 1,
            "remote ref with even merge arity ({})",
            remote_ref.target_terms.len(),
        );
        check_remote_ref_state(remote_ref.state)?;
        for term in &remote_ref.target_terms {
            term.value.iter().for_each(|id| self.add(id));
        }
        Ok(())
    }

    fn into_commit_ids(self) -> Vec<CommitId> {
        self.0.into_iter().map(CommitId::new).collect()
    }
}

/// Rejects remote ref states outside jj's enum.
fn check_remote_ref_state(state: i32) -> Result<()> {
    ensure!(
        proto::RemoteRefState::try_from(state).is_ok(),
        "unknown remote ref state {state}",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use jj_lib::object_id::ObjectId as _;
    use pollster::FutureExt as _;

    use super::*;
    use crate::{repo::JjRepo, testing::Fixture};

    /// Parses every op and view of a real repo with non-trivial content:
    /// the extracted DAG must match what jj_lib reads, and the extracted
    /// commits must cover everything the views and predecessors reference.
    #[tokio::test]
    async fn parses_real_repo_data() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        fx.jj(&dir, &["bookmark", "create", "main", "-r", "@"]);
        fx.jj(&dir, &["new", "-m", "second"]);
        fx.jj(&dir, &["bookmark", "create", "feature", "-r", "@"]);
        fx.jj(&dir, &["describe", "-m", "amended"]);

        let repo = JjRepo::discover(&dir).unwrap().open().unwrap();
        let heads = repo.op_heads().await.unwrap();
        let ops = repo.ancestors_until(&heads, &[]).await.unwrap();
        assert!(!ops.is_empty());

        for (id, op) in &ops {
            let meta = parse_operation(&repo.read_operation_bytes(id).unwrap()).unwrap();
            assert_eq!(meta.view_id, op.view_id);
            assert_eq!(meta.parents, op.parents);
            let predecessors: HashSet<CommitId> = op
                .commit_predecessors
                .iter()
                .flatten()
                .flat_map(|(commit, preds)| preds.iter().chain([commit]).cloned())
                .collect();
            let extracted: HashSet<CommitId> = meta.referenced_commits.into_iter().collect();
            assert_eq!(extracted, predecessors);

            let view = repo.read_view(&op.view_id).block_on().unwrap();
            let meta = parse_view(&repo.read_view_bytes(&op.view_id).unwrap()).unwrap();
            let mut heads: Vec<CommitId> = view.head_ids.iter().cloned().collect();
            heads.sort_unstable();
            let mut extracted_heads = meta.head_ids.clone();
            extracted_heads.sort_unstable();
            assert_eq!(extracted_heads, heads);

            let referenced: HashSet<CommitId> = meta.referenced_commits.into_iter().collect();
            for head in &view.head_ids {
                assert!(referenced.contains(head));
            }
            for target in view.local_bookmarks.values() {
                for id in target.as_merge().iter().flatten() {
                    assert!(referenced.contains(id), "missing bookmark target {id:?}");
                }
            }
            for wc in view.wc_commit_ids.values() {
                assert!(referenced.contains(wc));
            }
        }
    }

    #[test]
    fn rejects_malformed_operations() {
        // Not a valid proto message.
        assert!(parse_operation(&[0xff, 0xff, 0xff]).is_err());

        // Parentless op (only the root is, and it is never replicated).
        let op = proto::Operation {
            view_id: vec![9; STORE_ID_LEN],
            ..Default::default()
        };
        let err = parse_operation(&op.encode_to_vec()).unwrap_err();
        assert!(err.to_string().contains("no parents"), "{err:#}");

        // Bad parent id length.
        let op = proto::Operation {
            view_id: vec![9; STORE_ID_LEN],
            parents: vec![vec![1; 8]],
            ..Default::default()
        };
        let err = parse_operation(&op.encode_to_vec()).unwrap_err();
        assert!(err.to_string().contains("parent id length"), "{err:#}");
    }

    /// Legacy proto forms (pre-RefTarget git refs, legacy conflicts, the
    /// single wc_commit_id) must contribute their commit ids.
    #[test]
    #[expect(deprecated)]
    fn extracts_legacy_view_forms() {
        let view = proto::View {
            wc_commit_id: vec![1; 20],
            git_refs: vec![proto::GitRef {
                name: "refs/heads/old".to_owned(),
                commit_id: vec![2; 20],
                target: None,
            }],
            git_head_legacy: vec![3; 20],
            bookmarks: vec![proto::Bookmark {
                name: "conflicted".to_owned(),
                local_target: Some(proto::RefTarget {
                    value: Some(proto::ref_target::Value::ConflictLegacy(
                        proto::RefConflictLegacy {
                            removes: vec![vec![4; 20]],
                            adds: vec![vec![5; 20], vec![6; 20]],
                        },
                    )),
                }),
                remote_bookmarks: vec![],
            }],
            ..Default::default()
        };

        let meta = parse_view(&view.encode_to_vec()).unwrap();
        let ids: HashSet<Vec<u8>> = meta
            .referenced_commits
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect();
        for byte in 1..=6u8 {
            assert!(ids.contains(&vec![byte; 20]), "missing id {byte}");
        }
        assert!(meta.head_ids.is_empty());
    }

    /// Shapes jj's own view reader panics or errors on must be rejected at
    /// parse time: once stored and reachable from a published head they
    /// would break every jj command in the repo.
    #[test]
    fn rejects_views_jj_cannot_read() {
        let with_git_head = |target: proto::RefTarget| proto::View {
            git_head: Some(target),
            ..Default::default()
        };

        // A present ref target message must carry a value (jj unwraps it).
        let view = with_git_head(proto::RefTarget { value: None });
        let err = parse_view(&view.encode_to_vec()).unwrap_err();
        assert!(format!("{err:#}").contains("without a value"), "{err:#}");

        // Native conflicts need one more add than removes (jj panics).
        let view = with_git_head(proto::RefTarget {
            value: Some(proto::ref_target::Value::Conflict(proto::RefConflict {
                removes: vec![],
                adds: vec![],
            })),
        });
        let err = parse_view(&view.encode_to_vec()).unwrap_err();
        assert!(format!("{err:#}").contains("ref conflict"), "{err:#}");

        // Remote ref merges need odd arity (jj errors on read).
        let remote_view = |remote_ref: proto::RemoteRef| proto::View {
            remote_views: vec![proto::RemoteView {
                name: "origin".to_owned(),
                bookmarks: vec![remote_ref],
                tags: vec![],
            }],
            ..Default::default()
        };
        let view = remote_view(proto::RemoteRef {
            name: "main".to_owned(),
            target_terms: vec![],
            state: 1,
        });
        let err = parse_view(&view.encode_to_vec()).unwrap_err();
        assert!(format!("{err:#}").contains("even merge arity"), "{err:#}");

        // Unknown remote ref states (jj errors on read).
        let view = remote_view(proto::RemoteRef {
            name: "main".to_owned(),
            target_terms: vec![proto::RefTargetTerm {
                value: Some(vec![1; 20]),
            }],
            state: 7,
        });
        let err = parse_view(&view.encode_to_vec()).unwrap_err();
        assert!(format!("{err:#}").contains("remote ref state"), "{err:#}");

        // A git ref named exactly "refs/tags/" (jj's tag migration asserts
        // on the empty tag name).
        let view = proto::View {
            git_refs: vec![proto::GitRef {
                name: "refs/tags/".to_owned(),
                target: Some(proto::RefTarget {
                    value: Some(proto::ref_target::Value::Conflict(proto::RefConflict {
                        removes: vec![],
                        adds: vec![proto::ref_conflict::Term {
                            value: Some(vec![1; 20]),
                        }],
                    })),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let err = parse_view(&view.encode_to_vec()).unwrap_err();
        assert!(format!("{err:#}").contains("refs/tags/"), "{err:#}");

        // An unmigrated view with both a `refs/tags/*` git ref and tags
        // already in the "git" remote (jj's migration asserts the remote is
        // empty before moving the ref there).
        let view = proto::View {
            has_git_refs_migrated_to_remote_tags: false,
            git_refs: vec![proto::GitRef {
                name: "refs/tags/v1".to_owned(),
                target: Some(proto::RefTarget {
                    value: Some(proto::ref_target::Value::Conflict(proto::RefConflict {
                        removes: vec![],
                        adds: vec![proto::ref_conflict::Term {
                            value: Some(vec![1; 20]),
                        }],
                    })),
                }),
                ..Default::default()
            }],
            remote_views: vec![proto::RemoteView {
                name: "git".to_owned(),
                bookmarks: vec![],
                tags: vec![proto::RemoteRef {
                    name: "v1".to_owned(),
                    target_terms: vec![proto::RefTargetTerm {
                        value: Some(vec![1; 20]),
                    }],
                    state: 1,
                }],
            }],
            ..Default::default()
        };
        let err = parse_view(&view.encode_to_vec()).unwrap_err();
        assert!(format!("{err:#}").contains("migration"), "{err:#}");
    }
}

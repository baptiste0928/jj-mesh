//! Conversions between jj's operation log types and their wire mirrors.
//!
//! The wire structs in [`crate::net::sync`] mirror jj 0.43's `Operation`
//! and `View` field for field; this module owns the jj_lib side of that
//! contract. Decoding validates structure (merge arity), not content:
//! content integrity is enforced downstream by the id-preserving writes in
//! [`super::MeshRepo`].

use color_eyre::eyre::{Result, ensure};
use jj_lib::{
    backend::{CommitId, MillisSinceEpoch, Timestamp},
    merge::Merge,
    object_id::ObjectId as _,
    op_store::{
        Operation, OperationMetadata, RefTarget, RemoteRef, RemoteRefState, RemoteView,
        TimestampRange, View, ViewId,
    },
};

use crate::net::sync::{
    WireOperation, WireRefTarget, WireRemoteRef, WireRemoteView, WireTimestamp, WireView,
};

/// Encodes an operation for the wire.
pub fn encode_operation(op: &Operation) -> WireOperation {
    let meta = &op.metadata;
    WireOperation {
        view_id: op.view_id.as_bytes().to_vec(),
        parents: op.parents.iter().map(|id| id.as_bytes().to_vec()).collect(),
        start_time: encode_timestamp(meta.time.start),
        end_time: encode_timestamp(meta.time.end),
        description: meta.description.clone(),
        hostname: meta.hostname.clone(),
        username: meta.username.clone(),
        is_snapshot: meta.is_snapshot,
        workspace_name: meta
            .workspace_name
            .as_ref()
            .map(|name| name.as_str().to_owned()),
        attributes: meta
            .attributes
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        commit_predecessors: op.commit_predecessors.as_ref().map(|map| {
            map.iter()
                .map(|(commit, predecessors)| {
                    (
                        commit.as_bytes().to_vec(),
                        predecessors
                            .iter()
                            .map(|id| id.as_bytes().to_vec())
                            .collect(),
                    )
                })
                .collect()
        }),
    }
}

/// Decodes an operation received from the wire.
pub fn decode_operation(wire: WireOperation) -> Operation {
    Operation {
        view_id: ViewId::new(wire.view_id),
        parents: wire
            .parents
            .into_iter()
            .map(jj_lib::op_store::OperationId::new)
            .collect(),
        metadata: OperationMetadata {
            time: TimestampRange {
                start: decode_timestamp(wire.start_time),
                end: decode_timestamp(wire.end_time),
            },
            description: wire.description,
            hostname: wire.hostname,
            username: wire.username,
            is_snapshot: wire.is_snapshot,
            workspace_name: wire.workspace_name.map(Into::into),
            attributes: wire.attributes.into_iter().collect(),
        },
        commit_predecessors: wire.commit_predecessors.map(|pairs| {
            pairs
                .into_iter()
                .map(|(commit, predecessors)| {
                    (
                        CommitId::new(commit),
                        predecessors.into_iter().map(CommitId::new).collect(),
                    )
                })
                .collect()
        }),
    }
}

/// Encodes a view for the wire.
pub fn encode_view(view: &View) -> WireView {
    let mut head_ids: Vec<Vec<u8>> = view
        .head_ids
        .iter()
        .map(|id| id.as_bytes().to_vec())
        .collect();
    head_ids.sort_unstable();

    WireView {
        head_ids,
        local_bookmarks: view
            .local_bookmarks
            .iter()
            .map(|(name, target)| (name.as_str().to_owned(), encode_ref_target(target)))
            .collect(),
        local_tags: view
            .local_tags
            .iter()
            .map(|(name, target)| (name.as_str().to_owned(), encode_ref_target(target)))
            .collect(),
        remote_views: view
            .remote_views
            .iter()
            .map(|(name, remote)| (name.as_str().to_owned(), encode_remote_view(remote)))
            .collect(),
        git_refs: view
            .git_refs
            .iter()
            .map(|(name, target)| (name.as_str().to_owned(), encode_ref_target(target)))
            .collect(),
        git_head: encode_ref_target(&view.git_head),
        wc_commit_ids: view
            .wc_commit_ids
            .iter()
            .map(|(name, id)| (name.as_str().to_owned(), id.as_bytes().to_vec()))
            .collect(),
    }
}

/// Decodes a view received from the wire.
pub fn decode_view(wire: WireView) -> Result<View> {
    Ok(View {
        head_ids: wire.head_ids.into_iter().map(CommitId::new).collect(),
        local_bookmarks: wire
            .local_bookmarks
            .into_iter()
            .map(|(name, target)| Ok((name.into(), decode_ref_target(target)?)))
            .collect::<Result<_>>()?,
        local_tags: wire
            .local_tags
            .into_iter()
            .map(|(name, target)| Ok((name.into(), decode_ref_target(target)?)))
            .collect::<Result<_>>()?,
        remote_views: wire
            .remote_views
            .into_iter()
            .map(|(name, remote)| Ok((name.into(), decode_remote_view(remote)?)))
            .collect::<Result<_>>()?,
        git_refs: wire
            .git_refs
            .into_iter()
            .map(|(name, target)| Ok((name.into(), decode_ref_target(target)?)))
            .collect::<Result<_>>()?,
        git_head: decode_ref_target(wire.git_head)?,
        wc_commit_ids: wire
            .wc_commit_ids
            .into_iter()
            .map(|(name, id)| (name.into(), CommitId::new(id)))
            .collect(),
    })
}

fn encode_timestamp(ts: Timestamp) -> WireTimestamp {
    WireTimestamp {
        millis: ts.timestamp.0,
        tz_offset: ts.tz_offset,
    }
}

fn decode_timestamp(wire: WireTimestamp) -> Timestamp {
    Timestamp {
        timestamp: MillisSinceEpoch(wire.millis),
        tz_offset: wire.tz_offset,
    }
}

fn encode_ref_target(target: &RefTarget) -> WireRefTarget {
    target
        .as_merge()
        .iter()
        .map(|value| value.as_ref().map(|id| id.as_bytes().to_vec()))
        .collect()
}

fn decode_ref_target(wire: WireRefTarget) -> Result<RefTarget> {
    // jj's merge representation interleaves adds and removes and asserts
    // odd arity; the check keeps peer data from reaching that assert.
    ensure!(
        wire.len() % 2 == 1,
        "ref target has even merge arity ({})",
        wire.len(),
    );
    let values: Vec<Option<CommitId>> = wire
        .into_iter()
        .map(|value| value.map(CommitId::new))
        .collect();
    Ok(RefTarget::from_merge(Merge::from_vec(values)))
}

fn encode_remote_view(remote: &RemoteView) -> WireRemoteView {
    let refs = |map: &std::collections::BTreeMap<jj_lib::ref_name::RefNameBuf, RemoteRef>| {
        map.iter()
            .map(|(name, remote_ref)| {
                (
                    name.as_str().to_owned(),
                    WireRemoteRef {
                        target: encode_ref_target(&remote_ref.target),
                        tracked: remote_ref.state == RemoteRefState::Tracked,
                    },
                )
            })
            .collect()
    };
    WireRemoteView {
        bookmarks: refs(&remote.bookmarks),
        tags: refs(&remote.tags),
    }
}

fn decode_remote_view(wire: WireRemoteView) -> Result<RemoteView> {
    let refs = |pairs: Vec<(String, WireRemoteRef)>| {
        pairs
            .into_iter()
            .map(|(name, remote_ref)| {
                Ok((
                    name.into(),
                    RemoteRef {
                        target: decode_ref_target(remote_ref.target)?,
                        state: if remote_ref.tracked {
                            RemoteRefState::Tracked
                        } else {
                            RemoteRefState::New
                        },
                    },
                ))
            })
            .collect::<Result<_>>()
    };
    Ok(RemoteView {
        bookmarks: refs(wire.bookmarks)?,
        tags: refs(wire.tags)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{repo::JjRepo, tests::Fixture};

    /// Round-trips every op and view of a real repo with non-trivial
    /// content. Equality of the jj types implies identical content hashes,
    /// so a pass means replicated objects keep their ids.
    #[tokio::test]
    async fn roundtrips_real_repo_data() {
        let fx = Fixture::new();
        let dir = fx.init_repo("a");
        // Bookmarks (including a conflict-free move) and a named workspace
        // exercise the interesting view fields.
        fx.jj(&dir, &["bookmark", "create", "main", "-r", "@"]);
        fx.jj(&dir, &["new", "-m", "second"]);
        fx.jj(&dir, &["bookmark", "create", "feature", "-r", "@"]);
        fx.jj(&dir, &["describe", "-m", "amended"]);

        let repo = JjRepo::discover(&dir).unwrap().open().unwrap();
        let heads = repo.op_heads().await.unwrap();
        let ops = repo.ancestors_until(&heads, &[]).await.unwrap();
        assert!(!ops.is_empty());

        for (_, op) in &ops {
            let decoded = decode_operation(encode_operation(op));
            assert_eq!(&decoded, op);

            let view = repo.read_view(&op.view_id).await.unwrap();
            let decoded = decode_view(encode_view(&view)).unwrap();
            assert_eq!(decoded, view);
        }
    }

    #[test]
    fn rejects_even_merge_arity() {
        assert!(decode_ref_target(vec![]).is_err());
        assert!(decode_ref_target(vec![Some(vec![1; 20]), None]).is_err());
        assert!(decode_ref_target(vec![Some(vec![1; 20])]).is_ok());
    }
}

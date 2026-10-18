//! Orphan announcements: the latest announcement each peer made for repo
//! names not registered here.
//!
//! Peers replay all their repos on connect, so the [`HubState::orphans`]
//! map is how `clone` and server mode learn a repo's id and heads and who
//! serves it; [`SyncHub::clone_sources`] resolves that into
//! [`CloneSource`]s, which [`SyncHub::pull`] pulls from. The map is
//! bounded per peer and pruned on disconnect.

use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure, eyre};
use iroh::EndpointId;
use jj_lib::op_store::OperationId;
use tracing::warn;

use super::{HubState, SyncHub};
use crate::{
    config::{MAX_MESH_REPOS, RepoId},
    net::fetch::GitTransferFormat,
    repo::{
        JjRepo,
        transfer::{self, TransferPhase, TransferProgress},
    },
};

/// Budget for opening the pull stream on an established connection: a
/// peer that never grants stream credit must not stall the pull (a
/// clone's progress heartbeat would mask the hang from the CLI).
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// Deadline for the network-facing work of a pull (it may transfer an
/// entire repository); its local apply and index work runs unbounded.
const PULL_NET_TIMEOUT: Duration = Duration::from_mins(30);

/// A peer that can serve a `clone` of an unregistered repo, with the op
/// heads it claims.
#[derive(Debug)]
pub struct CloneSource {
    pub peer: EndpointId,
    pub heads: Vec<Vec<u8>>,
}

/// The latest announcement one peer made for an unregistered repo name.
#[derive(Debug, Clone)]
pub(super) struct OrphanAnnounce {
    pub(super) id: RepoId,
    pub(super) heads: Vec<Vec<u8>>,
}

impl HubState {
    /// Records `announce` as `peer`'s latest for the unregistered `name`.
    /// Refuses (returning `false`) a new name that would put the peer over
    /// [`MAX_ORPHAN_REPOS_PER_PEER`]; a known name always updates.
    pub(super) fn remember_orphan(
        &mut self,
        peer: EndpointId,
        name: String,
        announce: OrphanAnnounce,
    ) -> bool {
        let known = self
            .orphans
            .get(&name)
            .is_some_and(|peers| peers.contains_key(&peer));
        if !known && self.orphans_held_by(&peer) >= MAX_MESH_REPOS {
            return false;
        }
        self.orphans.entry(name).or_default().insert(peer, announce);
        true
    }

    /// Drops `peer`'s entry for `name` (it retracted the repo).
    pub(super) fn forget_orphan(&mut self, name: &str, peer: &EndpointId) {
        if let Some(peers) = self.orphans.get_mut(name) {
            peers.remove(peer);
            if peers.is_empty() {
                self.orphans.remove(name);
            }
        }
    }

    /// Drops every entry `peer` holds (its connection is gone).
    pub(super) fn forget_orphan_peer(&mut self, peer: &EndpointId) {
        self.orphans.retain(|_, peers| {
            peers.remove(peer);
            !peers.is_empty()
        });
    }

    /// Takes every announcement for `name`, which is being registered.
    pub(super) fn take_orphans(&mut self, name: &str) -> BTreeMap<EndpointId, OrphanAnnounce> {
        self.orphans.remove(name).unwrap_or_default()
    }

    /// How many orphan names hold an entry for `peer`, against
    /// [`MAX_ORPHAN_REPOS_PER_PEER`].
    fn orphans_held_by(&self, peer: &EndpointId) -> usize {
        self.orphans
            .values()
            .filter(|peers| peers.contains_key(peer))
            .count()
    }
}

impl SyncHub {
    /// Resolves when an orphan announcement may have been remembered since
    /// the last call: a repo may have become clonable. Wakeups coalesce.
    pub async fn orphans_changed(&self) {
        self.orphaned.notified().await;
    }

    /// Resolves a `clone` of the repo named `name`: the id every connected
    /// announcing peer agrees on, and each peer's claimed heads. Errors
    /// when nobody announces the name, or when peers disagree on the id
    /// (unrelated repos contesting one name, which only the user can
    /// resolve).
    pub fn clone_sources(&self, name: &str) -> Result<(RepoId, Vec<CloneSource>)> {
        let state = self.state.lock().unwrap();
        let sources: Vec<(EndpointId, OrphanAnnounce)> = state
            .orphans
            .get(name)
            .map(|peers| {
                peers
                    .iter()
                    .filter(|(peer, _)| state.peers.contains_key(*peer))
                    .map(|(peer, announce)| (*peer, announce.clone()))
                    .collect()
            })
            .unwrap_or_default();

        let Some(id) = sources.first().map(|(_, announce)| announce.id.clone()) else {
            bail!(
                "no connected peer announces a repo named `{name}`; check \
                 that the other machine's daemon is running and the name is \
                 correct"
            );
        };
        ensure!(
            sources.iter().all(|(_, announce)| announce.id == id),
            "peers announce different repos under the name `{name}`; \
             resolve the conflict before cloning",
        );

        Ok((
            id,
            sources
                .into_iter()
                .map(|(peer, announce)| CloneSource {
                    peer,
                    heads: announce.heads,
                })
                .collect(),
        ))
    }

    /// Pulls the full state of the mesh repo `name` into the repo at
    /// `path`, from the first of `sources` that serves it, reporting each
    /// attempt's progress with the source peer's name. Returns the pulled
    /// op and git object counts.
    pub async fn pull(
        &self,
        name: &str,
        id: &RepoId,
        sources: Vec<CloneSource>,
        path: &Path,
        progress: &(dyn Fn(&str, TransferProgress) + Send + Sync),
    ) -> Result<(u64, u64)> {
        let repo_path = path.to_owned();
        let repo = crate::spawn_blocking(move || -> Result<_> {
            Ok(Arc::new(JjRepo::at(&repo_path)?.open()?))
        })
        .await
        .wrap_err("repo open task failed")??;

        let mut last_error = eyre!("no usable source peer");
        for CloneSource { peer, heads } in sources {
            let wants: Vec<OperationId> = heads.into_iter().map(OperationId::new).collect();
            let Some(conn) = self.connection(&peer) else {
                continue;
            };

            // Reset to zeroed counters before dialing, so a fallback to
            // this source is visible.
            let peer_name = self.peer_name(&peer);
            let sink = |transfer| progress(&peer_name, transfer);
            sink(TransferProgress::start(TransferPhase::Ops));

            let pull = async {
                let (mut send, mut recv) =
                    tokio::time::timeout(OPEN_STREAM_TIMEOUT, conn.open_bi())
                        .await
                        .map_err(|_| eyre!("timed out opening the pull stream"))??;
                // A pull fetches a whole history: the pack format reuses
                // the server's on-disk deltas and lands as one pack file
                // here, instead of writing every object loose.
                let outcome = transfer::fetch(
                    &repo,
                    transfer::RepoIdent { name, id },
                    &wants,
                    transfer::FetchOptions {
                        format: GitTransferFormat::Pack,
                        net_timeout: PULL_NET_TIMEOUT,
                    },
                    &mut send,
                    &mut recv,
                    transfer::ProgressSink::new(&sink),
                )
                .await?;
                let _ = send.finish();
                Ok::<_, color_eyre::Report>(outcome)
            };
            match pull.await {
                Err(err) => last_error = err.wrap_err(format!("pull from {peer_name} failed")),
                Ok(outcome) => return Ok((outcome.ops as u64, outcome.git_objects as u64)),
            }
            warn!("pull attempt failed: {last_error:#}");
        }
        Err(last_error)
    }
}

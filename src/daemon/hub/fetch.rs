//! Fetches from peers. Repo tasks fetch the heads peers announce; `clone`
//! and server mode pull whole mesh repos from the sources
//! [`SyncHub::clone_sources`] resolves.

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context as _, Result, anyhow};
use iroh::{EndpointId, endpoint::Connection};
use jj_lib::op_store::OperationId;
use tracing::warn;

use super::SyncHub;
use crate::{
    config::RepoId,
    net::{
        fetch::{GitTransferFormat, IDLE_TIMEOUT},
        idle::Idle,
    },
    repo::{
        JjRepo, OpenRepo,
        transfer::{self, FetchOutcome, ProgressSink, TransferPhase, TransferProgress},
    },
};

/// Budget for opening a fetch stream on an established connection: a peer
/// that never grants stream credit must not stall the fetch.
const OPEN_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// A peer that can serve a pull of an unregistered repo, with the op heads
/// it claims.
#[derive(Debug)]
pub struct CloneSource {
    pub peer: EndpointId,
    pub heads: Vec<OperationId>,
}

impl SyncHub {
    /// Fetches the op heads `wants` of the repo `id` from `peer` into
    /// `repo`, over a fresh stream (see [`transfer::fetch`]).
    pub async fn fetch(
        &self,
        peer: &EndpointId,
        repo: &Arc<OpenRepo>,
        id: &RepoId,
        wants: &[OperationId],
        format: GitTransferFormat,
        progress: ProgressSink<'_>,
    ) -> Result<FetchOutcome> {
        let conn = self
            .connection(peer)
            .ok_or_else(|| anyhow!("peer is no longer connected"))?;
        let (send, recv) = tokio::time::timeout(OPEN_STREAM_TIMEOUT, conn.open_bi())
            .await
            .map_err(|_| anyhow!("timed out opening the fetch stream"))??;
        let mut send = Idle::new(send, IDLE_TIMEOUT);
        let mut recv = Idle::new(recv, IDLE_TIMEOUT);
        transfer::fetch(repo, id, wants, format, &mut send, &mut recv, progress).await
    }

    /// Pulls the full state of the mesh repo `id` into the repo at
    /// `path`, from the first of `sources` that serves it, reporting each
    /// attempt's progress with the source peer's name.
    pub async fn pull(
        &self,
        id: &RepoId,
        sources: Vec<CloneSource>,
        path: &Path,
        progress: &(dyn Fn(&str, TransferProgress) + Send + Sync),
    ) -> Result<FetchOutcome> {
        let repo_path = path.to_owned();
        let repo = crate::spawn_blocking(move || -> Result<_> {
            Ok(Arc::new(JjRepo::at(&repo_path)?.open()?))
        })
        .await
        .context("repo open task failed")??;

        let mut last_error = anyhow!("no usable source peer");
        for CloneSource { peer, heads } in sources {
            if self.connection(&peer).is_none() {
                continue;
            }
            // Reset to zeroed counters before each attempt, so a fallback
            // to this source is visible.
            let peer_name = self.peer_name(&peer);
            let sink = |transfer| progress(&peer_name, transfer);
            sink(TransferProgress::start(TransferPhase::Ops));

            // A pull fetches a whole history into an empty repo: the
            // serving peer sends its object store as it is, without
            // walking it.
            let format = GitTransferFormat::Store;
            match self
                .fetch(&peer, &repo, id, &heads, format, ProgressSink::new(&sink))
                .await
            {
                Ok(outcome) => return Ok(outcome),
                Err(err) => last_error = err.context(format!("pull from {peer_name} failed")),
            }
            warn!("pull attempt failed: {last_error:#}");
        }
        Err(last_error)
    }

    /// The live connection to a peer, for opening fetch streams.
    fn connection(&self, peer: &EndpointId) -> Option<Connection> {
        let state = self.state.lock().unwrap();
        state.peers.get(peer).map(|sender| sender.conn.clone())
    }
}

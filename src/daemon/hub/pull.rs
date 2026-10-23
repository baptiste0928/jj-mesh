//! Pulls of a whole mesh repo into a local one, for `clone` and server
//! mode, from the peers [`SyncHub::clone_sources`] resolves.

use std::{path::Path, sync::Arc, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _, eyre};
use iroh::EndpointId;
use jj_lib::op_store::OperationId;
use tracing::warn;

use super::SyncHub;
use crate::{
    config::RepoId,
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

/// A peer that can serve a pull of an unregistered repo, with the op heads
/// it claims.
#[derive(Debug)]
pub struct CloneSource {
    pub peer: EndpointId,
    pub heads: Vec<Vec<u8>>,
}

impl SyncHub {
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
                // the serving peer's on-disk deltas and lands as one pack file
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

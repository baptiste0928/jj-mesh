//! Serving inbound fetches.
//!
//! [`SyncHub::serve_fetch`] answers a peer's fetch on a detached task,
//! guarded by the repo's [`Serving`] handle. Serving is read-only and
//! dispatched straight from the hub: it must never depend on the repo
//! task's loop, which may itself be blocked fetching from the very peer
//! whose fetch we are serving.

use std::{sync::Arc, time::Duration};

use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
};
use tracing::debug;

use super::SyncHub;
use crate::{
    net::{
        fetch::{FetchRequest, MAX_OP_FRAME_SIZE, OpFrame},
        wire,
    },
    repo::{OpenRepo, transfer},
};

/// Fetches served concurrently per repo (read-only on the repo).
const MAX_SERVES: usize = 2;

/// Hard budget on serving one fetch; QUIC flow control means a stalled
/// fetcher could otherwise pin a serve task and its permit forever.
const SERVE_TIMEOUT: Duration = Duration::from_mins(30);

/// What the hub needs to serve fetches for an open repo.
#[derive(Debug, Clone)]
pub(super) struct Serving {
    repo: Arc<OpenRepo>,
    permits: Arc<tokio::sync::Semaphore>,
}

impl Serving {
    /// A serve handle for an open repo, allowing [`MAX_SERVES`] concurrent
    /// fetches.
    pub(super) fn new(repo: Arc<OpenRepo>) -> Self {
        Serving {
            repo,
            permits: Arc::new(tokio::sync::Semaphore::new(MAX_SERVES)),
        }
    }
}

impl SyncHub {
    /// Serves an inbound fetch on a detached task, or refuses it when the
    /// repo is not open here or too busy.
    pub fn serve_fetch(
        &self,
        peer: EndpointId,
        request: FetchRequest,
        mut send: SendStream,
        mut recv: RecvStream,
    ) {
        let Some(serving) = self.lookup_serving(&peer, &request) else {
            return refuse_fetch(send, "repo not available");
        };
        let Ok(permit) = serving.permits.clone().try_acquire_owned() else {
            debug!(repo = %request.name, "refusing fetch: too many being served");
            return refuse_fetch(send, "busy, retry later");
        };

        tokio::spawn(async move {
            let _permit = permit;
            let serve = transfer::serve(&serving.repo, request, &mut send, &mut recv);
            match tokio::time::timeout(SERVE_TIMEOUT, serve).await {
                Ok(Ok(())) => {
                    let _ = send.finish();
                    debug!(peer = %peer, "served fetch");
                }
                Ok(Err(err)) => debug!(peer = %peer, "serve failed: {err:#}"),
                Err(_) => debug!(peer = %peer, "serve timed out"),
            }
        });
    }

    /// Resolves the serve handle for a fetch request, logging every kind of
    /// refusal distinctly. The name needs no validation here: only names that
    /// passed it at registration are ever in the map, so an invalid one
    /// simply fails to match.
    fn lookup_serving(&self, peer: &EndpointId, request: &FetchRequest) -> Option<Serving> {
        let state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get(&request.name) else {
            debug!(repo = %request.name, "refusing fetch: repo not registered here");
            return None;
        };
        // An id mismatch means the fetcher's repo is not ours, whatever the
        // name says: refuse rather than mix unrelated histories.
        if entry.id != request.id {
            debug!(repo = %request.name, "refusing fetch: repo id mismatch");
            return None;
        }
        // Refused only for the conflicting peer, not repo-wide: it should
        // not be fetching while paused itself, but an older or hostile
        // build must still be denied the ops that feed the colocation
        // ping-pong. Everyone else is served normally.
        if entry.local_colocated && entry.colocated_peers.contains(peer) {
            debug!(repo = %request.name, "refusing fetch: colocation conflict with this peer");
            return None;
        }

        let serving = entry.serving.clone();
        if serving.is_none() {
            debug!(repo = %request.name, "refusing fetch: repo not open");
        }
        serving
    }
}

/// Refuses a fetch on a detached task, telling the peer why before closing.
fn refuse_fetch(mut send: SendStream, message: &'static str) {
    tokio::spawn(async move {
        let frame = OpFrame::Error {
            message: message.to_owned(),
        };
        let _ = wire::write_message(&mut send, &frame, MAX_OP_FRAME_SIZE).await;
        let _ = send.finish();
    });
}

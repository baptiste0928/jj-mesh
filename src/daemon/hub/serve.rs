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
    net::fetch::FetchRequest,
    repo::{OpenRepo, transfer},
};

/// Fetches served concurrently per repo (read-only on the repo).
const MAX_SERVES: usize = 2;

/// How long a fetch waits for a busy repo before being refused: with more
/// than [`MAX_SERVES`] peers syncing one change at once, queueing briefly
/// beats sending the surplus back to their retry timer.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

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

    /// Refuses the fetches still queued on this handle.
    pub(super) fn close(&self) {
        self.permits.close();
    }
}

impl SyncHub {
    /// Serves an inbound fetch on a detached task, or refuses it when the
    /// repo is not open here or stays too busy.
    pub fn serve_fetch(
        &self,
        peer: EndpointId,
        request: FetchRequest,
        mut send: SendStream,
        mut recv: RecvStream,
    ) {
        let serving = self.lookup_serving(&request);
        let peer = self.peer_name(&peer);
        tokio::spawn(async move {
            let Some(serving) = serving else {
                return refuse_and_close(send, "repo not available").await;
            };
            let queued = tokio::time::timeout(QUEUE_TIMEOUT, serving.permits.acquire_owned());
            let Ok(Ok(_permit)) = queued.await else {
                debug!(repo = %request.id, "refusing fetch: too many being served");
                return refuse_and_close(send, "busy, retry later").await;
            };

            let serve = transfer::serve(&serving.repo, request, &mut send, &mut recv);
            match tokio::time::timeout(SERVE_TIMEOUT, serve).await {
                Ok(Ok(())) => {
                    let _ = send.finish();
                    debug!(%peer, "served fetch");
                }
                Ok(Err(err)) => debug!(%peer, "serve failed: {err:#}"),
                Err(_) => debug!(%peer, "serve timed out"),
            }
        });
    }

    /// Resolves the serve handle for a fetch request, logging every kind of
    /// refusal distinctly.
    fn lookup_serving(&self, request: &FetchRequest) -> Option<Serving> {
        let state = self.state.lock().unwrap();
        let Some(entry) = state.repos.get(&request.id) else {
            debug!(repo = %request.id, "refusing fetch: repo not registered here");
            return None;
        };
        let serving = entry.serving.clone();
        if serving.is_none() {
            debug!(repo = %request.id, "refusing fetch: repo not open");
        }
        serving
    }
}

/// Refuses a fetch, telling the peer why before closing.
async fn refuse_and_close(mut send: SendStream, message: &str) {
    let _ = transfer::refuse(&mut send, message.to_owned()).await;
    let _ = send.finish();
}

//! Per-peer outbound message queue and the sender task draining it.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use iroh::endpoint::Connection;
use tokio::sync::Notify;
use tracing::debug;

use crate::{
    config::{Membership, RepoId},
    net::sync::{self, Announce, StatusReport, UniMessage},
};

/// Pending messages for one peer, coalesced latest-wins: one slot for the
/// membership, one for the status report and one per repo for
/// announcements, each kept until the sender task takes it.
#[derive(Debug, Default)]
pub(super) struct Outbox {
    pending: Mutex<OutboxState>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct OutboxState {
    membership: Option<Membership>,
    status: Option<StatusReport>,
    announces: BTreeMap<RepoId, Announce>,
}

impl Outbox {
    /// Queues a message, replacing any pending one it supersedes.
    pub(super) fn push(&self, message: UniMessage) {
        let mut pending = self.pending.lock().unwrap();
        match message {
            UniMessage::Announce(announce) => {
                pending.announces.insert(announce.id.clone(), announce);
            }
            UniMessage::Membership(membership) => pending.membership = Some(membership),
            UniMessage::Status(report) => pending.status = Some(report),
        }
        drop(pending);
        self.notify.notify_one();
    }

    /// Takes the next message, membership first (a new peer should learn
    /// the mesh before anything else), then the status report, then the
    /// repo announcements.
    pub(super) fn pop(&self) -> Option<UniMessage> {
        let mut pending = self.pending.lock().unwrap();
        if let Some(membership) = pending.membership.take() {
            return Some(UniMessage::Membership(membership));
        }
        if let Some(report) = pending.status.take() {
            return Some(UniMessage::Status(report));
        }
        pending
            .announces
            .pop_first()
            .map(|(_, announce)| UniMessage::Announce(announce))
    }
}

/// Sends a peer's outbox until its connection fails; messages lost with
/// the connection are recovered by the reconnect replay.
pub(super) async fn run_sender(conn: Connection, outbox: Arc<Outbox>) {
    loop {
        let Some(message) = outbox.pop() else {
            outbox.notify.notified().await;
            continue;
        };
        if let Err(err) = sync::send_uni(&conn, &message).await {
            debug!("send failed: {err:#}");
            return conn.close(0u32.into(), b"send failed");
        }
    }
}

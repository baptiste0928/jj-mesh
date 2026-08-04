//! Per-peer outbound message queue and the sender task draining it.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::endpoint::Connection;
use tokio::sync::Notify;
use tracing::debug;

use crate::{
    config::Membership,
    net::sync::{self, Announce, StatusReport, UniMessage},
};

/// Budget for sending one message; a stalled peer connection kills its
/// sender task (the reconnect replay recovers the state).
const SEND_TIMEOUT: Duration = Duration::from_secs(10);

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
    announces: BTreeMap<String, Announce>,
}

impl Outbox {
    pub(super) fn push_announce(&self, announce: Announce) {
        self.pending
            .lock()
            .unwrap()
            .announces
            .insert(announce.name.clone(), announce);
        self.notify.notify_one();
    }

    pub(super) fn push_membership(&self, membership: Membership) {
        self.pending.lock().unwrap().membership = Some(membership);
        self.notify.notify_one();
    }

    pub(super) fn push_status(&self, report: StatusReport) {
        self.pending.lock().unwrap().status = Some(report);
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
        match tokio::time::timeout(SEND_TIMEOUT, sync::send_uni(&conn, &message)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return debug!("send failed: {err:#}"),
            Err(_) => return debug!("send timed out"),
        }
    }
}

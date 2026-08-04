//! Daemon-side pairing.
//!
//! The daemon owns the machine-key endpoint, so all pairing runs through
//! it, driven by control-socket requests. The pairing ALPN is always
//! served; what gates pairing is the *ticket*: hosting issues a one-time
//! ticket valid for [`PAIR_TICKET_TTL`], at most one is valid at a time
//! (hosting again revokes the previous one), and redeeming it is atomic:
//! the ticket is consumed under its lock before the peer is registered, so
//! a revoked, expired or already-used ticket can never pair.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use color_eyre::eyre::{Result, eyre};
use iroh::{Endpoint, endpoint::Connection};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use super::{control::PAIR_TICKET_TTL, store::MeshStore};
use crate::net::pair;

/// How long to wait for a relay connection when issuing a ticket.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);

/// Time budget for one connection to complete the whole exchange.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(15);

/// Concurrent inbound exchanges; the pairing ALPN accepts connections from
/// unknown endpoints, so surplus is shed rather than queued.
const MAX_EXCHANGES: usize = 4;

/// Pairing state of the daemon: the outstanding ticket, if any.
#[derive(Debug)]
pub struct Pairing {
    endpoint: Endpoint,
    /// Whether the endpoint uses relays; without them (hermetic tests)
    /// there is no relay connection to wait for when issuing a ticket.
    uses_relays: bool,
    store: Arc<MeshStore>,
    issued: Mutex<Option<IssuedTicket>>,
    exchanges: Semaphore,
}

/// A ticket handed to the user, redeemable until revoked or expired.
#[derive(Debug)]
struct IssuedTicket {
    ticket: pair::PairTicket,
    expires: Instant,
    /// Name this machine announces to the redeeming joiner.
    local_name: String,
}

impl Pairing {
    pub fn new(endpoint: Endpoint, uses_relays: bool, store: Arc<MeshStore>) -> Self {
        Pairing {
            endpoint,
            uses_relays,
            store,
            issued: Mutex::new(None),
            exchanges: Semaphore::new(MAX_EXCHANGES),
        }
    }

    /// Issues a fresh one-time ticket, revoking any outstanding one.
    pub async fn host(&self, local_name: String) -> Result<pair::PairTicket> {
        // Revoke before the relay wait below: a user re-hosting to kill a
        // leaked ticket must not depend on the relay being reachable, and
        // a failed reissue must fail closed.
        if self.issued.lock().unwrap().take().is_some() {
            info!("pairing ticket revoked");
        }

        // Wait for a relay connection so the ticket contains a usable relay
        // address even before hole punching is possible. Without relays
        // `online()` would never resolve, and there is nothing to wait for.
        if self.uses_relays {
            tokio::time::timeout(ONLINE_TIMEOUT, self.endpoint.online())
                .await
                .map_err(|_| eyre!("cannot reach an iroh relay, check network connectivity"))?;
        }

        let ticket = pair::PairTicket::generate(self.endpoint.addr());
        *self.issued.lock().unwrap() = Some(IssuedTicket {
            ticket: ticket.clone(),
            expires: Instant::now() + PAIR_TICKET_TTL,
            local_name,
        });

        info!("pairing ticket issued");
        Ok(ticket)
    }

    /// Serves one accepted pair-ALPN connection: refused outright unless a
    /// ticket is outstanding, otherwise one exchange attempt runs against
    /// it. A failed attempt leaves the ticket valid (the joiner was told
    /// why and can retry); only a successful one redeems it.
    pub async fn serve_inbound(&self, conn: Connection) {
        let Ok(_permit) = self.exchanges.try_acquire() else {
            debug!("dropping surplus pairing connection");
            conn.close(0u32.into(), b"busy");
            return;
        };

        let Some((ticket, local_name)) = self.issued_ticket() else {
            debug!("refusing pairing connection: no valid ticket");
            pair::reject_attempt(&conn, "no pairing in progress on this machine").await;
            return;
        };

        let snapshot = self.store.snapshot();
        let exchange = pair::pair_with(&conn, &ticket, &local_name, &snapshot);
        let peer = match tokio::time::timeout(EXCHANGE_TIMEOUT, exchange).await {
            Ok(Ok(Some(peer))) => peer,
            // Invalid or interrupted attempt; nothing to do.
            Ok(Ok(None)) => return,
            // The attempt failed after proving ticket possession (e.g. an
            // unacceptable name): only the ticket holder can trigger this,
            // and it was sent the reason, so let it retry.
            Ok(Err(err)) => {
                warn!("pairing attempt failed: {err:#}");
                return;
            }
            Err(_timeout) => {
                conn.close(0u32.into(), b"timeout");
                return;
            }
        };

        // Consuming the ticket and saving the peer happen under the same
        // lock, so a ticket revoked or replaced mid-exchange can never
        // register a peer, and a ticket is redeemed at most once.
        let saved = {
            let mut issued = self.issued.lock().unwrap();
            let live = issued
                .as_ref()
                .is_some_and(|i| i.ticket.matches(&ticket) && Instant::now() < i.expires);
            if !live {
                warn!("pairing attempt discarded: the ticket is no longer valid");
                conn.close(0u32.into(), b"cancelled");
                return;
            }
            *issued = None;
            self.store.add_paired_peer(&peer)
        };
        match saved {
            Ok(()) => {
                pair::confirm_paired(&conn);
                info!(peer = %peer.name, "paired");
            }
            Err(err) => {
                conn.close(0u32.into(), b"failed");
                warn!("pairing failed: cannot save the peer: {err:#}");
            }
        }
    }

    /// The outstanding ticket and announced name, dropping the ticket when
    /// it turns out expired.
    fn issued_ticket(&self) -> Option<(pair::PairTicket, String)> {
        let mut issued = self.issued.lock().unwrap();
        if issued.as_ref().is_some_and(|i| i.expires <= Instant::now()) {
            *issued = None;
            info!("pairing ticket expired");
        }
        issued
            .as_ref()
            .map(|i| (i.ticket.clone(), i.local_name.clone()))
    }
}

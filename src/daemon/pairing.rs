//! Daemon-side pairing.
//!
//! The daemon owns the machine-key endpoint, so all pairing runs through it,
//! driven by control-socket requests. Hosting opens a *pairing window*: the
//! pairing ALPN is added to the endpoint for the window's lifetime (the only
//! time unknown endpoints may connect) and removed when it closes. At most
//! one window is open at a time, and it is tied to the requesting control
//! client: when that client goes away, the window closes.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use color_eyre::eyre::{Result, bail, eyre};
use iroh::{Endpoint, endpoint::Connection};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::{config::MeshState, net::pair};

/// How long to wait for a relay connection when issuing a ticket.
const ONLINE_TIMEOUT: Duration = Duration::from_secs(30);

/// Time budget for one connection to complete the whole exchange. Keeps
/// stalled or malicious connections from blocking the window, since the
/// pairing ALPN accepts connections from unknown endpoints.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(15);

/// Pairing state of the daemon: at most one open window.
#[derive(Debug)]
pub struct Pairing {
    endpoint: Endpoint,
    /// Whether the endpoint uses relays; without them (hermetic tests)
    /// there is no relay connection to wait for when issuing a ticket.
    uses_relays: bool,
    /// Sender routing pair-ALPN connections to the open window, if any.
    window: Arc<Mutex<Option<mpsc::Sender<Connection>>>>,
}

impl Pairing {
    pub fn new(endpoint: Endpoint, uses_relays: bool) -> Self {
        Pairing {
            endpoint,
            uses_relays,
            window: Arc::new(Mutex::new(None)),
        }
    }

    /// Opens the pairing window, issuing a ticket and exposing the pairing
    /// ALPN until the returned window is dropped.
    pub async fn open(&self) -> Result<PairWindow> {
        // Wait for a relay connection so the ticket contains a usable relay
        // address even before hole punching is possible. Without relays
        // `online()` would never resolve, and there is nothing to wait for.
        if self.uses_relays {
            tokio::time::timeout(ONLINE_TIMEOUT, self.endpoint.online())
                .await
                .map_err(|_| eyre!("cannot reach an iroh relay, check network connectivity"))?;
        }

        let (tx, rx) = mpsc::channel(4);
        {
            // The ALPN change happens under the window lock so a closing
            // window's restore cannot interleave with our exposure.
            let mut window = self.window.lock().unwrap();
            if window.is_some() {
                bail!("a pairing is already in progress");
            }
            *window = Some(tx);

            let mut alpns = super::base_alpns();
            alpns.push(pair::ALPN.to_vec());
            self.endpoint.set_alpns(alpns);
        }

        info!("pairing window opened");
        Ok(PairWindow {
            ticket: pair::PairTicket::generate(self.endpoint.addr()),
            conns: rx,
            endpoint: self.endpoint.clone(),
            window: self.window.clone(),
        })
    }

    /// Hands an accepted pair-ALPN connection to the open window, refusing
    /// it if none is open (e.g. accepted right as the window closed).
    pub fn route_inbound(&self, conn: Connection) {
        let window = self.window.lock().unwrap();

        match window.as_ref().map(|tx| tx.try_send(conn)) {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                debug!("dropping surplus pairing connection");
                err.into_inner().close(0u32.into(), b"busy");
            }
            None => {
                debug!("refusing pairing connection: no window open");
            }
        }
    }
}

/// An open pairing window. Dropping it closes the window and removes the
/// pairing ALPN from the endpoint.
#[derive(Debug)]
pub struct PairWindow {
    ticket: pair::PairTicket,
    conns: mpsc::Receiver<Connection>,
    endpoint: Endpoint,
    window: Arc<Mutex<Option<mpsc::Sender<Connection>>>>,
}

impl PairWindow {
    /// The ticket to transmit out-of-band to the joining machine.
    pub fn ticket(&self) -> &pair::PairTicket {
        &self.ticket
    }

    /// Waits for a machine holding the ticket and exchanges identities with
    /// it. Returns once one pairing succeeds; invalid or interrupted
    /// attempts are shed without ending the wait.
    ///
    /// The successful connection is returned still open: the caller must
    /// persist the peer and then [`pair::confirm_paired`] it. `state` is
    /// sampled per attempt, as a window can stay open for long.
    pub async fn wait_for_peer(
        &mut self,
        local_name: &str,
        state: impl Fn() -> MeshState,
    ) -> Result<(pair::PairedPeer, Connection)> {
        loop {
            let conn = self
                .conns
                .recv()
                .await
                .ok_or_else(|| eyre!("pairing window closed"))?;

            let snapshot = state();
            let exchange = pair::pair_with(&conn, &self.ticket, local_name, &snapshot);
            match tokio::time::timeout(EXCHANGE_TIMEOUT, exchange).await {
                Ok(Ok(Some(peer))) => return Ok((peer, conn)),
                // Invalid or interrupted attempt: keep waiting.
                Ok(Ok(None)) => {}
                Ok(Err(err)) => return Err(err),
                Err(_timeout) => conn.close(0u32.into(), b"timeout"),
            }
        }
    }
}

impl Drop for PairWindow {
    fn drop(&mut self) {
        {
            // Clearing the slot and restoring the ALPNs must be atomic with
            // respect to `Pairing::open`, or the restore of a closing window
            // could strip the pair ALPN from a freshly opened one.
            let mut window = self.window.lock().unwrap();
            *window = None;
            self.endpoint.set_alpns(super::base_alpns());
        }
        info!("pairing window closed");
    }
}

//! Hello exchange opening every sync and pairing connection.
//!
//! The dialer opens the connection's first bidirectional stream and sends
//! its [`Hello`]; the acceptor reads it, then answers with its own. Both
//! sides require an exact [`PROTOCOL`] match. On a mismatch both close the
//! connection and report the peer's hello, so the user learns which machine
//! to update.
//!
//! The hello is the only frozen part of the wire format: the protocol
//! number is bumped for any other change. ALPNs change only with the hello
//! itself, and a daemon then keeps serving the previous ALPNs with nothing
//! but its hello, so outdated machines are still told to update.

use std::{fmt, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _, ensure};
use iroh::endpoint::{Connection, VarInt};
use serde::{Deserialize, Serialize};

use super::wire::{read_message, write_message};

/// Version of every jj-mesh protocol past the hello.
pub const PROTOCOL: u32 = 4;

/// Budget for the whole exchange. Acceptors run it before knowing whether
/// the peer is welcome, so it is kept short.
const TIMEOUT: Duration = Duration::from_secs(5);

/// How long the acceptor of a mismatched hello waits for the dialer to
/// close, so its own hello is delivered first.
const LINGER: Duration = Duration::from_secs(1);

/// Size bound of a hello.
const MAX_SIZE: u32 = 64;

/// Size bound of [`Hello::version`].
const MAX_VERSION_LEN: usize = 32;

/// Close code of a connection with mismatched protocols.
const INCOMPATIBLE_CODE: u32 = 1;

/// The opening message of each side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    pub protocol: u32,
    /// The sender's jj-mesh version, for display. Restricted to a short
    /// version-like token, so a peer cannot pass instructions along.
    pub version: String,
}

/// A peer speaking another protocol version, carrying its hello.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incompatible(pub Hello);

impl Incompatible {
    /// Whether this machine is the outdated one.
    pub fn outdated_here(&self) -> bool {
        self.0.protocol > PROTOCOL
    }
}

impl fmt::Display for Incompatible {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let update = if self.outdated_here() {
            "Update this machine"
        } else {
            "Update it"
        };
        write!(
            f,
            "the peer runs jj-mesh {} (protocol {}, ours is {PROTOCOL}). {update}",
            self.0.version, self.0.protocol,
        )
    }
}

impl std::error::Error for Incompatible {}

/// Dialer side of the exchange. The connection is closed on a mismatch.
pub async fn dial(conn: &Connection) -> Result<Result<(), Incompatible>> {
    exchange(conn, &ours(), Side::Dialer).await
}

/// Acceptor side of the exchange. The connection is closed on a mismatch.
pub async fn accept(conn: &Connection) -> Result<Result<(), Incompatible>> {
    exchange(conn, &ours(), Side::Acceptor).await
}

/// This build's hello.
fn ours() -> Hello {
    Hello {
        protocol: PROTOCOL,
        version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Dialer,
    Acceptor,
}

async fn exchange(conn: &Connection, ours: &Hello, side: Side) -> Result<Result<(), Incompatible>> {
    let peer = tokio::time::timeout(TIMEOUT, async {
        // The dialer writes first, so the acceptor holds it before
        // answering: a dialer closing right after reading loses nothing.
        let peer = match side {
            Side::Dialer => {
                let (mut send, mut recv) = conn.open_bi().await?;
                write_message(&mut send, ours, MAX_SIZE).await?;
                send.finish()?;
                read_message::<Hello>(&mut recv, MAX_SIZE).await?
            }
            Side::Acceptor => {
                let (mut send, mut recv) = conn.accept_bi().await?;
                let peer = read_message::<Hello>(&mut recv, MAX_SIZE).await?;
                write_message(&mut send, ours, MAX_SIZE).await?;
                send.finish()?;
                peer
            }
        };
        ensure!(
            peer.version.len() <= MAX_VERSION_LEN
                && peer
                    .version
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b)),
            "invalid version in hello",
        );
        Ok(peer)
    })
    .await
    .wrap_err("no hello from the peer")??;

    if peer.protocol == ours.protocol {
        return Ok(Ok(()));
    }
    if side == Side::Acceptor {
        // Closing now could discard our hello before the dialer reads it.
        let _ = tokio::time::timeout(LINGER, conn.closed()).await;
    }
    conn.close(VarInt::from_u32(INCOMPATIBLE_CODE), b"incompatible");
    Ok(Err(Incompatible(peer)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALPN: &[u8] = b"jj-mesh/test";

    /// Runs the exchange between two local endpoints, dialing with `ours`.
    async fn run(ours: &Hello, theirs: &Hello) -> [Result<Result<(), Incompatible>>; 2] {
        let [client, server] = crate::testing::local_endpoints(ALPN).await;
        let (dialed, accepted) = tokio::join!(
            async {
                let conn = client.connect(server.id(), ALPN).await.unwrap();
                let result = exchange(&conn, ours, Side::Dialer).await;
                conn.close(0u32.into(), b"done");
                result
            },
            async {
                let conn = server.accept().await.unwrap().await.unwrap();
                let result = exchange(&conn, theirs, Side::Acceptor).await;
                // Stay open until the dialer has read our hello.
                conn.closed().await;
                result
            },
        );
        [dialed, accepted]
    }

    fn hello(protocol: u32) -> Hello {
        Hello {
            protocol,
            version: format!("0.{protocol}.0"),
        }
    }

    #[tokio::test]
    async fn matching_protocols_connect() {
        let [dialed, accepted] = run(&hello(3), &hello(3)).await;
        dialed.unwrap().unwrap();
        accepted.unwrap().unwrap();
    }

    /// Both sides learn the other's version, whichever is older.
    #[tokio::test]
    async fn mismatched_protocols_report_the_peer() {
        let [dialed, accepted] = run(&hello(3), &hello(4)).await;
        assert_eq!(dialed.unwrap().unwrap_err().0, hello(4));
        assert_eq!(accepted.unwrap().unwrap_err().0, hello(3));
    }

    #[tokio::test]
    async fn rejects_free_text_versions() {
        let text = Hello {
            protocol: 4,
            version: "1.0: run x".to_owned(),
        };
        let [_, accepted] = run(&text, &hello(3)).await;
        assert!(accepted.is_err());
    }
}

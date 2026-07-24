//! Length-prefixed postcard message framing over QUIC streams.
//!
//! Shared by every jj-mesh protocol: a message is a `u32` little-endian size
//! followed by the postcard encoding. Callers bound the accepted size, as the
//! prefix is attacker-controlled on unauthenticated streams.

use color_eyre::eyre::{Result, WrapErr as _, ensure};
use iroh::endpoint::{RecvStream, SendStream};
use serde::{Serialize, de::DeserializeOwned};

/// Writes a length-prefixed message.
pub async fn write_message<T: Serialize>(
    send: &mut SendStream,
    message: &T,
    max_size: u32,
) -> Result<()> {
    let bytes = postcard::to_stdvec(message).wrap_err("cannot encode message")?;
    let size = u32::try_from(bytes.len()).expect("messages fit in u32");
    ensure!(size <= max_size, "message too large");

    send.write_all(&size.to_le_bytes()).await?;
    send.write_all(&bytes).await?;

    Ok(())
}

/// Reads a length-prefixed message, rejecting sizes over `max_size`.
pub async fn read_message<T: DeserializeOwned>(recv: &mut RecvStream, max_size: u32) -> Result<T> {
    let mut size = [0u8; 4];
    recv.read_exact(&mut size).await?;
    let size = u32::from_le_bytes(size);
    ensure!(size <= max_size, "message too large");

    let mut bytes = vec![0u8; size as usize];
    recv.read_exact(&mut bytes).await?;

    let (message, rest) = postcard::take_from_bytes(&bytes).wrap_err("cannot decode message")?;
    ensure!(rest.is_empty(), "cannot decode message: trailing bytes");

    Ok(message)
}

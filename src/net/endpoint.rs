//! Iroh endpoint construction.

use color_eyre::eyre::Result;
use iroh::{Endpoint, endpoint::presets};

use crate::config::MachineKey;

/// Binds the iroh endpoint with this machine's identity key.
///
/// Uses the n0 defaults: public relays with pkarr/DNS discovery. `alpns` lists
/// the protocols accepted for incoming connections (empty for dial-only use).
pub async fn bind_endpoint(key: &MachineKey, alpns: Vec<Vec<u8>>) -> Result<Endpoint> {
    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(key.secret().clone())
        .alpns(alpns)
        .bind()
        .await?;

    Ok(endpoint)
}

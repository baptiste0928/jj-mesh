//! Iroh endpoint construction.

use color_eyre::eyre::Result;
use iroh::{Endpoint, endpoint::presets};

use crate::config::MachineKey;

/// How the iroh endpoint reaches the network.
#[derive(Debug, Clone, Default)]
pub enum EndpointOptions {
    /// The n0 defaults: public relays with pkarr/DNS discovery.
    #[default]
    Production,
    /// Hermetic mode for tests: no relays, no external discovery. The bound
    /// endpoint registers its own address in `lookup` and resolves peers
    /// through it, so endpoints sharing one lookup can dial each other by
    /// id without leaving localhost.
    #[cfg(feature = "test-util")]
    LocalTest {
        lookup: iroh::address_lookup::MemoryLookup,
    },
}

impl EndpointOptions {
    /// Whether endpoints bound with these options use iroh relays.
    pub fn uses_relays(&self) -> bool {
        match self {
            EndpointOptions::Production => true,
            #[cfg(feature = "test-util")]
            EndpointOptions::LocalTest { .. } => false,
        }
    }
}

/// Binds the iroh endpoint with this machine's identity key. `alpns` lists
/// the protocols accepted for incoming connections (empty for dial-only
/// use).
pub async fn bind_endpoint(
    key: &MachineKey,
    alpns: Vec<Vec<u8>>,
    options: &EndpointOptions,
) -> Result<Endpoint> {
    match options {
        EndpointOptions::Production => {
            let endpoint = Endpoint::builder(presets::N0)
                .secret_key(key.secret().clone())
                .alpns(alpns)
                .bind()
                .await?;
            Ok(endpoint)
        }
        #[cfg(feature = "test-util")]
        EndpointOptions::LocalTest { lookup } => {
            let endpoint = Endpoint::builder(presets::Minimal)
                .relay_mode(iroh::RelayMode::Disabled)
                .secret_key(key.secret().clone())
                .alpns(alpns)
                .bind()
                .await?;
            endpoint
                .address_lookup()
                .expect("endpoint was just bound")
                .add(lookup.clone());
            lookup.add_endpoint_info(endpoint.addr());
            Ok(endpoint)
        }
    }
}

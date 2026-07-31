//! Peer-to-peer networking over iroh.
//!
//! Hosts the endpoint construction shared by the CLI and the daemon, the
//! message framing, and the pairing and sync protocols.

mod endpoint;
pub(crate) mod pair;
pub(crate) mod sync;
pub(crate) mod wire;

pub use endpoint::EndpointOptions;
pub(crate) use endpoint::bind_endpoint;

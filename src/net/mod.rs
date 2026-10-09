//! Peer-to-peer networking over iroh.

mod endpoint;
pub(crate) mod fetch;
pub(crate) mod pair;
pub(crate) mod sync;
pub(crate) mod wire;

pub use endpoint::EndpointOptions;
pub(crate) use endpoint::bind_endpoint;

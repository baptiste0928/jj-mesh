//! Peer-to-peer networking over iroh.
//!
//! Hosts the endpoint construction shared by the CLI and the daemon, and the
//! pairing protocol. The sync protocol (Bunshin) will also live here.

mod endpoint;
pub mod pair;
pub mod wire;

pub use endpoint::bind_endpoint;

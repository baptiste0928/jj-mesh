//! Peer-to-peer networking over iroh.
//!
//! Hosts the endpoint construction shared by the CLI and the daemon, and the
//! pairing protocol. The sync protocol will also live here.

mod endpoint;
pub mod pair;
pub mod sync;
pub mod wire;

pub use endpoint::{EndpointOptions, bind_endpoint};

//! Peer-to-peer networking over iroh.
//!
//! Any change to the sync or pairing wire format must bump
//! `hello::PROTOCOL`, so mismatched machines are told which one to update
//! instead of mis-decoding each other.

mod endpoint;
pub(crate) mod fetch;
pub(crate) mod hello;
pub(crate) mod pair;
pub(crate) mod sync;
pub(crate) mod wire;

pub use endpoint::EndpointOptions;
pub(crate) use endpoint::bind_endpoint;

//! Daemon control socket.
//!
//! The CLI talks to the running daemon over a unix socket with
//! length-prefixed postcard messages (see [`crate::net::wire`]). Every
//! request is one request/response exchange; joining a pairing merely keeps
//! its connection open so the daemon notices a cancelling client.
//!
//! The daemon is the only holder of the machine-key endpoint and the only
//! writer of the mesh state, so live peer state, pairing and every
//! user-driven mesh mutation go through here. Gossip-driven mutations
//! reach the same store from the daemon's membership loop.
//!
//! Split in three: [`protocol`] is the request/response vocabulary the CLI
//! and daemon share, [`server`] serves it, and [`client`] is what the CLI
//! dials. The CLI depends on `protocol` and `client`, never on `server`.

mod client;
mod protocol;
mod server;

pub use client::{ControlClient, query_status_blocking, request_blocking};
pub use protocol::*;
pub use server::{ControlContext, ControlServer};

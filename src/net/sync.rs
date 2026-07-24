//! Sync protocol.
//!
//! Only the ALPN for now: the daemon uses it for its persistent peer
//! connections, and head announcements, op negotiation and git object
//! transfer will be multiplexed over these connections next.

/// ALPN of the sync protocol.
pub const ALPN: &[u8] = b"jj-mesh/sync/0";

//! The op and git object transfer engine: serving fetches and fetching.
//!
//! Both sides run over any `AsyncRead`/`AsyncWrite` pair (QUIC streams in
//! production, in-memory duplexes in tests) and exchange the frame types
//! defined in [`crate::net::sync`]:
//!
//! ```text
//! fetcher                                     server
//!    |  FetchRequest { wants, haves }           |
//!    |----------------------------------------->|
//!    |  OpFrame: Begin, (View | Op)*, Done      |
//!    |<-----------------------------------------|
//!    |  GitRequest { wants, haves, format }     |
//!    |----------------------------------------->|
//!    |  GitFrame: (Object* | Pack chunk*), Done |
//!    |<-----------------------------------------|
//!    |  apply + publish (local only)            |
//! ```
//!
//! Ops and views travel as raw stored bytes and keep their sender-side ids
//! (see the `mesh` module docs for why re-hashing them is impossible).
//! Peer-supplied data is authenticated but untrusted: op and view bytes are
//! validated structurally before anything is written, git objects are
//! hash-verified before writing (loose frames against their claimed id,
//! packed objects while indexing the pack), and replicated bytes can never
//! replace already-stored objects (loose writes skip existing ids; pack ids
//! are content hashes).
//!
//! The engine splits along its two sides and the apply step:
//! - [`serve`] answers a fetch (read-only): the op-log delta, then the git
//!   object closure the fetcher lacks, loose or as one packfile (see
//!   [`crate::net::sync::GitTransferFormat`]).
//! - [`fetch`] pulls and validates that delta, then hands it to [`apply`],
//!   which writes it in the crash-safe order: git objects, anti-GC keep
//!   refs, views and ops (parents first), change-id extras, the colocated
//!   ref mirror, and only then the op head publication.
//!
//! Bulk store and git work runs on blocking threads (see the `mesh` module
//! docs).

mod apply;
mod fetch;
mod pack;
mod serve;
#[cfg(test)]
mod tests;

use std::collections::HashMap;

use color_eyre::eyre::{Result, eyre};
use jj_lib::{
    backend::CommitId,
    object_id::ObjectId as _,
    op_store::{OperationId, ViewId},
};

pub use fetch::fetch;
pub use serve::serve;

use super::codec::{OpMeta, ViewMeta};

/// What a completed fetch did, for logging and status.
#[derive(Debug)]
pub struct FetchOutcome {
    /// Op heads published locally (empty when already up to date). The
    /// daemon only logs the counts; the tests assert on it.
    #[cfg_attr(not(test), expect(dead_code))]
    pub published: Vec<OperationId>,
    pub ops: usize,
    pub git_objects: usize,
}

/// A snapshot of an in-flight fetch, for progress display. Serialized as
/// part of the control protocol (see `crate::daemon::control`); the unit
/// of `current`/`total` is phase-defined so a change of transfer
/// representation only renumbers, never reshapes, it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransferProgress {
    pub phase: TransferPhase,
    /// Progress in the phase's unit: op and view frames in [`Ops`]; in
    /// [`Git`], objects indexed from the pack (pack format) or objects
    /// received (loose format). Unused in [`Apply`].
    ///
    /// [`Ops`]: TransferPhase::Ops
    /// [`Git`]: TransferPhase::Git
    /// [`Apply`]: TransferPhase::Apply
    pub current: u64,
    /// Exact end of the phase, `None` until known: announced by the peer
    /// for the op phase, read from the pack header for the git phase
    /// (never known in the loose format).
    pub total: Option<u64>,
    /// Wire (compressed) payload bytes received so far in the phase.
    pub bytes: u64,
}

impl TransferProgress {
    /// Zeroed counters at the start of `phase`.
    pub fn start(phase: TransferPhase) -> Self {
        TransferProgress {
            phase,
            current: 0,
            total: None,
            bytes: 0,
        }
    }
}

/// The sequential stages of a fetch, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransferPhase {
    /// Pulling the op-log delta.
    Ops,
    /// Pulling the git objects the new views reference.
    Git,
    /// Writing everything into the local repo (and indexing it after a
    /// clone, which can rival the transfer for large histories).
    Apply,
}

/// Sink for [`TransferProgress`] updates, called inline from the transfer
/// loops: implementations must be cheap and never block. The default sink
/// discards everything.
#[derive(Clone, Copy, Default)]
pub struct ProgressSink<'a>(Option<&'a (dyn Fn(TransferProgress) + Send + Sync)>);

impl<'a> ProgressSink<'a> {
    pub fn new(sink: &'a (dyn Fn(TransferProgress) + Send + Sync)) -> Self {
        ProgressSink(Some(sink))
    }

    /// Reports one snapshot.
    fn report(self, progress: TransferProgress) {
        if let Some(sink) = self.0 {
            sink(progress);
        }
    }
}

/// A replicated operation: raw bytes to store under `id`, plus the
/// structural metadata extracted from them.
struct StoredOp {
    id: OperationId,
    bytes: Vec<u8>,
    meta: OpMeta,
}

/// A replicated view (see [`StoredOp`]).
struct StoredView {
    id: ViewId,
    bytes: Vec<u8>,
    meta: ViewMeta,
}

/// The validated result of a fetch's op phase: [`fetch`] builds it, [`apply`]
/// consumes it.
struct OpBatch {
    /// Parents-first, as received.
    ops: Vec<StoredOp>,
    views: Vec<StoredView>,
}

impl OpBatch {
    /// Indexes the batch ops' metadata by op id.
    fn ops_by_id(&self) -> HashMap<&OperationId, &OpMeta> {
        self.ops.iter().map(|op| (&op.id, &op.meta)).collect()
    }
}

/// Converts a jj commit id into a gix object id.
fn to_gix_id(id: &CommitId) -> Result<gix::ObjectId> {
    gix::ObjectId::try_from(id.as_bytes()).map_err(|err| eyre!("bad commit id: {err}"))
}

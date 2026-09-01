//! This machine's own mesh record.

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};

use super::{
    membership::{MAX_RECORD_VERSION, Peer, PeerStatus, next_version},
    validate_name,
};

/// The record this machine gossips about itself: the name it announces,
/// versioned like a [`Peer`] so a rename outranks every copy the mesh
/// holds. Named after the short hostname until renamed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Machine {
    pub name: String,
    pub version: u64,
}

impl Machine {
    /// Changes the name; the bumped version propagates it through the
    /// gossip. A same-name rename changes nothing.
    pub(super) fn rename(&mut self, name: String) -> Result<()> {
        validate_name("machine", &name)?;
        if name != self.name {
            self.version = next_version(self.version, "this machine")?;
            self.name = name;
        }
        Ok(())
    }

    /// The record gossiped for this machine.
    pub(super) fn record(&self) -> Peer {
        Peer {
            version: self.version,
            status: PeerStatus::Alive {
                name: self.name.clone(),
            },
        }
    }

    /// Absorbs a copy of our record gossiped back by a peer. Peers bump
    /// their copies on their own (removal, re-pairing), so our version
    /// tracks the highest seen. Our name is the authority: a copy under
    /// another name is outranked outright, so the mesh converges on ours.
    /// A tombstone is only matched, not outranked here; a later rename
    /// still bumps past it (an accepted risk: renaming is as trusted as
    /// the machines themselves).
    ///
    /// Clamped below [`MAX_RECORD_VERSION`]: a peer must not be able to
    /// park us on the ceiling, where renames fail and every machine
    /// discards our record.
    pub(super) fn observe(&mut self, copy: &Peer) {
        let outranking = match copy.name() {
            Some(name) if name != self.name => copy.version + 1,
            _ => copy.version,
        };
        self.version = self.version.max(outranking).min(MAX_RECORD_VERSION - 1);
    }
}

impl Default for Machine {
    fn default() -> Self {
        Machine {
            name: short_hostname(),
            version: 0,
        }
    }
}

/// The first label of the hostname (macOS reports `name.local`), or a
/// placeholder when the host carries a name the mesh would refuse.
fn short_hostname() -> String {
    let name = gethostname::gethostname().to_string_lossy().into_owned();
    let short = match name.split_once('.') {
        Some((short, _)) if !short.is_empty() => short.to_owned(),
        _ => name,
    };
    match validate_name("machine", &short) {
        Ok(()) => short,
        Err(_) => "[invalid hostname]".to_owned(),
    }
}

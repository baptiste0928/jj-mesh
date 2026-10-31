//! This machine's own mesh record.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::{
    membership::{Peer, PeerStatus, next_version},
    validate_name,
};

/// The record this machine gossips about itself: the name it announces and
/// whether it runs in server mode, versioned like a [`Peer`] so a change
/// outranks every copy the mesh holds. Named after the short hostname until
/// renamed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Machine {
    pub name: String,
    /// Whether this machine keeps a bare repo of every mesh repo (see
    /// `daemon::bare`).
    #[serde(default)]
    pub server: bool,
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

    /// Switches server mode; the bumped version propagates it through the
    /// gossip.
    pub(super) fn set_server(&mut self, server: bool) -> Result<()> {
        if server != self.server {
            self.version = next_version(self.version, "this machine")?;
            self.server = server;
        }
        Ok(())
    }

    /// The record gossiped for this machine.
    pub(super) fn record(&self) -> Peer {
        Peer {
            version: self.version,
            status: PeerStatus::Alive {
                name: self.name.clone(),
                server: self.server,
            },
        }
    }

    /// Absorbs a copy of our record gossiped back by a peer. Peers bump
    /// their copies on their own (removal, re-pairing), so our version
    /// tracks the highest seen. Our record is the authority: a copy with
    /// another name or role is outranked outright, so the mesh converges on
    /// ours.
    /// A tombstone is only matched, not outranked here; a later rename
    /// still bumps past it (an accepted risk: renaming is as trusted as
    /// the machines themselves).
    pub(super) fn observe(&mut self, copy: &Peer) {
        let outranking = match &copy.status {
            PeerStatus::Alive { name, server } if *name != self.name || *server != self.server => {
                copy.version.saturating_add(1)
            }
            _ => copy.version,
        };
        self.version = self.version.max(outranking);
    }
}

impl Default for Machine {
    fn default() -> Self {
        Machine {
            name: short_hostname(),
            server: false,
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

//! Configuration directory files and state.
//!
//! We store the following files under the config directory (usually
//! `~/.config/jj-mesh`):
//! - `machine.key`: private identity key of the current machine, used by iroh
//! - `mesh.json`: the mesh state (paired peers and registered repos), owned
//!   and written by the daemon only; the CLI mutates it through the control
//!   socket

mod key;
mod name;
mod resolve;
mod state;

pub use key::MachineKey;
#[cfg(test)]
pub(crate) use name::MAX_NAME_LEN;
pub(crate) use name::{sanitize, sanitize_bounded, validate_name};
pub use resolve::ConfigDir;
pub use state::{
    MAX_MACHINE_WORKSPACES, MAX_MESH_PEERS, MAX_MESH_REPOS, MAX_MESH_WORKSPACES, Machine,
    Membership, MeshRepo, MeshRepoStatus, MeshState, Peer, PeerStatus, Repo, RepoClaims, RepoId,
    WorkspaceClaims,
};

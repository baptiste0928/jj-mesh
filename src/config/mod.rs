//! Configuration directory files and state.

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

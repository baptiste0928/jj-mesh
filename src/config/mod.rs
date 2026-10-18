//! Configuration directory files and state, and the jj-mesh settings in
//! jj config.

mod key;
mod name;
mod resolve;
mod settings;
mod state;

pub use key::MachineKey;
#[cfg(test)]
pub(crate) use name::MAX_NAME_LEN;
pub(crate) use name::{sanitize, sanitize_bounded, validate_name};
pub use resolve::ConfigDir;
pub(crate) use resolve::create_private;
pub(crate) use settings::{MAX_SNAPSHOT_INTERVAL, SNAPSHOT_INTERVAL, Settings, parse_interval};
pub use state::{
    MAX_MACHINE_WORKSPACES, MAX_MESH_CLAIMED_REPOS, MAX_MESH_PEERS, MAX_MESH_REPOS,
    MAX_MESH_WORKSPACES, Machine, Membership, MeshRepo, MeshRepoStatus, MeshState, ObjectHash,
    Peer, PeerStatus, Repo, RepoClaims, RepoId, WorkspaceClaims,
};

//! Configuration directory files and state, and the jj-mesh settings in
//! jj config.

mod key;
mod name;
mod resolve;
mod settings;
mod state;

pub use key::MachineKey;
pub(crate) use name::{sanitize, sanitize_bounded, validate_name};
pub use resolve::ConfigDir;
pub(crate) use resolve::create_private;
pub(crate) use settings::{MAX_SNAPSHOT_INTERVAL, SNAPSHOT_INTERVAL, Settings, parse_interval};
pub use state::{
    Machine, Membership, MeshRepo, MeshRepoStatus, MeshState, ObjectHash, Peer, PeerStatus, Repo,
    RepoClaims, RepoId, WorkspaceClaims,
};

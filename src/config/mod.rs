//! Configuration directory files and state.
//!
//! We store the following files under the config directory (usually
//! `~/.config/jj-mesh`):
//! - `machine.key`: private identity key of the current machine, used by iroh
//! - `mesh.json`: the mesh state (paired peers and registered repos), owned
//!   and written by the daemon only; the CLI mutates it through the control
//!   socket
//! - `config.toml`: user-tunable daemon behavior, edited by the user and
//!   read once at daemon start

mod key;
mod name;
mod resolve;
mod settings;
mod state;

pub use key::MachineKey;
#[cfg(test)]
pub(crate) use name::MAX_NAME_LEN;
pub(crate) use name::{is_confusable, sanitize, validate_name};
pub use resolve::ConfigDir;
pub use settings::{RepoSettings, Settings};
pub use state::{
    MAX_MESH_PEERS, MAX_MESH_REPOS, Membership, MeshRepo, MeshRepoStatus, MeshState, Peer,
    PeerStatus, Repo, RepoId,
};

//! Configuration directory files and state.
//!
//! We store the following files under the config directory (usually
//! `~/.config/jj-mesh`):
//! - `machine.key`: private identity key of the current machine, used by iroh
//! - `peers.json`: the mesh state (paired peers and registered repos), owned
//!   and written by the daemon only; the CLI mutates it through the control
//!   socket

mod key;
mod resolve;
mod state;

pub use key::MachineKey;
pub use resolve::ConfigDir;
pub use state::{MeshState, Peer, Repo, RepoId};
pub(crate) use state::{is_confusable, validate_name};

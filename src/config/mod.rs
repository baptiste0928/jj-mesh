//! Configuration files and state.
//!
//! We store the following files under the config directory (usually
//! `~/.config/jj-mesh`):
//! - `machine.key`: private identity key of the current machine, used by iroh
//! - `config.toml`: user configuration and state, with the paired peers
//!   and registered repos

mod edit;
mod key;
mod model;
mod resolve;
mod watch;

pub use edit::ConfigEdit;
pub use key::MachineKey;
pub use model::{Config, Peer, Repo, RepoId};
pub use resolve::ConfigDir;
pub use watch::ConfigWatcher;

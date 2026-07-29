//! Dynamic shell completion of repo and peer names.
//!
//! Candidates come from the local mesh state file, so completion works
//! whether or not the daemon is running. Completion must never get in the
//! user's way: any error degrades to an empty candidate list.

use clap_complete::CompletionCandidate;

use crate::config::{ConfigDir, MeshState};

/// Loads the mesh state from the default configuration directory. A custom
/// `--config-dir` is not honored during completion.
fn state() -> MeshState {
    ConfigDir::new(None)
        .and_then(|dir| MeshState::load(&dir))
        .unwrap_or_default()
}

/// The repos registered on this machine (`repo forget`).
pub fn registered_repos() -> Vec<CompletionCandidate> {
    state().repos.keys().map(CompletionCandidate::new).collect()
}

/// The repo names in use on the mesh (`repo remove`).
pub fn mesh_repos() -> Vec<CompletionCandidate> {
    state()
        .mesh_repo_names()
        .map(CompletionCandidate::new)
        .collect()
}

/// The mesh repos not registered on this machine, i.e. the clonable ones
/// (`repo clone`).
pub fn clonable_repos() -> Vec<CompletionCandidate> {
    let state = state();
    state
        .mesh_repo_names()
        .filter(|name| !state.repos.contains_key(*name))
        .map(CompletionCandidate::new)
        .collect()
}

/// The alive peers (`peer remove`), with their endpoint id as help text
/// since names can be ambiguous.
pub fn peers() -> Vec<CompletionCandidate> {
    state()
        .alive_peers()
        .map(|(endpoint, name)| {
            CompletionCandidate::new(name).help(Some(endpoint.to_string().into()))
        })
        .collect()
}

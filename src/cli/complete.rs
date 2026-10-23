//! Dynamic shell completion of repo and peer names.
//!
//! Candidates come from the local mesh state file, so completion works
//! whether or not the daemon is running. Completion must never get in the
//! user's way: any error degrades to an empty candidate list.

use std::collections::BTreeSet;

use clap_complete::CompletionCandidate;

use crate::config::{ConfigDir, MeshState};

/// Loads the mesh state from the default configuration directory. A custom
/// `--config-dir` is not honored during completion.
fn state() -> MeshState {
    ConfigDir::new(None)
        .and_then(|dir| MeshState::load(&dir))
        .unwrap_or_default()
}

/// The names of the repos registered on this machine (`repo forget`).
pub(super) fn registered_repos() -> Vec<CompletionCandidate> {
    let state = state();
    let names: BTreeSet<&str> = state
        .repos
        .keys()
        .filter_map(|id| state.repo_name(id))
        .collect();
    names.into_iter().map(CompletionCandidate::new).collect()
}

/// The names of the repos in the mesh (`repo clone`, `repo remove`).
pub(super) fn mesh_repos() -> Vec<CompletionCandidate> {
    state()
        .repo_names()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

/// The alive peers (`peer remove`).
pub(super) fn peers() -> Vec<CompletionCandidate> {
    state()
        .alive_peers()
        .map(|(_, name)| CompletionCandidate::new(name))
        .collect()
}

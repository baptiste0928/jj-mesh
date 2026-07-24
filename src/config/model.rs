//! Configuration file (`config.toml`).
//!
//! This file contains the paired peers and registered repos. This module
//! contains only the read-only view, update methods are in the `edit` module.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, bail, ensure};
use data_encoding::HEXLOWER;
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use super::{ConfigDir, ConfigEdit};

/// Maximum length of a peer or repo name, in bytes.
///
/// Names can be announced by remote machines and end up as TOML keys and
/// terminal output, so they are kept short and free of control characters.
const MAX_NAME_LEN: usize = 64;

/// Checks that a peer or repo name is usable; `kind` names it in errors.
fn validate_name(kind: &str, name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "{kind} name cannot be empty");
    ensure!(
        name.len() <= MAX_NAME_LEN,
        "{kind} name is longer than {MAX_NAME_LEN} bytes",
    );
    ensure!(
        name.chars().all(|c| !c.is_control()),
        "{kind} name contains control characters",
    );

    Ok(())
}

/// Configuration of jj-mesh.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub peers: BTreeMap<String, Peer>,
    pub repos: BTreeMap<String, Repo>,
}

impl Config {
    /// Loads `config.toml` from the configuration directory, defaulting to an
    /// empty config if the file does not exist yet.
    pub fn from_config(dir: &ConfigDir) -> Result<Self> {
        Ok(ConfigEdit::from_config(dir)?.into_config())
    }

    /// The name under which an endpoint is paired, if any.
    pub fn peer_name(&self, endpoint: &EndpointId) -> Option<&str> {
        self.peers
            .iter()
            .find(|(_, peer)| &peer.endpoint == endpoint)
            .map(|(name, _)| name.as_str())
    }

    /// Checks that a peer can be registered under this name and endpoint,
    /// rejecting invalid names and duplicates of either.
    pub fn validate_new_peer(&self, name: &str, endpoint: &EndpointId) -> Result<()> {
        validate_name("peer", name)?;
        ensure!(
            !self.peers.contains_key(name),
            "a peer named `{name}` already exists",
        );

        if let Some(existing) = self.peer_name(endpoint) {
            bail!("endpoint {endpoint} is already paired as `{existing}`");
        }

        Ok(())
    }

    /// Checks that a repo can be registered under this name and path,
    /// rejecting invalid names and duplicates of either.
    pub fn validate_new_repo(&self, name: &str, path: &Path) -> Result<()> {
        validate_name("repo", name)?;
        ensure!(
            !self.repos.contains_key(name),
            "a repo named `{name}` already exists",
        );

        if let Some((existing, _)) = self.repos.iter().find(|(_, r)| r.path == path) {
            bail!("{} is already added as `{existing}`", path.display());
        }

        Ok(())
    }
}

/// A paired machine. This represents remote peers that we are allowed to
/// exchange data with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    /// The peer's iroh endpoint id (its public key).
    pub endpoint: EndpointId,
}

/// A repo registered on this machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Repo {
    /// Mesh-wide identifier of the repo.
    pub id: RepoId,
    /// Local repository root (the directory containing `.jj`).
    pub path: PathBuf,
}

/// Mesh-wide identifier of a repo, shared by all machines syncing it. Randomly
/// generated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RepoId(String);

impl RepoId {
    /// Generates a random id repo id.
    pub fn generate() -> Self {
        RepoId(HEXLOWER.encode(&rand::random::<[u8; 16]>()))
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

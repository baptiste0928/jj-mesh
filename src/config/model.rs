//! Configuration file (`config.toml`).
//!
//! This file contains the paired peers and registered repos. This module
//! contains only the read-only view, update methods are in the `edit` module.

use std::{collections::BTreeMap, fmt, fs, io, path::PathBuf};

use color_eyre::eyre::{Result, WrapErr as _};
use data_encoding::HEXLOWER;
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use super::ConfigDir;

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
        let path = dir.config_file();
        let config = match fs::read_to_string(&path) {
            Ok(content) => toml_edit::de::from_str(&content)
                .wrap_err_with(|| format!("invalid config in {}", path.display()))?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => Self::default(),
            Err(err) => {
                return Err(err).wrap_err_with(|| format!("cannot read {}", path.display()));
            }
        };

        Ok(config)
    }
}

/// A paired machine. This represente remote peers that we are allowed to
/// exchange data wtih.
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

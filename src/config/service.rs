//! `service.toml`: records which program installed the daemon service.

use std::{fs, io::ErrorKind};

use color_eyre::eyre::{Result, WrapErr as _};
use serde::{Deserialize, Serialize};

use super::ConfigDir;

/// Which program installed the daemon service, and under which label.
///
/// `jj-mesh service install` records itself as [`Self::CLI`]; external
/// managers (the Home Manager module) record their own name. A missing
/// file means no recorded installation.
#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceState {
    pub installer: String,
    pub label: String,
}

impl ServiceState {
    /// Installer name recorded by `jj-mesh service install`.
    pub const CLI: &str = "cli";

    /// Reads the state file; `None` when it does not exist.
    pub fn load(dir: &ConfigDir) -> Result<Option<Self>> {
        let path = dir.service_state_file();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).wrap_err_with(|| format!("cannot read {}", path.display()));
            }
        };
        toml::from_str(&text)
            .map(Some)
            .wrap_err_with(|| format!("cannot parse {}", path.display()))
    }

    /// Records `label` as installed by the CLI.
    pub fn record_cli(dir: &ConfigDir, label: &str) -> Result<()> {
        let state = Self {
            installer: Self::CLI.to_owned(),
            label: label.to_owned(),
        };
        let path = dir.service_state_file();
        fs::write(
            &path,
            toml::to_string(&state).expect("state must serialize"),
        )
        .wrap_err_with(|| format!("cannot write {}", path.display()))
    }

    /// Removes the state file; a missing file is fine.
    pub fn clear(dir: &ConfigDir) -> Result<()> {
        let path = dir.service_state_file();
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).wrap_err_with(|| format!("cannot remove {}", path.display())),
        }
    }
}

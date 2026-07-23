//! Configuration files resolution.
//!
//! The folder containing the configuration follows the XDG convention and is
//! resolved with `etcetera` (it will be `~/.config/jj-mesh` in most cases). It
//! will be created if not existing.

use std::{
    fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr, eyre};
use etcetera::BaseStrategy;

/// Resolved configuration directory.
#[derive(Clone, Debug)]
pub struct ConfigDir(PathBuf);

impl ConfigDir {
    /// Resolves the configuration directory if no `dir` is provided.
    ///
    /// The directory will be created if it doesn't exist. This types guarantees
    /// that the configuration directory exists, so config files are safe to
    /// create inside.
    pub fn new(override_dir: Option<PathBuf>) -> Result<Self> {
        if let Some(config_dir) = override_dir {
            // When a custom directory is provided, we don't create it implicitely.
            let metadata =
                fs::metadata(&config_dir).wrap_err("cannot open custom config directory")?;

            if !metadata.is_dir() {
                return Err(eyre!("custom config path is not a directory"));
            }

            return Ok(ConfigDir(config_dir));
        }

        let config_dir = etcetera::choose_base_strategy()
            .wrap_err("cannot determine the config directory")?
            .config_dir()
            .join("jj-mesh");

        fs::create_dir_all(&config_dir)
            .wrap_err_with(|| format!("cannot create {}", config_dir.display()))?;

        Ok(ConfigDir(config_dir))
    }

    /// Get the resolved path of the config directory.
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Get the path to the machine key file.
    ///
    /// There is no guarantee that the file exists.
    pub fn machine_key(&self) -> PathBuf {
        self.0.join("machine.key")
    }

    /// Get the path to the configuration file.
    ///
    /// There is no guarantee that the file exists.
    pub fn config_file(&self) -> PathBuf {
        self.0.join("config.toml")
    }
}

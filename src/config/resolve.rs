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
pub struct ConfigDir {
    path: PathBuf,
    /// Whether the directory was overridden on the command line, in which
    /// case per-machine paths like the daemon socket stay inside it.
    custom: bool,
}

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

            return Ok(ConfigDir {
                path: config_dir,
                custom: true,
            });
        }

        let config_dir = etcetera::choose_base_strategy()
            .wrap_err("cannot determine the config directory")?
            .config_dir()
            .join("jj-mesh");

        // Created user-only: the directory holds the machine's private key.
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&config_dir)
            .wrap_err_with(|| format!("cannot create {}", config_dir.display()))?;

        Ok(ConfigDir {
            path: config_dir,
            custom: false,
        })
    }

    /// Get the resolved path of the config directory.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the path to the machine key file.
    ///
    /// There is no guarantee that the file exists.
    pub fn machine_key(&self) -> PathBuf {
        self.path.join("machine.key")
    }

    /// Get the path to the configuration file.
    ///
    /// There is no guarantee that the file exists.
    pub fn config_file(&self) -> PathBuf {
        self.path.join("config.toml")
    }

    /// Get the path to the daemon control socket.
    ///
    /// Usually `$XDG_RUNTIME_DIR/jj-mesh.sock`; kept inside custom config
    /// directories so several daemons can coexist on one machine (tests).
    pub fn socket_path(&self) -> PathBuf {
        if !self.custom
            && let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR")
        {
            return PathBuf::from(runtime_dir).join("jj-mesh.sock");
        }

        self.path.join("jj-mesh.sock")
    }
}

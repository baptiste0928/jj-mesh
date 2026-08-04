//! Configuration directory resolution.
//!
//! The directory follows the XDG convention, resolved with `etcetera`
//! (usually `~/.config/jj-mesh`), and is created on first use.

use std::{
    fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr, ensure};
use etcetera::BaseStrategy;

/// Resolved configuration directory. Constructing it guarantees the
/// directory exists, so config files are safe to create inside.
#[derive(Clone, Debug)]
pub struct ConfigDir {
    path: PathBuf,
    /// Whether the directory was overridden on the command line, in which
    /// case per-machine paths like the daemon socket stay inside it.
    custom: bool,
}

impl ConfigDir {
    /// Resolves the configuration directory, or uses `override_dir` when
    /// given. An overridden directory is never created implicitly: it must
    /// already exist.
    pub fn new(override_dir: Option<PathBuf>) -> Result<Self> {
        if let Some(config_dir) = override_dir {
            let metadata =
                fs::metadata(&config_dir).wrap_err("cannot open custom config directory")?;
            ensure!(metadata.is_dir(), "custom config path is not a directory");

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

    /// The resolved config directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the directory was overridden on the command line.
    pub fn is_custom(&self) -> bool {
        self.custom
    }

    /// Path of the machine identity key file (`machine.key`).
    pub fn machine_key(&self) -> PathBuf {
        self.path.join("machine.key")
    }

    /// Path of the mesh state file (`mesh.json`).
    pub fn mesh_file(&self) -> PathBuf {
        self.path.join("mesh.json")
    }

    /// Path of the daemon settings file (`config.toml`).
    pub fn settings_file(&self) -> PathBuf {
        self.path.join("config.toml")
    }

    /// Path of the daemon control socket.
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

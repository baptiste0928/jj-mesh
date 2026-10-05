//! Configuration directory resolution.
//!
//! The directory follows the XDG convention, resolved with `etcetera`
//! (usually `~/.config/jj-mesh`), and is created on first use. The daemon
//! socket goes in the runtime directory, or the state directory
//! (`~/.local/state/jj-mesh`) on systems without one (macOS).

use std::{
    fs,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr, ensure};
use etcetera::BaseStrategy;

/// File name of the daemon control socket.
const SOCKET: &str = "jj-mesh.sock";

/// Resolved configuration directory. Constructing it guarantees the
/// directory exists, so config files are safe to create inside.
#[derive(Clone, Debug)]
pub struct ConfigDir {
    path: PathBuf,
    /// Whether the directory was overridden on the command line.
    custom: bool,
    /// See [`Self::socket_path`].
    socket: PathBuf,
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
                socket: config_dir.join(SOCKET),
                path: config_dir,
                custom: true,
            });
        }

        let strategy =
            etcetera::choose_base_strategy().wrap_err("cannot determine the config directory")?;
        // Created user-only: the directory holds the machine's private key.
        let config_dir = strategy.config_dir().join("jj-mesh");
        create_private(&config_dir)?;

        let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR").filter(|dir| !dir.is_empty());
        let socket = if let Some(runtime_dir) = runtime_dir {
            PathBuf::from(runtime_dir).join(SOCKET)
        } else {
            let state_dir = strategy
                .state_dir()
                .unwrap_or_else(|| strategy.data_dir())
                .join("jj-mesh");
            create_private(&state_dir)?;
            state_dir.join(SOCKET)
        };

        Ok(ConfigDir {
            path: config_dir,
            custom: false,
            socket,
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

    /// Path of the daemon control socket.
    ///
    /// Usually `$XDG_RUNTIME_DIR/jj-mesh.sock`; kept inside custom config
    /// directories so several daemons can coexist on one machine (tests).
    pub fn socket_path(&self) -> &Path {
        &self.socket
    }
}

/// Creates a user-only directory and its missing parents.
fn create_private(dir: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(dir)
        .wrap_err_with(|| format!("cannot create {}", dir.display()))
}

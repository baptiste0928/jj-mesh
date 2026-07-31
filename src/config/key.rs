//! Machine identity key file (`machine.key`).
//!
//! This private key is used by iroh to identify the machine in the p2p network
//! and must not be shared across machines. The public part is the
//! [`EndpointId`], which is used to connect to other peers.
//!
//! The key is stored base64-encoded.

use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use color_eyre::eyre::{Result, WrapErr, eyre};
use data_encoding::BASE64;
use iroh::{EndpointId, SecretKey};

use super::ConfigDir;

/// The machine's secret identity key in the iroh mesh.
#[derive(Clone, Debug)]
pub struct MachineKey(SecretKey);

impl MachineKey {
    /// Loads the key from the configuration directory, generating and
    /// persisting a new one on first use.
    pub fn from_config(config: &ConfigDir) -> Result<Self> {
        let key_file = config.machine_key();

        match fs::read_to_string(&key_file) {
            Ok(content) => Self::from_base64(&content),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Self::generate(&key_file),
            Err(err) => Err(err).wrap_err_with(|| format!("cannot read {}", key_file.display())),
        }
    }

    /// Parses a base64-encoded key (32 bytes, ed25519).
    fn from_base64(content: &str) -> Result<Self> {
        let key: [u8; 32] = BASE64
            .decode(content.trim_ascii().as_bytes())
            .wrap_err("failed to decode machine key")?
            .try_into()
            .map_err(|_| eyre!("failed to decode machine key: should be 32 bytes"))?;

        Ok(Self(SecretKey::from_bytes(&key)))
    }

    /// Generates a new key and writes it to `path`, owner-only (0600) and
    /// refusing to overwrite an existing file.
    fn generate(path: &Path) -> Result<Self> {
        let key = SecretKey::generate();
        let encoded = BASE64.encode(&key.to_bytes());

        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        options
            .open(path)?
            .write_all(encoded.as_bytes())
            .wrap_err("failed to write machine key")?;

        Ok(Self(key))
    }

    /// This machine's public identity on the mesh.
    pub fn endpoint_id(&self) -> EndpointId {
        self.0.public()
    }

    /// The secret key, needed to bring up the iroh endpoint.
    pub fn secret(&self) -> &SecretKey {
        &self.0
    }
}

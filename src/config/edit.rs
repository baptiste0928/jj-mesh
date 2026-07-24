//! Editing `config.toml`.

use std::{fs, io, path::PathBuf};

use color_eyre::eyre::{ContextCompat as _, Result, WrapErr as _, eyre};
use serde::Serialize;
use toml_edit::{DocumentMut, Item, Table};

use super::{Config, ConfigDir, Peer, Repo};

/// Editable view of the config file, using `toml_edit` to preserve user comments
/// and formatting.
#[derive(Debug)]
pub struct ConfigEdit {
    path: PathBuf,
    doc: DocumentMut,
    config: Config,
}

impl ConfigEdit {
    /// Loads `config.toml` from the configuration directory, defaulting to an
    /// empty config if the file does not exist yet.
    pub fn from_config(dir: &ConfigDir) -> Result<Self> {
        let path = dir.config_file();
        let doc: DocumentMut = match fs::read_to_string(&path) {
            Ok(content) => content
                .parse()
                .wrap_err_with(|| format!("invalid TOML in {}", path.display()))?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => DocumentMut::new(),
            Err(err) => {
                return Err(err).wrap_err_with(|| format!("cannot read {}", path.display()));
            }
        };

        let config = toml_edit::de::from_document(doc.clone())
            .wrap_err_with(|| format!("invalid config in {}", path.display()))?;

        Ok(ConfigEdit { path, doc, config })
    }

    /// Typed read-only view of the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Consumes the editor, keeping only the typed configuration.
    pub fn into_config(self) -> Config {
        self.config
    }

    /// Adds a peer, rejecting duplicate names and endpoints.
    pub fn add_peer(&mut self, name: String, peer: Peer) -> Result<()> {
        self.config.validate_new_peer(&name, &peer.endpoint)?;

        self.insert_entry("peers", &name, &peer)?;
        self.config.peers.insert(name, peer);

        Ok(())
    }

    /// Removes a peer by name.
    pub fn remove_peer(&mut self, name: &str) -> Result<Peer> {
        let peer = self
            .config
            .peers
            .remove(name)
            .ok_or_else(|| eyre!("no peer named `{name}`"))?;
        self.remove_entry("peers", name);

        Ok(peer)
    }

    /// Adds a repo, rejecting duplicate names and paths.
    pub fn add_repo(&mut self, name: String, repo: Repo) -> Result<()> {
        self.config.validate_new_repo(&name, &repo.path)?;

        self.insert_entry("repos", &name, &repo)?;
        self.config.repos.insert(name, repo);

        Ok(())
    }

    /// Saves the document, atomically so a crash cannot truncate the config.
    pub fn save(&self) -> Result<()> {
        let tmp = self.path.with_extension("toml.tmp");
        fs::write(&tmp, self.doc.to_string())
            .wrap_err_with(|| format!("cannot write {}", tmp.display()))?;
        fs::rename(&tmp, &self.path)
            .wrap_err_with(|| format!("cannot write {}", self.path.display()))?;

        Ok(())
    }

    /// Inserts `value` as the `[<section>.<name>]` table of the document.
    fn insert_entry(&mut self, section: &str, name: &str, value: &impl Serialize) -> Result<()> {
        let item = toml_edit::ser::to_document(value)
            .expect("config entries must serialize to TOML tables")
            .as_item()
            .clone();

        let table = self
            .doc
            .entry(section)
            .or_insert(Item::Table(Table::new()))
            .as_table_mut()
            .with_context(|| format!("`{section}` is not a table in {}", self.path.display()))?;

        table.set_implicit(true);
        table.insert(name, item);

        Ok(())
    }

    /// Removes the `[<section>.<name>]` table from the document.
    fn remove_entry(&mut self, section: &str, name: &str) {
        if let Some(table) = self.doc.get_mut(section).and_then(Item::as_table_mut) {
            table.remove(name);
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    #[test]
    fn add_and_remove_peer_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        let endpoint = SecretKey::generate().public();

        let mut edit = ConfigEdit::from_config(&dir).unwrap();
        edit.add_peer("laptop".to_owned(), Peer { endpoint })
            .unwrap();
        edit.save().unwrap();

        let mut edit = ConfigEdit::from_config(&dir).unwrap();
        assert!(edit.remove_peer("desktop").is_err());
        let removed = edit.remove_peer("laptop").unwrap();
        assert_eq!(removed.endpoint, endpoint);
        edit.save().unwrap();

        let config = Config::from_config(&dir).unwrap();
        assert!(config.peers.is_empty());
    }
}

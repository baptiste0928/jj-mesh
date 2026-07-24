//! Watching the configuration file for changes.
//!
//! Lets the daemon pick up new peers and repos without restarting, whether
//! the config was changed by the CLI or edited by hand.

use std::{ffi::OsStr, time::Duration};

use color_eyre::eyre::Result;

use super::ConfigDir;
use crate::watch::DirWatcher;

/// Debounce window: editors and the CLI's atomic save emit event bursts.
const DEBOUNCE: Duration = Duration::from_millis(300);

/// Cap on the total debounce, so a busy writer cannot starve reloads.
const DEBOUNCE_MAX: Duration = Duration::from_secs(2);

/// Watches `config.toml` for modifications.
///
/// The watch is set on the configuration directory: saves replace the file
/// by rename, so watching the file's inode directly would miss updates.
#[derive(Debug)]
pub struct ConfigWatcher {
    watch: DirWatcher,
}

impl ConfigWatcher {
    /// Starts watching the configuration directory.
    pub fn new(dir: &ConfigDir) -> Result<Self> {
        let watch = DirWatcher::new(dir.path(), DEBOUNCE, DEBOUNCE_MAX, |event| {
            event
                .paths
                .iter()
                .any(|path| path.file_name() == Some(OsStr::new("config.toml")))
        })?;

        Ok(ConfigWatcher { watch })
    }

    /// Waits for the next config change, debouncing event bursts. Errors
    /// when the watch died (e.g. the config directory was removed).
    pub async fn changed(&mut self) -> Result<()> {
        self.watch.changed().await
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, time::Duration};

    use super::*;

    #[tokio::test]
    async fn detects_atomic_save() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        let mut watcher = ConfigWatcher::new(&dir).unwrap();

        // Mimic `ConfigEdit::save`: write a temp file, then rename over.
        let tmp_file = PathBuf::from(tmp.path()).join("config.toml.tmp");
        fs::write(&tmp_file, "[peers]\n").unwrap();
        fs::rename(&tmp_file, dir.config_file()).unwrap();

        tokio::time::timeout(Duration::from_secs(5), watcher.changed())
            .await
            .expect("no change detected")
            .unwrap();
    }

    #[tokio::test]
    async fn ignores_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        fs::write(dir.config_file(), "[peers]\n").unwrap();
        let mut watcher = ConfigWatcher::new(&dir).unwrap();

        // Reading the config (as the daemon does on reload) must not count
        // as a change, or reloading would re-trigger the watch forever.
        fs::read(dir.config_file()).unwrap();

        let woke = tokio::time::timeout(Duration::from_secs(1), watcher.changed()).await;
        assert!(woke.is_err(), "read must not trigger the watcher");
    }
}

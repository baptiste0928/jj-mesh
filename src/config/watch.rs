//! Watching the configuration file for changes.
//!
//! Lets the daemon pick up new peers and repos without restarting, whether
//! the config was changed by the CLI or edited by hand.

use std::{ffi::OsStr, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _, eyre};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use tokio::sync::mpsc;

use super::ConfigDir;

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
    /// Dropping the watcher stops notify's background threads.
    _watcher: RecommendedWatcher,
    events: mpsc::UnboundedReceiver<()>,
}

impl ConfigWatcher {
    /// Starts watching the configuration directory.
    pub fn new(dir: &ConfigDir) -> Result<Self> {
        let (tx, events) = mpsc::unbounded_channel();

        let mut watcher = notify::recommended_watcher(move |event: notify::Result<Event>| {
            let Ok(event) = event else { return };
            let concerns_config = event
                .paths
                .iter()
                .any(|path| path.file_name() == Some(OsStr::new("config.toml")));
            if concerns_config {
                let _ = tx.send(());
            }
        })
        .wrap_err("cannot create config watcher")?;

        watcher
            .watch(dir.path(), RecursiveMode::NonRecursive)
            .wrap_err_with(|| format!("cannot watch {}", dir.path().display()))?;

        Ok(ConfigWatcher {
            _watcher: watcher,
            events,
        })
    }

    /// Waits for the next config change, debouncing event bursts.
    pub async fn changed(&mut self) -> Result<()> {
        self.events
            .recv()
            .await
            .ok_or_else(|| eyre!("config watcher stopped"))?;

        let deadline = tokio::time::Instant::now() + DEBOUNCE_MAX;
        loop {
            match tokio::time::timeout(DEBOUNCE, self.events.recv()).await {
                Ok(Some(())) if tokio::time::Instant::now() < deadline => {}
                _ => return Ok(()),
            }
        }
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
}

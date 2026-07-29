//! `config.toml`: user-tunable daemon behavior.
//!
//! Every key is optional and defaults in code, so a missing or empty file
//! is valid. Tables under `[repos.<name>]` override the global keys for
//! one repo, keyed by mesh repo name. A fully commented template
//! documenting the defaults is written on first daemon start.
//!
//! Parsing is strict (unknown keys are errors): a typoed key silently
//! doing nothing is worse than a load failure, which the daemon reports
//! and survives by keeping the previous settings.

use std::{collections::BTreeMap, fs, io::ErrorKind, time::Duration};

use color_eyre::eyre::{Result, WrapErr as _};
use serde::Deserialize;

use super::ConfigDir;

/// Default seconds between an edit and its automatic snapshot.
const DEFAULT_SNAPSHOT_INTERVAL: u64 = 20;

/// Default for running `jj workspace update-stale` after syncs.
const DEFAULT_UPDATE_STALE: bool = true;

/// Ceiling on the snapshot interval, so arithmetic on deadlines can never
/// overflow. A day is already indistinguishable from disabled.
const MAX_SNAPSHOT_INTERVAL: u64 = 24 * 60 * 60;

/// Template written to a missing `config.toml`. Every key stays commented
/// out: the file documents the defaults without freezing them.
const TEMPLATE: &str = r"# jj-mesh configuration
#
# The commented values below are the default options. Restart the background
# service after updating this file. 

# Interval between automatic snapshots of the working copy, in seconds.
# Set 0 to disable auto-snapshotting.
#snapshot-interval = 20

# Update the working copy after syncing if stale.
#update-stale = true

# Configuration can be overriden per repo:
#
#[repos.example]
#snapshot-interval = 0
#update-stale = false
";

/// Parsed contents of `config.toml`. Fields keep the "unset" state so
/// resolution can fall back per key: repo override, then global, then
/// default.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
pub struct Settings {
    snapshot_interval: Option<u64>,
    update_stale: Option<bool>,
    repos: BTreeMap<String, RepoOverrides>,
}

/// One `[repos.<name>]` table.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
struct RepoOverrides {
    snapshot_interval: Option<u64>,
    update_stale: Option<bool>,
}

/// Effective settings for one repo, after override resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepoSettings {
    /// Wait between an edit and its snapshot; `None` disables
    /// auto-snapshotting.
    pub snapshot_interval: Option<Duration>,
    /// Whether to run `jj workspace update-stale` after syncing.
    pub update_stale: bool,
}

impl Settings {
    /// Loads `config.toml`, treating a missing file as empty. Errors on
    /// unreadable or invalid contents; the caller decides the fallback.
    pub fn load(dir: &ConfigDir) -> Result<Self> {
        let path = dir.settings_file();
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => {
                return Err(err).wrap_err_with(|| format!("cannot read {}", path.display()));
            }
        };
        toml::from_str(&text).wrap_err_with(|| format!("cannot parse {}", path.display()))
    }

    /// Writes the commented template, unless `config.toml` already exists
    /// (a concurrent writer included: creation is exclusive).
    pub fn write_template(dir: &ConfigDir) -> Result<()> {
        use std::io::Write as _;

        let path = dir.settings_file();
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => file
                .write_all(TEMPLATE.as_bytes())
                .wrap_err_with(|| format!("cannot write {}", path.display())),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(err) => Err(err).wrap_err_with(|| format!("cannot create {}", path.display())),
        }
    }

    /// Resolves the effective settings for one repo.
    pub fn for_repo(&self, name: &str) -> RepoSettings {
        let overrides = self.repos.get(name);
        let interval = overrides
            .and_then(|o| o.snapshot_interval)
            .or(self.snapshot_interval)
            .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL)
            .min(MAX_SNAPSHOT_INTERVAL);

        RepoSettings {
            snapshot_interval: (interval > 0).then(|| Duration::from_secs(interval)),
            update_stale: overrides
                .and_then(|o| o.update_stale)
                .or(self.update_stale)
                .unwrap_or(DEFAULT_UPDATE_STALE),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Settings {
        toml::from_str(text).unwrap()
    }

    #[test]
    fn empty_file_yields_defaults() {
        let settings = parse("");
        let repo = settings.for_repo("any");
        assert_eq!(
            repo.snapshot_interval,
            Some(Duration::from_secs(DEFAULT_SNAPSHOT_INTERVAL)),
        );
        assert!(repo.update_stale);
    }

    #[test]
    fn per_repo_overrides_global_overrides_default() {
        let settings = parse(
            "snapshot-interval = 60\n\
             [repos.quiet]\n\
             snapshot-interval = 0\n\
             update-stale = false\n",
        );

        let global = settings.for_repo("other");
        assert_eq!(global.snapshot_interval, Some(Duration::from_mins(1)));
        assert!(global.update_stale);

        let quiet = settings.for_repo("quiet");
        assert_eq!(quiet.snapshot_interval, None);
        assert!(!quiet.update_stale);
    }

    #[test]
    fn oversized_interval_is_clamped() {
        let settings = parse(&format!("snapshot-interval = {}", u64::MAX));
        assert_eq!(
            settings.for_repo("any").snapshot_interval,
            Some(Duration::from_secs(MAX_SNAPSHOT_INTERVAL)),
        );
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(toml::from_str::<Settings>("snapshot-intervall = 20").is_err());
        assert!(toml::from_str::<Settings>("[repos.a]\nsnapshotting = true").is_err());
    }

    /// The template must parse as-is (all comments, so all defaults), and
    /// its commented-out keys must parse once uncommented: the template
    /// stays in sync with the schema.
    #[test]
    fn template_matches_schema() {
        assert_eq!(parse(TEMPLATE), Settings::default());

        let mut uncommented = String::new();
        for line in TEMPLATE.lines() {
            let key = line
                .strip_prefix('#')
                .filter(|rest| !rest.is_empty() && !rest.starts_with([' ', '#']));
            uncommented.push_str(key.unwrap_or(line));
            uncommented.push('\n');
        }
        let settings = parse(&uncommented);
        assert_ne!(settings, Settings::default());
        assert_eq!(settings.for_repo("example").snapshot_interval, None);
        assert!(!settings.for_repo("example").update_stale);
    }

    #[test]
    fn template_written_once_and_loadable() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();

        assert_eq!(Settings::load(&dir).unwrap(), Settings::default());

        Settings::write_template(&dir).unwrap();
        assert_eq!(Settings::load(&dir).unwrap(), Settings::default());

        // A user-edited file must survive later template writes.
        fs::write(dir.settings_file(), "snapshot-interval = 5").unwrap();
        Settings::write_template(&dir).unwrap();
        let settings = Settings::load(&dir).unwrap();
        assert_eq!(
            settings.for_repo("any").snapshot_interval,
            Some(Duration::from_secs(5)),
        );
    }
}

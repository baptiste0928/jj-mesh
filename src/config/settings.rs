//! Workspace settings, read from the user's jj config.
//!
//! Settings live in the `[jj-mesh]` table and resolve like any jj setting
//! (user, then repo, then workspace config), so each workspace task reads
//! its own through the jj binary when it starts. Every key is optional and
//! defaults in code. `jj-mesh add` and `clone` write the
//! [`SNAPSHOT_INTERVAL`] given on their command line.
//!
//! Parsing is strict (unknown keys are errors): a typoed key silently
//! doing nothing is worse than a load failure, which the workspace task
//! reports and survives by falling back to the defaults.

use std::{path::Path, time::Duration};

use anyhow::{Context as _, Result, anyhow, ensure};
use jiff::SignedDuration;
use serde::Deserialize;

use crate::repo::jj_output;

/// The jj config table holding the settings.
const TABLE: &str = "jj-mesh";

/// The jj config key of the wait between an edit and its automatic
/// snapshot: seconds, or a duration with units (see [`parse_interval`]);
/// 0 disables auto-snapshot.
pub const SNAPSHOT_INTERVAL: &str = "jj-mesh.snapshot-interval";

/// Default seconds between an edit and its automatic snapshot.
const DEFAULT_SNAPSHOT_INTERVAL: u64 = 20;

/// Default for running `jj workspace update-stale` on stale working copies.
const DEFAULT_UPDATE_STALE: bool = true;

/// Ceiling on the snapshot interval, so arithmetic on deadlines can never
/// overflow. A day is already indistinguishable from disabled.
pub const MAX_SNAPSHOT_INTERVAL: u64 = 24 * 60 * 60;

/// Budget for reading the config.
const LOAD_TIMEOUT: Duration = Duration::from_secs(30);

/// `jj config list` arguments printing one `<name>\t<JSON value>` line
/// per key of the table.
const LIST_ARGS: [&str; 6] = [
    "--ignore-working-copy",
    "config",
    "list",
    TABLE,
    "--template",
    r#"name ++ "\t" ++ json(value) ++ "\n""#,
];

/// Effective settings of one workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Settings {
    /// Wait between an edit and its snapshot; `None` disables
    /// auto-snapshotting.
    pub snapshot_interval: Option<Duration>,
    /// Whether to run `jj workspace update-stale` on stale working copies.
    pub update_stale: bool,
}

/// The `[jj-mesh]` table as configured.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "kebab-case")]
struct Table {
    snapshot_interval: Option<Interval>,
    update_stale: Option<bool>,
}

/// A configured snapshot interval, in seconds.
#[derive(Debug, Deserialize)]
#[serde(try_from = "RawInterval")]
struct Interval(u64);

/// The forms a snapshot interval is written in.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawInterval {
    Secs(u64),
    Text(String),
}

impl TryFrom<RawInterval> for Interval {
    type Error = String;

    fn try_from(raw: RawInterval) -> Result<Self, Self::Error> {
        match raw {
            RawInterval::Secs(secs) => Ok(Interval(secs)),
            RawInterval::Text(text) => parse_interval(&text)
                .map(Interval)
                .map_err(|err| format!("{err:#}")),
        }
    }
}

/// Parses a snapshot interval into whole seconds: bare seconds, or a
/// duration with units (`30s`, `5m`, `1h30m`) of which only zero is below
/// a second.
pub fn parse_interval(text: &str) -> Result<u64> {
    if let Ok(secs) = text.parse() {
        return Ok(secs);
    }
    let interval: SignedDuration = text
        .parse()
        .map_err(|_| anyhow!("invalid duration `{text}`, expected e.g. `30s`, `5m` or `1h30m`"))?;
    let interval = Duration::try_from(interval).context("the interval cannot be negative")?;
    ensure!(
        interval.is_zero() || interval.as_secs() > 0,
        "the interval must be at least a second, or 0 to disable",
    );
    Ok(interval.as_secs())
}

impl Settings {
    /// Reads the settings of the workspace at `root`.
    pub async fn load(root: &Path) -> Result<Self> {
        Self::parse(&jj_output(root, &LIST_ARGS, LOAD_TIMEOUT).await?)
    }

    /// Parses the `jj config list` output of [`Self::load`].
    fn parse(output: &str) -> Result<Self> {
        let mut table = serde_json::Map::new();
        for line in output.lines() {
            let (name, value) = line
                .split_once('\t')
                .ok_or_else(|| anyhow!("unexpected jj config output: {line}"))?;
            let key = name
                .strip_prefix(TABLE)
                .and_then(|key| key.strip_prefix('.'))
                .ok_or_else(|| anyhow!("unexpected jj config key: {name}"))?;
            let value = serde_json::from_str(value)
                .with_context(|| format!("unexpected jj config value: {line}"))?;
            table.insert(key.to_owned(), value);
        }

        let table: Table = serde_json::from_value(table.into())
            .with_context(|| format!("invalid [{TABLE}] config"))?;
        Ok(table.into())
    }
}

impl Default for Settings {
    fn default() -> Self {
        Table::default().into()
    }
}

impl From<Table> for Settings {
    fn from(table: Table) -> Self {
        let interval = table
            .snapshot_interval
            .map_or(DEFAULT_SNAPSHOT_INTERVAL, |Interval(secs)| secs)
            .min(MAX_SNAPSHOT_INTERVAL);

        Settings {
            snapshot_interval: (interval > 0).then(|| Duration::from_secs(interval)),
            update_stale: table.update_stale.unwrap_or(DEFAULT_UPDATE_STALE),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Fixture;

    #[test]
    fn empty_output_yields_defaults() {
        let settings = Settings::parse("").unwrap();
        assert_eq!(
            settings.snapshot_interval,
            Some(Duration::from_secs(DEFAULT_SNAPSHOT_INTERVAL)),
        );
        assert!(settings.update_stale);
    }

    #[test]
    fn parses_keys() {
        let output = format!("{SNAPSHOT_INTERVAL}\t0\njj-mesh.update-stale\tfalse\n");
        let settings = Settings::parse(&output).unwrap();
        assert_eq!(settings.snapshot_interval, None);
        assert!(!settings.update_stale);
    }

    #[test]
    fn parses_intervals_with_units() {
        let interval = |text: &str| {
            let output = format!("{SNAPSHOT_INTERVAL}\t{}", serde_json::json!(text));
            Settings::parse(&output).map(|settings| settings.snapshot_interval)
        };
        assert_eq!(interval("5m").unwrap(), Some(Duration::from_mins(5)));
        assert_eq!(interval("1h 30m").unwrap(), Some(Duration::from_mins(90)));
        assert_eq!(interval("1.5s").unwrap(), Some(Duration::from_secs(1)));
        assert_eq!(interval("0s").unwrap(), None);
        assert_eq!(interval("007").unwrap(), Some(Duration::from_secs(7)));
        for bad in ["500ms", "-5m", "soon"] {
            assert!(interval(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn oversized_interval_is_clamped() {
        let settings =
            Settings::parse(&format!("jj-mesh.snapshot-interval\t{}", u64::MAX)).unwrap();
        assert_eq!(
            settings.snapshot_interval,
            Some(Duration::from_secs(MAX_SNAPSHOT_INTERVAL)),
        );
    }

    #[test]
    fn invalid_entries_are_rejected() {
        assert!(Settings::parse("jj-mesh.snapshot-intervall\t20").is_err());
        assert!(Settings::parse("jj-mesh.repos.a.update-stale\ttrue").is_err());
        assert!(Settings::parse("other.snapshot-interval\t20").is_err());
    }

    /// The workspace config overrides the repo config, which overrides
    /// the user config. Runs [`LIST_ARGS`] through the fixture, as the
    /// daemon's jj would read the developer's real config.
    #[test]
    fn resolves_jj_config_levels() {
        let fx = Fixture::new();
        fx.set_user_config("[jj-mesh]\nsnapshot-interval = 7\nupdate-stale = false\n");
        let dir = fx.init_repo("a");
        fx.jj(
            &dir,
            &["config", "set", "--repo", "jj-mesh.snapshot-interval", "5m"],
        );
        fx.jj(&dir, &["workspace", "add", "../child"]);
        let child = fx.path().join("child");
        fx.jj(
            &child,
            &[
                "config",
                "set",
                "--workspace",
                "jj-mesh.snapshot-interval",
                "0",
            ],
        );
        let load = |dir: &std::path::Path| Settings::parse(&fx.jj_output(dir, &LIST_ARGS)).unwrap();

        let settings = load(&dir);
        assert_eq!(settings.snapshot_interval, Some(Duration::from_mins(5)));
        assert!(!settings.update_stale);

        let settings = load(&child);
        assert_eq!(settings.snapshot_interval, None);
        assert!(!settings.update_stale);
    }
}

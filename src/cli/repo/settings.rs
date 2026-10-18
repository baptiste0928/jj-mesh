//! Settings flags of `repo add` and `repo clone`, written to jj config.
//!
//! The daemon reads a workspace's settings once, when it starts keeping
//! it fresh, so they are written before it can: repo config before the
//! repo is registered or gets its first workspace, workspace config while
//! the daemon holds the new workspace.

use std::path::Path;

use clap::Args;
use color_eyre::eyre::Result;

use super::jj;
use crate::config::{MAX_SNAPSHOT_INTERVAL, SNAPSHOT_INTERVAL, parse_interval};

/// Settings of the repo, or of the workspace when cloning a repo that
/// already has one here.
#[derive(Debug, Args)]
#[command(next_help_heading = "Settings")]
pub struct SettingsArgs {
    /// Seconds between an edit and its automatic snapshot, or a duration
    /// with units like `5m` (0 disables auto-snapshot, and the file
    /// watching it needs)
    #[arg(long, value_name = "INTERVAL", value_parser = interval)]
    snapshot_interval: Option<String>,
}

/// Validates a `--snapshot-interval`, kept as given for jj config: what
/// jj stores, as a number or a string, parses back to the same interval.
fn interval(text: &str) -> Result<String, String> {
    let secs = parse_interval(text).map_err(|err| format!("{err:#}"))?;
    if secs > MAX_SNAPSHOT_INTERVAL {
        return Err("the interval must be at most a day".to_owned());
    }
    Ok(text.to_owned())
}

/// The jj config level settings are written to.
#[derive(Clone, Copy, Debug)]
pub enum Scope {
    Repo,
    Workspace,
}

impl SettingsArgs {
    /// Whether any setting is given.
    pub fn is_empty(&self) -> bool {
        self.snapshot_interval.is_none()
    }

    /// Writes the given settings to the `scope` config of the workspace or
    /// bare repo at `root`.
    pub fn write(&self, root: &Path, scope: Scope) -> Result<()> {
        let scope = match scope {
            Scope::Repo => "--repo",
            Scope::Workspace => "--workspace",
        };
        if let Some(interval) = &self.snapshot_interval {
            jj(
                Some(root),
                &["config", "set", scope, SNAPSHOT_INTERVAL, interval],
            )?;
        }
        Ok(())
    }
}

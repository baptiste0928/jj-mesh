//! `jj-mesh server`: switch server mode on this machine.

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, bail};

use super::ui;
use crate::{
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Manage server mode
///
/// A server keeps a copy of every mesh repo, without a working copy, so the
/// whole mesh stays available from it.
#[derive(Debug, Args)]
pub struct ServerArgs {
    #[command(subcommand)]
    command: ServerCommand,
}

#[derive(Debug, Subcommand)]
enum ServerCommand {
    /// Replicate every mesh repo on this machine
    Enable,
    /// Stop replicating the mesh repos
    Disable,
}

/// Runs the `server` command.
pub fn run(args: &ServerArgs, dir: &ConfigDir) -> Result<()> {
    set(dir, matches!(args.command, ServerCommand::Enable))
}

/// Switches server mode through the daemon.
pub fn set(dir: &ConfigDir, enabled: bool) -> Result<()> {
    let request = Request::SetServer { enabled };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::ServerSet = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    let message = if enabled {
        "Server mode enabled: this machine replicates every mesh repo"
    } else {
        "Server mode disabled"
    };
    println!("{}", ui::good(message));
    Ok(())
}

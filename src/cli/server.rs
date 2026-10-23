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
/// whole mesh stays available from it. `jj-mesh clone` on a server adds a
/// workspace to its copy.
#[derive(Debug, Args)]
pub struct ServerArgs {
    #[command(subcommand)]
    command: ServerCommand,
}

#[derive(Debug, Subcommand)]
enum ServerCommand {
    /// Keep a bare repo of every mesh repo on this machine
    Enable,
    /// Stop keeping bare repos, deleting those no workspace uses
    Disable,
}

/// Runs the `server` command.
pub fn run(args: &ServerArgs, dir: &ConfigDir) -> Result<()> {
    set(dir, matches!(args.command, ServerCommand::Enable))
}

/// Switches server mode through the daemon.
pub fn set(dir: &ConfigDir, server: bool) -> Result<()> {
    let request = Request::SetServer { server };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::ServerSet = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    let message = if server {
        "Server mode enabled: this machine keeps a bare repo of every mesh repo"
    } else {
        "Server mode disabled"
    };
    println!("{}", ui::good(message));
    Ok(())
}

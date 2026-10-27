//! `jj-mesh server`: switch server mode on this machine.

use anyhow::{Result, bail};
use clap::{Args, Subcommand};

use super::ui;
use crate::{
    config::{ConfigDir, MeshState},
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
        "Enabled server mode: this machine keeps a bare repo of every mesh repo"
    } else {
        "Disabled server mode"
    };
    ui::success(message);
    if server {
        show_unstored(&MeshState::load(dir)?);
    }
    Ok(())
}

/// Lists the repos registered with their own storage, which server mode
/// leaves where they are.
fn show_unstored(state: &MeshState) {
    let names: Vec<_> = state
        .repos
        .iter()
        .filter(|(_, repo)| !repo.bare)
        .filter_map(|(id, _)| state.repo_name(id))
        .collect();
    if names.is_empty() {
        return;
    }
    ui::info(format_args!("Not in bare repos: {}", names.join(", ")));
    ui::hint("Move one into a bare repo with `jj-mesh repo forget <name>`");
}

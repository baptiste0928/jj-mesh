//! `jj-mesh peer`: manage the paired machines.

mod add;
mod remove;

use clap::{Args, Subcommand};
use color_eyre::eyre::Result;

use crate::config::ConfigDir;

/// Manage the paired machines
///
/// Paired machines have full access to the repositories added to the mesh.
/// After adding a machine, use `jj-mesh repo` to manage the repositories.
#[derive(Debug, Args)]
#[command(arg_required_else_help = true)]
pub struct PeerArgs {
    #[command(subcommand)]
    command: PeerCommand,
}

#[derive(Debug, Subcommand)]
enum PeerCommand {
    Add(add::AddArgs),
    Remove(remove::RemoveArgs),
}

/// Runs the `peer` command.
pub fn run(args: PeerArgs, dir: &ConfigDir) -> Result<()> {
    match args.command {
        PeerCommand::Add(args) => add::run(args, dir),
        PeerCommand::Remove(args) => remove::run(args, dir),
    }
}

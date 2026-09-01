//! `jj-mesh peer`: manage the paired machines.

mod add;
mod remove;
mod rename;

use clap::{Args, Subcommand};
use color_eyre::eyre::Result;

use crate::config::ConfigDir;

/// Manage the machines of the mesh
///
/// Paired machines have full access to the repositories added to the mesh.
/// After adding a machine, use `jj-mesh repo` to manage the repositories.
#[derive(Debug, Args)]
pub struct PeerArgs {
    #[command(subcommand)]
    command: PeerCommand,
}

#[derive(Debug, Subcommand)]
enum PeerCommand {
    Add(add::AddArgs),
    Remove(remove::RemoveArgs),
    Rename(rename::RenameArgs),
}

/// Runs the `peer` command.
pub fn run(args: PeerArgs, dir: &ConfigDir) -> Result<()> {
    match args.command {
        PeerCommand::Add(args) => add::run(args, dir),
        PeerCommand::Remove(args) => remove::run(args, dir),
        PeerCommand::Rename(args) => rename::run(args, dir),
    }
}

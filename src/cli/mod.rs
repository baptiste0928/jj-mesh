//! Command-line interface for `jj-mesh`.
//!
//! Each subcommand lives in its own module, holding its clap arguments and
//! entry point. Networked commands build their own tokio runtime; config-only
//! commands stay synchronous.

mod add;
mod daemon;
mod pair;
mod peers;
mod status;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use color_eyre::eyre::Result;

use crate::config::ConfigDir;

/// Peer-to-peer synchronization of jj repos across personal machines
///
/// Each machine works in its own jj workspace of a shared logical repo, and
/// changes are replicated in the background by syncing git objects and the jj
/// operation log directly between peers.
#[derive(Debug, Parser)]
#[command(name = "jj-mesh", version)]
pub struct Cli {
    /// Custom configuration directory
    ///
    /// Configuration is stored in `$XDG_CONFIG_HOME/jj-mesh` by default.
    #[arg(long, short = 'C', global = true, value_name = "DIR")]
    config_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Add(add::AddArgs),
    Daemon(daemon::DaemonArgs),
    Pair(pair::PairArgs),
    Peers(peers::PeersArgs),
    Status(status::StatusArgs),
}

/// Entry point of the `jj-mesh` CLI
pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let dir = ConfigDir::new(cli.config_dir)?;

    match cli.command {
        Command::Add(args) => add::run(args, &dir),
        Command::Daemon(args) => daemon::run(args, &dir),
        Command::Pair(args) => pair::run(args, &dir),
        Command::Peers(args) => peers::run(args, &dir),
        Command::Status(args) => status::run(args, &dir),
    }
}

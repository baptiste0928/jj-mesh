//! Command-line interface for `jj-mesh`.

use clap::Parser;
use color_eyre::eyre::Result;

/// Peer-to-peer synchronization of jj repos across personal machines.
///
/// Each machine works in its own jj workspace of a shared logical repo, and
/// changes are replicated in the background by syncing git objects and the jj
/// operation log directly between peers.
#[derive(Debug, Parser)]
#[command(name = "jj-mesh", version)]
pub struct Cli {}

/// Entry point of the `jj-mesh` CLI
pub fn run() -> Result<()> {
    let _cli = Cli::parse();
    println!("Hello from jj-mesh!");
    Ok(())
}

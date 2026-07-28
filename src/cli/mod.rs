//! Command-line interface for `jj-mesh`.

mod complete;
mod peer;
mod repo;
mod run_daemon;
mod service;
mod status;
mod ui;

use std::path::PathBuf;

use clap::{CommandFactory as _, Parser, Subcommand, ValueHint};
use color_eyre::eyre::Result;

use crate::config::ConfigDir;

/// Peer-to-peer synchronization of jj repositories
///
/// `jj-mesh` keeps copies of Jujutsu (https://jj-vcs.dev) repositories in
/// sync across your machines. It is similar to `jj workspaces`, but across
/// computers: each machine has its own working copy, and a background daemon
/// replicates commits and the jj operation log directly between paired
/// machines, with no central server.
///
/// Getting started:
///   1. Install the background service:  jj-mesh service install
///   2. Pair with another machine:       jj-mesh peer add
///   3. Put a repo on the mesh:          jj-mesh repo add <PATH>
///   4. Clone it on the other machine:   jj-mesh repo clone <NAME>
///
/// Use `jj-mesh status` to inspect peers, repos, and synchronization status.
#[derive(Debug, Parser)]
#[command(name = "jj-mesh", version, verbatim_doc_comment)]
pub struct Cli {
    /// Custom configuration directory
    ///
    /// Configuration is stored in `~/.config/jj-mesh` by default.
    #[arg(long, short = 'C', global = true, value_name = "DIR", value_hint = ValueHint::DirPath)]
    config_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Repo(repo::RepoArgs),
    Peer(peer::PeerArgs),
    Service(service::ServiceArgs),
    Status(status::StatusArgs),
    // Hidden: this is what the installed service runs; users manage the
    // daemon through `jj-mesh service`.
    #[command(hide = true)]
    RunDaemon(run_daemon::RunDaemonArgs),
}

/// Entry point of the `jj-mesh` CLI
pub fn run() -> Result<()> {
    // Answers shell completion requests (`COMPLETE=<shell> jj-mesh ...`)
    // and exits; a no-op on regular invocations. Must run before anything
    // is parsed or printed.
    clap_complete::CompleteEnv::with_factory(Cli::command).complete();

    let cli = Cli::parse();
    let dir = ConfigDir::new(cli.config_dir)?;

    match cli.command {
        Command::Repo(args) => repo::run(args, &dir),
        Command::Peer(args) => peer::run(args, &dir),
        Command::Service(args) => service::run(args, &dir),
        Command::Status(args) => status::run(args, &dir),
        Command::RunDaemon(args) => run_daemon::run(args, &dir),
    }
}

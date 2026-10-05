//! Command-line interface for `jj-mesh`.

mod complete;
mod logs;
mod peer;
mod repo;
mod run_daemon;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod service;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod setup;
mod status;
mod ui;

use std::path::PathBuf;

use clap::{CommandFactory as _, Parser, Subcommand, ValueHint};
use color_eyre::eyre::Result;

use crate::config::{ConfigDir, MeshState};

/// Peer-to-peer synchronization of jj repositories
///
/// `jj-mesh` keeps copies of Jujutsu (https://jj-vcs.dev) repositories in
/// sync across your machines. It is similar to `jj workspaces`, but across
/// computers: each machine has its own working copy, and a background daemon
/// replicates commits and the jj operation log directly between paired
/// machines, with no central server.
///
/// Getting started:
///   1. Set up and get a pairing ticket:  jj-mesh setup
///   2. Pair the other machine:           jj-mesh setup <TICKET>
///   3. Put a repo on the mesh:           jj-mesh add <PATH>
///   4. Clone it on the other machine:    jj-mesh clone <NAME>
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
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Setup(setup::SetupArgs),
    #[command(about = "Clone a repo from another machine (alias for `repo clone`)")]
    Clone(repo::CloneArgs),
    #[command(about = "Add a repo to the mesh (alias for `repo add`)")]
    Add(repo::AddArgs),
    Repo(repo::RepoArgs),
    Peer(peer::PeerArgs),
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Service(service::ServiceArgs),
    Status(status::StatusArgs),
    Logs(logs::LogsArgs),
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
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        Command::Setup(args) => setup::run(args, &dir),
        Command::Clone(args) => repo::clone(args, &dir),
        Command::Add(args) => repo::add(args, &dir),
        Command::Repo(args) => repo::run(args, &dir),
        Command::Peer(args) => peer::run(args, &dir),
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        Command::Service(args) => service::run(args, &dir),
        Command::Status(args) => status::run(args, &dir),
        Command::Logs(args) => logs::run(&args, &dir),
        Command::RunDaemon(args) => run_daemon::run(args, &dir),
    }
}

/// Prints a top-level CLI error to stderr in the shared palette. The
/// expected "daemon not running" case prints as a plain message, without
/// the error prefix.
pub fn report_error(err: &color_eyre::Report) {
    let message = if err.is::<crate::daemon::control::DaemonNotRunning>() {
        format!("{err:#}")
    } else {
        format!("Error: {err:#}")
    };
    eprintln!("{}", ui::bad(message).for_stderr());
}

/// This machine's name in the mesh, the default workspace name in synced
/// repos.
fn machine_name(dir: &ConfigDir) -> Result<String> {
    Ok(MeshState::load(dir)?.machine.name)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory as _;

    use super::Cli;

    #[test]
    fn cli_is_well_formed() {
        Cli::command().debug_assert();
    }
}

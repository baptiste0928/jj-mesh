//! `jj-mesh repo remove`: retire a repo from the whole mesh.

use std::io::{BufRead as _, IsTerminal as _};

use clap::Args;
use clap_complete::ArgValueCandidates;
use color_eyre::eyre::{Result, bail};

use crate::{
    cli::{complete, ui},
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Remove a repo from the whole mesh
///
/// Every machine stops synchronizing the repo. The files stay untouched.
///
/// If you want to stop synchronizing on this machine only, use `jj-mesh repo
/// forget` instead.
#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// Name of the repo in the mesh
    #[arg(add = ArgValueCandidates::new(complete::mesh_repos))]
    name: String,

    /// Skip the confirmation prompt
    #[arg(long, short = 'y')]
    yes: bool,
}

/// Runs the `repo remove` command.
pub fn run(args: RemoveArgs, dir: &ConfigDir) -> Result<()> {
    let RemoveArgs { name, yes } = args;
    if !yes && !confirm(&name)? {
        println!("Aborted");
        return Ok(());
    }

    let request = Request::RemoveRepo { name: name.clone() };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::RepoRemoved { was_local } = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!(
        "{}",
        ui::good(format_args!("Removed repo `{name}` from the mesh"))
    );
    if was_local {
        println!("Files on this machine are left untouched.");
    }
    Ok(())
}

/// Asks the user to confirm the mesh-wide removal. The prompt goes to
/// stderr, so it stays visible when stdout is redirected.
fn confirm(name: &str) -> Result<bool> {
    if !std::io::stdin().is_terminal() {
        bail!("pass --yes to confirm removal of `{name}`");
    }

    eprint!("Remove `{name}` from every machine on the mesh? [y/N] ");
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;

    let answer = answer.trim();
    Ok(answer.eq_ignore_ascii_case("y") || answer.eq_ignore_ascii_case("yes"))
}

//! `jj-mesh repo`: manage the repos synchronized on the mesh.

mod add;
mod clone;
mod forget;
mod remove;

use std::path::Path;

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, WrapErr as _, ensure};

use crate::config::ConfigDir;

/// Manage the repos synchronized on the mesh
///
/// Registered repos will be synced on the mesh and available for other
/// machines to clone. Use `jj-mesh status` to list the repos currently
/// available on the mesh.
#[derive(Debug, Args)]
pub struct RepoArgs {
    #[command(subcommand)]
    command: RepoCommand,
}

#[derive(Debug, Subcommand)]
enum RepoCommand {
    Add(add::AddArgs),
    Clone(clone::CloneArgs),
    Forget(forget::ForgetArgs),
    Remove(remove::RemoveArgs),
}

/// Runs the `repo` command.
pub fn run(args: RepoArgs, dir: &ConfigDir) -> Result<()> {
    match args.command {
        RepoCommand::Add(args) => add::run(args, dir),
        RepoCommand::Clone(args) => clone::run(args, dir),
        RepoCommand::Forget(args) => forget::run(args, dir),
        RepoCommand::Remove(args) => remove::run(args, dir),
    }
}

/// Runs a jj command, surfacing its stderr on failure.
fn jj(dir: Option<&Path>, args: &[&str]) -> Result<()> {
    let mut command = std::process::Command::new(crate::repo::jj_bin());
    if let Some(dir) = dir {
        command.current_dir(dir);
    }
    let out = command.args(args).output().wrap_err("cannot run jj")?;
    ensure!(
        out.status.success(),
        "jj {} failed:\n{}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr),
    );
    Ok(())
}

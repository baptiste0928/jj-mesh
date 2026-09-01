//! `jj-mesh peer rename`: rename this machine in the mesh.

use clap::Args;
use color_eyre::eyre::{Result, bail};

use crate::{
    cli::ui,
    config::ConfigDir,
    daemon::control::{self, Request, Response},
};

/// Rename this machine
///
/// Machines are named after their host by default. Updating the machine name
/// will not rename the workspaces for already cloned repos.
#[derive(Debug, Args)]
pub struct RenameArgs {
    /// New name of this machine
    name: String,
}

/// Runs the `peer rename` command.
pub fn run(args: RenameArgs, dir: &ConfigDir) -> Result<()> {
    let RenameArgs { name } = args;
    let request = Request::RenameMachine { name: name.clone() };
    let response = control::request_blocking(dir, &request, control::MUTATE_WAIT)?;
    let Response::MachineRenamed = response else {
        bail!("unexpected response from the daemon: {response:?}");
    };

    println!("{}", ui::good(format_args!("This machine is now `{name}`")));
    Ok(())
}

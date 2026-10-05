//! `jj-mesh service`: manage the daemon as a user service.

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::{Args, Subcommand};
use color_eyre::eyre::{OptionExt as _, Result, WrapErr as _, bail, ensure, eyre};

use super::ui;
use crate::{
    config::ConfigDir,
    service::{Service, Spec, Status},
};

/// How long `restart` waits for the service to reach the running state:
/// launchd restarts asynchronously.
const RESTART_WAIT: Duration = Duration::from_secs(10);

/// Delay after a verified start before re-checking that the service is
/// still up, to catch a daemon that exits right away (bad binary, another
/// instance holding the lock).
const RESTART_SETTLE: Duration = Duration::from_secs(1);

/// Install and manage the background service
///
/// `jj-mesh` requires a background daemon to run to keep connection with the
/// mesh and sync changes. We provide commands to install and manage it as
/// a user service (with systemd on Linux, launchd on macOS).
///
/// If you wish to manage the service manually, you should run the daemon with
/// `jj-mesh run-daemon`.
#[derive(Debug, Args)]
pub struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Install and start the daemon service
    Install {
        /// Program path written into the service (defaults to current binary)
        ///
        /// If the binary location is not stable across updates, you can pass
        /// another stable path here. For example, you'll want to use
        /// `~/.nix-profile/bin/jj-mesh` if you are using Nix.
        #[arg(long, value_name = "PATH")]
        program: Option<PathBuf>,
        /// jj binary the daemon runs (sets `JJ_BIN` in the service)
        #[arg(long, value_name = "PATH")]
        jj_bin: Option<PathBuf>,
    },
    /// Stop and remove the daemon service
    Uninstall,
    /// Start the installed daemon service
    Start,
    /// Stop the daemon service
    Stop,
    /// Restart the daemon service
    Restart,
}

/// Runs the `service` command.
pub fn run(args: ServiceArgs, dir: &ConfigDir) -> Result<()> {
    match args.command {
        ServiceCommand::Install { program, jj_bin } => install(dir, program, jj_bin.as_deref())?,
        ServiceCommand::Uninstall => {
            let service = installed()?;
            ensure_ours(&service)?;
            service.uninstall()?;
            println!("{}", ui::good("Stopped the service"));
            println!(
                "{}",
                ui::good(format_args!("Removed {}", service.path().display()))
            );
        }
        ServiceCommand::Start => {
            installed()?.start().wrap_err("cannot start the service")?;
            println!("{}", ui::good("jj-mesh daemon service started"));
        }
        ServiceCommand::Stop => {
            installed()?.stop().wrap_err("cannot stop the service")?;
            println!("{}", ui::good("jj-mesh daemon service stopped"));
        }
        ServiceCommand::Restart => {
            restart(&installed()?)?;
            println!("{}", ui::good("jj-mesh daemon service restarted"));
        }
    }
    Ok(())
}

/// The installed service.
fn installed() -> Result<Service> {
    Service::find()?.ok_or_eyre("the service is not installed: run `jj-mesh service install`")
}

/// Refuses to replace or remove a service another program manages.
fn ensure_ours(service: &Service) -> Result<()> {
    ensure!(
        !service.is_external(),
        "{} is managed by another program (such as Home Manager): \
         update that configuration instead",
        service.path().display(),
    );
    Ok(())
}

/// Installs the service and (re)starts it.
fn install(dir: &ConfigDir, program: Option<PathBuf>, jj_bin: Option<&Path>) -> Result<()> {
    if let Some(service) = Service::find()? {
        ensure_ours(&service)?;
    }
    let service = Service::ours()?;

    let program = match program {
        Some(program) => program,
        None => std::env::current_exe().wrap_err("cannot resolve the jj-mesh binary path")?,
    };
    let mut command = vec![service_path("the program path", &program)?];
    // A custom config directory is baked into the service (useful for side
    // setups); the default is resolved by the daemon at startup.
    if dir.is_custom() {
        command.push("--config-dir".to_owned());
        command.push(service_path("the config directory", dir.path())?);
    }
    command.push("run-daemon".to_owned());

    let mut env = vec![("RUST_LOG".to_owned(), "jj_mesh=info".to_owned())];
    if let Some(jj_bin) = jj_bin {
        // Absolute rather than resolved: `jj` would resolve to `./jj`.
        ensure!(jj_bin.is_absolute(), "the jj binary path must be absolute");
        env.push((
            "JJ_BIN".to_owned(),
            service_path("the jj binary path", jj_bin)?,
        ));
    }

    service
        .install(&Spec { command, env })
        .wrap_err("cannot install the service")?;
    println!("Created {}", service.path().display());
    println!("{}", ui::good("Service enabled and started"));
    Ok(())
}

/// Restarts the service, verifying it comes up and stays up.
fn restart(service: &Service) -> Result<()> {
    service.restart().wrap_err("cannot restart the service")?;

    let deadline = Instant::now() + RESTART_WAIT;
    while service.status()? != Status::Running {
        if Instant::now() >= deadline {
            bail!("the service did not start; check its logs");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    std::thread::sleep(RESTART_SETTLE);
    ensure!(
        service.status()? == Status::Running,
        "the service started but died; check its logs",
    );
    Ok(())
}

/// A path as written into the service definition: absolute, since the
/// daemon runs from another directory, and UTF-8.
fn service_path(what: &str, path: &Path) -> Result<String> {
    let path = std::path::absolute(path)
        .wrap_err_with(|| format!("cannot resolve {what} {}", path.display()))?;
    path.into_os_string()
        .into_string()
        .map_err(|path| eyre!("{what} is not valid UTF-8: {}", Path::new(&path).display()))
}

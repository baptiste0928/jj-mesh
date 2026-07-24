//! `jj-mesh service`: manage the daemon as a user service.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, WrapErr as _, ensure};
use service_manager::{
    RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager, ServiceStartCtx,
    ServiceStopCtx, ServiceUninstallCtx,
};

use crate::config::ConfigDir;

/// Seconds before the service is restarted after a failure.
const RESTART_DELAY_SECS: u32 = 5;

/// Manage the daemon as a user service
///
/// Installs a service running `jj-mesh daemon` (a systemd user unit on
/// Linux, a launchd agent on macOS), so the mesh stays connected in the
/// background and across reboots.
#[derive(Debug, Args)]
pub struct ServiceArgs {
    #[command(subcommand)]
    command: ServiceCommand,
}

#[derive(Debug, Subcommand)]
enum ServiceCommand {
    /// Install and start the daemon service
    Install {
        /// Program path written into the service (defaults to the running
        /// jj-mesh binary)
        ///
        /// Pass a path that stays stable across updates when the install
        /// location changes, e.g. `~/.nix-profile/bin/jj-mesh`.
        #[arg(long, value_name = "PATH")]
        program: Option<PathBuf>,
    },
    /// Stop and remove the daemon service
    Uninstall,
    /// Start the installed daemon service
    Start,
    /// Stop the daemon service
    Stop,
}

/// Runs the `service` command.
pub fn run(args: ServiceArgs, dir: &ConfigDir) -> Result<()> {
    let label: ServiceLabel = "jj-mesh".parse()?;
    let mut manager =
        <dyn ServiceManager>::native().wrap_err("no supported service manager on this system")?;
    manager
        .set_level(ServiceLevel::User)
        .wrap_err("user services are not supported on this system")?;

    match args.command {
        ServiceCommand::Install { program } => install(&*manager, label, dir, program),
        ServiceCommand::Uninstall => uninstall(&*manager, label),
        ServiceCommand::Start => {
            manager
                .start(ServiceStartCtx { label })
                .wrap_err("cannot start the service")?;
            println!("jj-mesh daemon service started");
            Ok(())
        }
        ServiceCommand::Stop => {
            manager
                .stop(ServiceStopCtx { label })
                .wrap_err("cannot stop the service")?;
            println!("jj-mesh daemon service stopped");
            Ok(())
        }
    }
}

/// Installs the service and starts it.
fn install(
    manager: &dyn ServiceManager,
    label: ServiceLabel,
    dir: &ConfigDir,
    program: Option<PathBuf>,
) -> Result<()> {
    let program = match program {
        Some(program) => program,
        None => std::env::current_exe().wrap_err("cannot resolve the jj-mesh binary path")?,
    };
    validate_service_path("the program path", &program)?;

    // A custom config directory is baked into the service (useful for side
    // setups); the default is resolved by the daemon at startup.
    let mut args = Vec::new();
    if dir.is_custom() {
        validate_service_path("the config directory", dir.path())?;
        args.push(OsString::from("--config-dir"));
        args.push(dir.path().into());
    }
    args.push(OsString::from("daemon"));

    let command = std::iter::once(program.as_os_str())
        .chain(args.iter().map(OsString::as_os_str))
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");

    manager
        .install(ServiceInstallCtx {
            label: label.clone(),
            program,
            args,
            contents: None,
            username: None,
            working_directory: None,
            environment: Some(vec![("RUST_LOG".to_owned(), "jj_mesh=info".to_owned())]),
            autostart: true,
            restart_policy: RestartPolicy::OnFailure {
                delay_secs: Some(RESTART_DELAY_SECS),
                max_retries: None,
                reset_after_secs: None,
            },
        })
        .wrap_err("cannot install the service")?;

    println!("Created {}", service_file(&label)?.display());
    println!("  command: {command}");

    manager
        .start(ServiceStartCtx { label })
        .wrap_err("installed the service, but cannot start it")?;

    println!("Enabled and started the service");
    #[cfg(target_os = "linux")]
    {
        println!("View its logs with: journalctl --user -u jj-mesh");
        println!(
            "Note: on machines without a graphical session, run `loginctl enable-linger` once \
             so the service also runs while logged out"
        );
    }

    Ok(())
}

/// Stops the service if running, then removes it.
fn uninstall(manager: &dyn ServiceManager, label: ServiceLabel) -> Result<()> {
    // Best effort: stopping fails harmlessly when the service is not running.
    let _ = manager.stop(ServiceStopCtx {
        label: label.clone(),
    });

    let file = service_file(&label)?;
    manager
        .uninstall(ServiceUninstallCtx { label })
        .wrap_err("cannot uninstall the service")?;

    println!("Stopped the service");
    println!("Removed {}", file.display());
    Ok(())
}

/// Checks that a path can be embedded in a service definition.
///
/// `service-manager` writes systemd units without any quoting, so a path
/// with whitespace silently corrupts `ExecStart`, and control characters
/// (newlines) could inject arbitrary unit directives. systemd also refuses
/// non-absolute paths outside its fixed `/usr` search path.
fn validate_service_path(what: &str, path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "{what} must be absolute: {path:?}");

    let text = path.as_os_str().to_string_lossy();
    ensure!(
        !text.chars().any(|c| c.is_whitespace() || c.is_control()),
        "{what} must not contain whitespace or control characters, \
         as it is written unquoted into the service definition: {path:?}",
    );

    Ok(())
}

/// Path of the service definition file, where `service-manager` puts user
/// services on each platform.
fn service_file(label: &ServiceLabel) -> Result<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        Ok(service_manager::systemd_user_dir_path()?
            .join(format!("{}.service", label.to_script_name())))
    }
    #[cfg(target_os = "macos")]
    {
        Ok(etcetera::home_dir()
            .wrap_err("cannot determine the home directory")?
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{label}.plist")))
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = label;
        color_eyre::eyre::bail!("unsupported platform for user services");
    }
}

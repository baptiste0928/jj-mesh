//! `jj-mesh service`: manage the daemon as a user service.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};
use service_manager::{
    RestartPolicy, ServiceInstallCtx, ServiceLabel, ServiceLevel, ServiceManager, ServiceStartCtx,
    ServiceStatus, ServiceStatusCtx, ServiceStopCtx, ServiceUninstallCtx,
};

use super::ui;
use crate::config::ConfigDir;

/// Seconds before the service is restarted after a failure.
const RESTART_DELAY_SECS: u32 = 5;

/// How long `restart` waits for the service to leave or reach the running
/// state. Stopping is asynchronous on launchd, and starting reports
/// success as soon as the process forks, so both transitions are verified
/// by polling rather than trusted.
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
    let label: ServiceLabel = "jj-mesh".parse()?;
    let mut manager =
        <dyn ServiceManager>::native().wrap_err("no supported service manager on this system")?;
    manager
        .set_level(ServiceLevel::User)
        .wrap_err("user services are not supported on this system")?;

    match args.command {
        ServiceCommand::Install { program, jj_bin } => {
            install(&*manager, label, dir, program, jj_bin.as_deref())
        }
        ServiceCommand::Uninstall => uninstall(&*manager, label),
        ServiceCommand::Start => {
            manager
                .start(ServiceStartCtx { label })
                .wrap_err("cannot start the service")?;
            println!("{}", ui::good("jj-mesh daemon service started"));
            Ok(())
        }
        ServiceCommand::Stop => {
            manager
                .stop(ServiceStopCtx { label })
                .wrap_err("cannot stop the service")?;
            println!("{}", ui::good("jj-mesh daemon service stopped"));
            Ok(())
        }
        ServiceCommand::Restart => restart(&*manager, &label),
    }
}

/// Stops and starts the service, verifying both transitions by polling
/// (see [`RESTART_WAIT`]).
fn restart(manager: &dyn ServiceManager, label: &ServiceLabel) -> Result<()> {
    stop_quietly(manager, label);
    wait_status(manager, label, "stop", |status| {
        status != &ServiceStatus::Running
    })?;

    manager
        .start(ServiceStartCtx {
            label: label.clone(),
        })
        .wrap_err("cannot start the service")?;
    wait_status(manager, label, "start", |status| {
        status == &ServiceStatus::Running
    })?;
    std::thread::sleep(RESTART_SETTLE);
    let status = manager
        .status(ServiceStatusCtx {
            label: label.clone(),
        })
        .wrap_err("cannot query the service status")?;
    ensure!(
        status == ServiceStatus::Running,
        "the service started but died (status: {status:?}); check its logs",
    );

    println!("{}", ui::good("jj-mesh daemon service restarted"));
    Ok(())
}

/// Stops the service, ignoring failures: stopping a service that is not
/// running errors harmlessly, and the callers verify the state they need
/// afterwards.
fn stop_quietly(manager: &dyn ServiceManager, label: &ServiceLabel) {
    let _ = manager.stop(ServiceStopCtx {
        label: label.clone(),
    });
}

/// Polls the service status until `reached` accepts it, bounded by
/// [`RESTART_WAIT`]; `what` names the awaited transition in errors.
fn wait_status(
    manager: &dyn ServiceManager,
    label: &ServiceLabel,
    what: &str,
    reached: impl Fn(&ServiceStatus) -> bool,
) -> Result<()> {
    let deadline = Instant::now() + RESTART_WAIT;
    loop {
        let status = manager
            .status(ServiceStatusCtx {
                label: label.clone(),
            })
            .wrap_err("cannot query the service status")?;
        if reached(&status) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("the service did not {what} (status: {status:?})");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Installs the service and starts it.
fn install(
    manager: &dyn ServiceManager,
    label: ServiceLabel,
    dir: &ConfigDir,
    program: Option<PathBuf>,
    jj_bin: Option<&Path>,
) -> Result<()> {
    let program = match program {
        Some(program) => program,
        None => std::env::current_exe().wrap_err("cannot resolve the jj-mesh binary path")?,
    };
    validate_service_path("the program path", &program)?;

    let mut environment = vec![("RUST_LOG".to_owned(), "jj_mesh=info".to_owned())];
    if let Some(jj_bin) = jj_bin {
        // Environment values are written into the service definition just
        // as unescaped as the paths, so they get the same restrictions.
        validate_service_path("the jj binary path", jj_bin)?;
        environment.push(("JJ_BIN".to_owned(), jj_bin.to_string_lossy().into_owned()));
    }

    // A custom config directory is baked into the service (useful for side
    // setups); the default is resolved by the daemon at startup.
    let mut args = Vec::new();
    if dir.is_custom() {
        validate_service_path("the config directory", dir.path())?;
        args.push(OsString::from("--config-dir"));
        args.push(dir.path().into());
    }
    args.push(OsString::from("run-daemon"));

    manager
        .install(ServiceInstallCtx {
            label: label.clone(),
            program,
            args,
            contents: None,
            username: None,
            working_directory: None,
            environment: Some(environment),
            autostart: true,
            restart_policy: RestartPolicy::OnFailure {
                delay_secs: Some(RESTART_DELAY_SECS),
                max_retries: None,
                reset_after_secs: None,
            },
        })
        .wrap_err("cannot install the service")?;

    // A reinstall rewrites the unit file, but systemd keeps serving the
    // cached one (with a possibly stale ExecStart) until told to reload;
    // `service-manager` never does. Best effort: a failure only means the
    // cache heals on the next reboot or manual reload.
    #[cfg(target_os = "linux")]
    let _ = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status();

    println!("Created {}", service_file(&label)?.display());

    manager
        .start(ServiceStartCtx { label })
        .wrap_err("installed the service, but cannot start it")?;

    println!("{}", ui::good("Service enabled and started"));

    Ok(())
}

/// Stops the service if running, then removes it.
fn uninstall(manager: &dyn ServiceManager, label: ServiceLabel) -> Result<()> {
    stop_quietly(manager, &label);

    let file = service_file(&label)?;
    manager
        .uninstall(ServiceUninstallCtx { label })
        .wrap_err("cannot uninstall the service")?;

    println!("{}", ui::good("Stopped the service"));
    println!("{}", ui::good(format_args!("Removed {}", file.display())));
    Ok(())
}

/// Checks that a path can be embedded in a service definition.
///
/// `service-manager` writes systemd units without any quoting or escaping,
/// so a path with whitespace silently corrupts `ExecStart`, control
/// characters (newlines) could inject arbitrary unit directives, `%` is
/// rewritten by systemd's specifier expansion, and the XML metacharacters
/// would break the launchd plist. systemd also refuses non-absolute paths
/// outside its fixed `/usr` search path.
fn validate_service_path(what: &str, path: &Path) -> Result<()> {
    ensure!(path.is_absolute(), "{what} must be absolute: {path:?}");

    let text = path.as_os_str().to_string_lossy();
    ensure!(
        !text
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || "%<>&\"'".contains(c)),
        "{what} must not contain whitespace, control characters or any of \
         `%<>&\"'`, as it is written unescaped into the service definition: {path:?}",
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
        bail!("unsupported platform for user services");
    }
}

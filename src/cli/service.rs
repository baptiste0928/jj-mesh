//! `jj-mesh service`: manage the daemon as a user service.

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use clap::{Args, Subcommand};

use super::ui;
use crate::{
    config::ConfigDir,
    daemon::control::{self, ControlClient, Request, Response},
    hint::hinted,
    repo::jj_bin,
    service::{Service, Spec, Status, which},
};

/// How long a started service may take to answer requests: launchd starts
/// services asynchronously, and the daemon binds its network endpoint
/// first.
const START_WAIT: Duration = Duration::from_secs(30);

/// Delay after a daemon answered before checking that the service is still
/// up, to catch a service that exits right away while another daemon
/// answers (another instance holding the lock).
const START_SETTLE: Duration = Duration::from_secs(1);

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
        /// jj binary the daemon runs (defaults to jj on PATH)
        ///
        /// The path is written as `JJ_BIN` into the service, whose own PATH
        /// is minimal.
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
        ServiceCommand::Install { program, jj_bin } => {
            install(dir, program, jj_bin)?;
        }
        ServiceCommand::Uninstall => {
            let service = installed()?;
            ensure_ours(&service)?;
            service.uninstall()?;
            ui::success(format_args!(
                "Uninstalled the service, removing {}",
                ui::path(service.path())
            ));
        }
        ServiceCommand::Start => {
            start(&installed()?, dir)?;
        }
        ServiceCommand::Stop => {
            installed()?.stop().context("cannot stop the service")?;
            ui::success("Stopped the service");
        }
        ServiceCommand::Restart => {
            let service = installed()?;
            service.restart().context("cannot restart the service")?;
            wait_started(&service, dir)?;
            ui::success("Restarted the service");
        }
    }
    Ok(())
}

/// Makes sure a daemon is running, installing or starting the service
/// once the user agrees, and returns its status.
pub(super) fn ensure_running(dir: &ConfigDir) -> Result<control::Status> {
    if let Some(status) = status(dir, START_WAIT)? {
        return Ok(status);
    }
    match Service::find()? {
        None => {
            ensure!(
                ui::confirm("Install the background service?", true)?.unwrap_or(false),
                hinted(
                    "jj-mesh needs its daemon",
                    "Install it with `jj-mesh service install`"
                )
            );
            install(dir, None, None)
        }
        Some(service) if service.status()? == Status::Running => wait_ready(dir),
        Some(service) => {
            ensure!(
                ui::confirm("The background service is stopped. Start it?", true)?.unwrap_or(false),
                hinted(
                    "jj-mesh needs its daemon",
                    "Start it with `jj-mesh service start`"
                )
            );
            start(&service, dir)
        }
    }
}

/// The installed service.
fn installed() -> Result<Service> {
    Service::find()?.ok_or_else(|| {
        hinted(
            "the service is not installed",
            "Install it with `jj-mesh service install`",
        )
    })
}

/// Refuses to replace or remove a service another program manages.
fn ensure_ours(service: &Service) -> Result<()> {
    ensure!(
        !service.is_external(),
        hinted(
            format!(
                "{} is managed by another program (such as Home Manager or NixOS)",
                ui::path(service.path()),
            ),
            "Update that configuration instead"
        )
    );
    Ok(())
}

/// Installs the service and (re)starts it.
fn install(
    dir: &ConfigDir,
    program: Option<PathBuf>,
    jj: Option<PathBuf>,
) -> Result<control::Status> {
    let installed = Service::find()?;
    if let Some(service) = &installed {
        ensure_ours(service)?;
    }
    // The service could not take the daemon lock: it would crash-loop
    // while the other daemon answers.
    let running = installed.is_some_and(|s| s.status().is_ok_and(|s| s == Status::Running));
    ensure!(
        running || matches!(control::block_on(ControlClient::connect(dir)), Ok(None)),
        hinted(
            "a jj-mesh daemon already runs outside the service",
            "Stop it, then retry"
        )
    );
    let service = Service::ours()?;

    let program = match program {
        Some(program) => program,
        None => current_program()?,
    };
    let mut command = vec![service_path("the program path", &program)?];
    // A custom config directory is baked into the service (useful for side
    // setups); the default is resolved by the daemon at startup.
    if dir.is_custom() {
        command.push("--config-dir".to_owned());
        command.push(service_path("the config directory", dir.path())?);
    }
    command.push("run-daemon".to_owned());

    let jj = jj.unwrap_or_else(|| jj_bin().into());
    let jj = which(&jj, &search_path()).ok_or_else(|| {
        hinted(
            format!("cannot find {}", ui::path(&jj)),
            "Pass its path with `--jj-bin`",
        )
    })?;
    let env = vec![
        ("RUST_LOG".to_owned(), "jj_mesh=info".to_owned()),
        (
            "JJ_BIN".to_owned(),
            service_path("the jj binary path", &jj)?,
        ),
    ];

    service
        .install(&Spec { command, env })
        .context("cannot install the service")?;
    ui::info(format_args!("Created {}", ui::path(service.path())));
    ui::info(format_args!("The daemon runs {}", ui::path(&jj)));
    let status = wait_started(&service, dir)?;
    ui::success("Installed and started the service");
    Ok(status)
}

/// This binary as the shell finds it on PATH, so a profile symlink
/// survives updates, or its resolved path otherwise.
fn current_program() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("cannot resolve the jj-mesh binary path")?;
    let same =
        |path: &PathBuf| std::fs::canonicalize(path).ok() == std::fs::canonicalize(&exe).ok();
    Ok(which(Path::new("jj-mesh"), &search_path())
        .filter(same)
        .unwrap_or(exe))
}

/// This shell's PATH.
fn search_path() -> std::ffi::OsString {
    std::env::var_os("PATH").unwrap_or_default()
}

/// Starts the service and waits for its daemon.
fn start(service: &Service, dir: &ConfigDir) -> Result<control::Status> {
    service.start().context("cannot start the service")?;
    let status = wait_started(service, dir)?;
    ui::success("Started the service");
    Ok(status)
}

/// Waits for the daemon of a just started service, then checks that the
/// service itself stayed up.
fn wait_started(service: &Service, dir: &ConfigDir) -> Result<control::Status> {
    let status = wait_ready(dir)?;
    std::thread::sleep(START_SETTLE);
    ensure!(
        service.status()? == Status::Running,
        hinted("the service started but died", "Check the service logs")
    );
    Ok(status)
}

/// Waits for a daemon of this build to answer, for up to [`START_WAIT`].
fn wait_ready(dir: &ConfigDir) -> Result<control::Status> {
    let deadline = Instant::now() + START_WAIT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if let Some(status) = status(dir, left)? {
            return Ok(status);
        }
        if left.is_zero() {
            return Err(hinted(
                format!("no daemon answered for {}", ui::path(dir.path())),
                "Check the service logs, and that the service uses this config directory",
            ));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Asks the daemon for its status, waiting up to `limit` for the answer;
/// `None` when no daemon runs.
fn status(dir: &ConfigDir, limit: Duration) -> Result<Option<control::Status>> {
    control::block_on(async {
        let Some(mut client) = ControlClient::connect(dir).await? else {
            return Ok(None);
        };
        client.send(&Request::Status).await?;
        match client.recv(Some(limit)).await?.into_result()? {
            Response::Status(status) => Ok(Some(status)),
            response => bail!("unexpected response from the daemon: {response:?}"),
        }
    })
}

/// A path as written into the service definition: absolute, since the
/// daemon runs from another directory, and UTF-8.
fn service_path(what: &str, path: &Path) -> Result<String> {
    let path = std::path::absolute(path)
        .with_context(|| format!("cannot resolve {what} {}", path.display()))?;
    path.into_os_string()
        .into_string()
        .map_err(|path| anyhow!("{what} is not valid UTF-8: {}", Path::new(&path).display()))
}

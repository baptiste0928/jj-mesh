//! `jj-mesh run-daemon`: run the sync daemon in the foreground, or detached
//! from the terminal where no service manager runs it.
//!
//! It can be run directly (with `RUST_LOG`) for debugging purposes.

use std::{
    io,
    os::unix::process::CommandExt as _,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result, bail, ensure};
use clap::Args;
use tracing::Subscriber;
use tracing_subscriber::{
    EnvFilter, Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

use super::ui;
use crate::{
    config::ConfigDir,
    daemon::{
        self, LogBuffer,
        control::{self, ControlClient},
    },
    hint::hinted,
};

/// How long a detached daemon may take to start.
const DETACH_WAIT: Duration = Duration::from_secs(30);

/// Hint for a detached daemon that failed to start.
const RUN_HINT: &str = "Run `jj-mesh run-daemon` to see its output";

/// Run the sync daemon
///
/// Use `jj-mesh setup` or `jj-mesh service` to run it as a user service
/// instead, which restarts it on failure and at login.
#[derive(Debug, Args)]
pub struct RunDaemonArgs {
    /// Run in the background, detached from the terminal
    ///
    /// The daemon's output is discarded: read its logs with `jj-mesh logs`.
    #[arg(long)]
    detach: bool,
}

/// Runs the `run-daemon` command.
pub fn run(args: &RunDaemonArgs, dir: &ConfigDir) -> Result<()> {
    if args.detach {
        return detach(dir);
    }
    let filter = EnvFilter::builder()
        .with_default_directive("jj_mesh=info".parse()?)
        .from_env_lossy();
    let logs = LogBuffer::default();
    subscriber(filter, &logs).init();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(daemon::run(dir, logs))
}

/// Runs the daemon in a new session, so closing the terminal does not
/// hang it up, and waits until it answers.
fn detach(dir: &ConfigDir) -> Result<()> {
    ensure!(
        control::block_on(ControlClient::connect(dir))?.is_none(),
        hinted(
            "a jj-mesh daemon already runs",
            "Check it with `jj-mesh status`"
        )
    );
    let exe = std::env::current_exe().context("cannot resolve the jj-mesh binary path")?;
    let mut command = Command::new(exe);
    if dir.is_custom() {
        command.arg("--config-dir").arg(dir.path());
    }
    command
        .arg("run-daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: setsid is async-signal-safe.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command.spawn().context("cannot start the daemon")?;

    let deadline = Instant::now() + DETACH_WAIT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let status = control::try_status_blocking(dir, left);
        // A daemon dying while starting fails the query: its exit says why.
        if let Some(exit) = child.try_wait()? {
            bail!(hinted(format!("the daemon exited ({exit})"), RUN_HINT));
        }
        if status?.is_some() {
            break;
        }
        if left.is_zero() {
            let _ = child.kill();
            bail!(hinted(
                format!("the daemon did not start within {}s", DETACH_WAIT.as_secs()),
                RUN_HINT,
            ));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    ui::success(format_args!("Started the daemon (pid {})", child.id()));
    Ok(())
}

/// The daemon's subscriber: `filter` only applies to the output, `logs`
/// always gets the daemon's events.
fn subscriber(filter: EnvFilter, logs: &LogBuffer) -> impl Subscriber + Send + Sync + use<> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter))
        .with(logs.layer())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_filter_spares_logs() {
        let logs = LogBuffer::default();
        let subscriber = subscriber(EnvFilter::new("off"), &logs);
        tracing::subscriber::with_default(subscriber, || {
            let _repo = tracing::info_span!("repo", repo = "r").entered();
            tracing::info!("synced");
        });

        let entries = logs.subscribe().entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].repo.as_deref(), Some("r"));
    }
}

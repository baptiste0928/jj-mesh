//! `jj-mesh run-daemon`: run the sync daemon in the foreground.
//!
//! It can be run directly (with `RUST_LOG`) for debugging purposes.

use clap::Args;
use color_eyre::eyre::Result;
use tracing::Subscriber;
use tracing_subscriber::{
    EnvFilter, Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

use crate::{
    config::ConfigDir,
    daemon::{self, LogBuffer},
};

/// Run the sync daemon in the foreground
#[derive(Debug, Args)]
pub struct RunDaemonArgs {}

/// Runs the `run-daemon` command.
pub fn run(_args: RunDaemonArgs, dir: &ConfigDir) -> Result<()> {
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
        tracing::subscriber::with_default(subscriber, || tracing::info!("synced"));

        assert_eq!(logs.subscribe().entries.len(), 1);
    }
}

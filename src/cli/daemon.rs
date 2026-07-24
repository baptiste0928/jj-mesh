//! `jj-mesh daemon`: run the sync daemon in the foreground.

use clap::Args;
use color_eyre::eyre::Result;

use crate::{config::ConfigDir, daemon};

/// Run the sync daemon
///
/// Maintains connections to the paired machines and synchronizes the
/// registered repos until stopped. Logging is controlled with `RUST_LOG`.
#[derive(Debug, Args)]
pub struct DaemonArgs {}

/// Runs the `daemon` command.
pub fn run(_args: DaemonArgs, dir: &ConfigDir) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("jj_mesh=info".parse()?)
                .from_env_lossy(),
        )
        .init();

    tokio::runtime::Runtime::new()?.block_on(daemon::run(dir))
}

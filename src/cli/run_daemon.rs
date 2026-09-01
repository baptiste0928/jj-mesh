//! `jj-mesh run-daemon`: run the sync daemon in the foreground.
//!
//! It can be run directly (with `RUST_LOG`) for debugging purposes.

use clap::Args;
use color_eyre::eyre::Result;

use crate::{config::ConfigDir, daemon};

/// Run the sync daemon in the foreground
#[derive(Debug, Args)]
pub struct RunDaemonArgs {}

/// Runs the `run-daemon` command.
pub fn run(_args: RunDaemonArgs, dir: &ConfigDir) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("jj_mesh=info".parse()?)
                .from_env_lossy(),
        )
        .init();

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(daemon::run(dir))
}

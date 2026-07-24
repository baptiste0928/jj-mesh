//! `jj-mesh pair`: register another machine as a peer.
//!
//! Pairing always runs through the daemon, which owns the machine-key
//! endpoint: the CLI drives it over the control socket and renders progress.

use std::time::Duration;

use clap::Args;
use color_eyre::eyre::{Result, bail};

use crate::{
    config::ConfigDir,
    daemon::control::{ControlClient, Request, Response},
    net::pair::PairTicket,
};

/// How long to wait for the daemon to issue a ticket (it may first wait for
/// its relay connection to come up).
const TICKET_TIMEOUT: Duration = Duration::from_secs(45);

/// Pair with another machine, registering each other as sync peers
///
/// Run without arguments on one machine to print a pairing ticket, then run
/// with that ticket on the other machine. Both machines add each other to
/// their peer list. The daemon must be running on both machines.
#[derive(Debug, Args)]
pub struct PairArgs {
    /// Pairing ticket printed by `jj-mesh pair` on the other machine
    ///
    /// If omitted, generate a ticket and wait for the other machine.
    ticket: Option<String>,

    /// Name announced to the other machine (defaults to the hostname)
    #[arg(long)]
    name: Option<String>,
}

/// Runs the `pair` command.
pub fn run(args: PairArgs, dir: &ConfigDir) -> Result<()> {
    let name = args
        .name
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned());

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(pair(args.ticket, name, dir))
}

/// Drives the pairing through the daemon.
async fn pair(ticket: Option<String>, name: String, dir: &ConfigDir) -> Result<()> {
    let Some(mut client) = ControlClient::connect(dir).await? else {
        bail!("the jj-mesh daemon is not running; start it with `jj-mesh daemon` first");
    };

    let outcome = if let Some(ticket) = ticket {
        // Parse locally first, for fast feedback on a mangled paste.
        let _: PairTicket = ticket.parse()?;

        println!("Connecting to the pairing host...");
        client.send(&Request::PairJoin { ticket, name }).await?;
        client.recv(None).await?
    } else {
        client.send(&Request::PairHost { name }).await?;
        match client.recv(Some(TICKET_TIMEOUT)).await? {
            Response::PairTicket(ticket) => {
                println!("To pair, run this on the other machine:\n");
                println!("    jj-mesh pair {ticket}\n");
                println!("Waiting for the other machine (Ctrl-C to abort)...");
            }
            Response::Error(err) => bail!("cannot start pairing: {err}"),
            other => bail!("unexpected response from the daemon: {other:?}"),
        }

        // The daemon closes the window if we disconnect, so Ctrl-C
        // aborting this wait cleans up on its own.
        client.recv(None).await?
    };

    match outcome {
        Response::Paired { name, endpoint } => {
            println!("Paired with `{name}` ({endpoint})");
            Ok(())
        }
        Response::Error(err) => bail!("pairing failed: {err}"),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

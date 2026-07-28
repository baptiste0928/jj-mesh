//! `jj-mesh peer add`: register another machine as a peer.
//!
//! Pairing always runs through the daemon, which owns the machine-key
//! endpoint: the CLI drives it over the control socket. Hosting only asks
//! the daemon for a ticket and exits; the daemon completes the pairing on
//! its own once the other machine redeems the ticket.

use std::time::Duration;

use clap::Args;
use color_eyre::eyre::{Result, bail};

use crate::{
    config::ConfigDir,
    daemon::control::{ControlClient, PAIR_TICKET_TTL, Request, Response},
    net::pair::PairTicket,
};

/// How long to wait for the daemon to issue a ticket (it may first wait for
/// its relay connection to come up).
const TICKET_TIMEOUT: Duration = Duration::from_secs(45);

/// Pair with another machine, adding it to the mesh
///
/// Run this command on one machine to print a pairing ticket, then run it again
/// with the ticket on the other machine. Each ticket can only be used once.
///
/// When a machine gets added to the mesh, it gets access to all synced
/// repositories and can sync from any of the machines already in the mesh.
#[derive(Debug, Args)]
pub struct AddArgs {
    /// Pairing ticket printed by `jj-mesh peer add` on the other machine
    ///
    /// If omitted, a ticket will be generated. The ticket can be once, and
    /// expires after 3 minutes.
    ticket: Option<String>,

    /// Name announced to the other machine (defaults to the hostname)
    #[arg(long)]
    name: Option<String>,
}

/// Runs the `peer add` command.
pub fn run(args: AddArgs, dir: &ConfigDir) -> Result<()> {
    let name = args
        .name
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned());

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(pair(args.ticket, name, dir))
}

/// Dispatches to the hosting or joining side of the pairing.
async fn pair(ticket: Option<String>, name: String, dir: &ConfigDir) -> Result<()> {
    let Some(mut client) = ControlClient::connect(dir).await? else {
        bail!(
            "the jj-mesh daemon is not running; start it with `jj-mesh service \
             start` (or set it up with `jj-mesh service install`)"
        );
    };

    match ticket {
        Some(ticket) => join(&mut client, ticket, name).await,
        None => host(&mut client, name).await,
    }
}

/// Asks the daemon for a fresh pairing ticket and prints it. The daemon
/// finishes the pairing on its own, so this returns right away.
async fn host(client: &mut ControlClient, name: String) -> Result<()> {
    client.send(&Request::PairHost { name }).await?;

    match client.recv(Some(TICKET_TIMEOUT)).await? {
        Response::PairTicket(ticket) => {
            println!("To pair, run this on the other machine:\n");
            println!("    jj-mesh peer add {ticket}\n");
            println!(
                "The ticket is valid for {} minutes, for a single pairing; \
                 running `jj-mesh peer add` again replaces it. The new peer \
                 will show up in `jj-mesh status`.",
                PAIR_TICKET_TTL.as_secs() / 60,
            );
            Ok(())
        }
        Response::Error(err) => bail!("cannot start pairing: {err}"),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

/// Joins a pairing hosted by another machine, waiting for the outcome.
async fn join(client: &mut ControlClient, ticket: String, name: String) -> Result<()> {
    // Parse locally first, for fast feedback on a mangled paste.
    let _: PairTicket = ticket.parse()?;

    println!("Connecting to the pairing host...");
    client.send(&Request::PairJoin { ticket, name }).await?;

    match client.recv(None).await? {
        Response::Paired { name, endpoint } => {
            println!("Paired with `{name}` ({endpoint})");
            Ok(())
        }
        Response::Error(err) => bail!("pairing failed: {err}"),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

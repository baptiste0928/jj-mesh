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
    cli::ui,
    config::ConfigDir,
    daemon::control::{self, ControlClient, PAIR_TICKET_TTL, Request, Response},
    net::pair::PairTicket,
};

/// How long to wait for the daemon to issue a ticket (it may first wait for
/// its relay connection to come up).
const TICKET_TIMEOUT: Duration = Duration::from_secs(45);

/// Pair with another machine, adding it to the mesh
///
/// Run this command on a machine of the mesh to print a pairing ticket, then
/// run `jj-mesh setup` with the ticket on the other machine. Each ticket can
/// only be used once.
///
/// When a machine gets added to the mesh, it gets access to all synced
/// repositories and can sync from any of the machines already in the mesh.
#[derive(Debug, Args)]
pub struct AddArgs {
    /// Pairing ticket printed on the other machine
    ///
    /// If omitted, a ticket will be generated. The ticket can be used once,
    /// and expires after a few minutes.
    ticket: Option<PairTicket>,
}

/// Runs the `peer add` command.
pub fn run(args: AddArgs, dir: &ConfigDir) -> Result<()> {
    pair(dir, args.ticket)
}

/// Joins with `ticket`, or hosts a pairing and prints its ticket.
pub fn pair(dir: &ConfigDir, ticket: Option<PairTicket>) -> Result<()> {
    control::block_on(async {
        let mut client = ControlClient::connect_required(dir).await?;

        match ticket {
            Some(ticket) => join(&mut client, &ticket).await,
            None => host(&mut client).await,
        }
    })
}

/// Asks the daemon for a fresh pairing ticket and prints it. The daemon
/// finishes the pairing on its own, so this returns right away.
async fn host(client: &mut ControlClient) -> Result<()> {
    client.send(&Request::PairHost).await?;

    match client.recv(Some(TICKET_TIMEOUT)).await? {
        Response::PairTicket(ticket) => {
            println!("Run this on the other machine to pair:\n");
            // `setup` joins from fresh and paired machines alike.
            println!(
                "    {}\n",
                ui::heading(format_args!("jj-mesh setup {ticket}"))
            );
            println!(
                "{}",
                ui::dim(format_args!(
                    "The paired machine will have access to all repos from the mesh. The ticket is valid for {} minutes.",
                    PAIR_TICKET_TTL.as_secs() / 60,
                )),
            );
            Ok(())
        }
        Response::Error(err) => bail!("cannot start pairing: {err}"),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

/// Joins a pairing hosted by another machine, waiting for the outcome.
async fn join(client: &mut ControlClient, ticket: &PairTicket) -> Result<()> {
    println!("Connecting to the pairing host...");
    let ticket = ticket.to_string();
    client.send(&Request::PairJoin { ticket }).await?;

    match client.recv(None).await? {
        Response::Paired { name, .. } => {
            println!("{}", ui::good(format_args!("Paired with `{name}`")));
            Ok(())
        }
        Response::Error(err) => bail!("pairing failed: {err}"),
        other => bail!("unexpected response from the daemon: {other:?}"),
    }
}

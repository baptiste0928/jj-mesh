//! `jj-mesh pair`: register another machine as a peer.

use std::time::Duration;

use clap::Args;
use color_eyre::eyre::{Result, bail, eyre};

use crate::{
    config::{Config, ConfigDir, ConfigEdit, MachineKey, Peer},
    daemon::control,
    net::pair::{PairHost, PairTicket, PairedPeer},
};

/// Time budget for the joiner side, from dialing to completion.
const JOIN_TIMEOUT: Duration = Duration::from_mins(1);

/// Pair with another machine, registering each other as sync peers
///
/// Run without arguments on one machine to print a pairing ticket, then run
/// with that ticket on the other machine. Both machines add each other to
/// their peer list.
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
    let key = MachineKey::from_config(dir)?;
    let config = Config::from_config(dir)?;
    let local_name = args
        .name
        .unwrap_or_else(|| gethostname::gethostname().to_string_lossy().into_owned());

    let runtime = tokio::runtime::Runtime::new()?;

    // Pairing binds the machine-key endpoint, which the daemon already holds
    // when running (daemon-managed pairing is a planned follow-up).
    if runtime.block_on(control::query_status(dir))?.is_some() {
        bail!("a jj-mesh daemon is running on this machine; stop it before pairing");
    }

    let paired = match args.ticket {
        Some(ticket) => {
            let ticket: PairTicket = ticket.parse()?;
            println!("Connecting to the pairing host...");
            runtime.block_on(join(&key, &ticket, &local_name, &config))?
        }
        None => runtime.block_on(host(&key, &local_name, &config))?,
    };

    // Reload the config for editing only now: on the host side, the pairing
    // window may stay open for long.
    let mut edit = ConfigEdit::from_config(dir)?;
    edit.add_peer(
        paired.name.clone(),
        Peer {
            endpoint: paired.endpoint,
        },
    )?;
    edit.save()?;

    println!("Paired with `{}` ({})", paired.name, paired.endpoint);
    Ok(())
}

/// Hosts a pairing exchange: prints a ticket and waits for the other machine.
async fn host(key: &MachineKey, local_name: &str, config: &Config) -> Result<PairedPeer> {
    println!("Binding the pairing endpoint...");
    let host = PairHost::bind(key).await?;

    println!("To pair, run this on the other machine:\n");
    println!("    jj-mesh pair {}\n", host.ticket());
    println!("Waiting for the other machine (Ctrl-C to abort)...");

    host.wait_for_peer(local_name, config).await
}

/// Joins a pairing exchange hosted by the other machine.
async fn join(
    key: &MachineKey,
    ticket: &PairTicket,
    local_name: &str,
    config: &Config,
) -> Result<PairedPeer> {
    let pairing = crate::net::pair::join(key, ticket, local_name, config);

    tokio::time::timeout(JOIN_TIMEOUT, pairing)
        .await
        .map_err(|_| eyre!("pairing timed out after {}s", JOIN_TIMEOUT.as_secs()))?
}

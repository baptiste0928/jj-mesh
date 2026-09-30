//! `jj-mesh logs`: show the daemon's recent events.

use std::io;

use chrono::{DateTime, Local};
use clap::Args;
use color_eyre::eyre::{Result, bail};

use super::ui;
use crate::{
    config::{ConfigDir, sanitize},
    daemon::control::{self, CLIENT_TIMEOUT, ControlClient, LogEntry, LogLevel, Request, Response},
};

/// Show the daemon's recent events
///
/// The daemon keeps its latest events in memory: history starts when it
/// starts.
#[derive(Debug, Args)]
pub struct LogsArgs {
    /// Keep printing new events
    #[arg(long, short)]
    follow: bool,

    /// Only show events of this repo
    #[arg(long, value_name = "NAME")]
    repo: Option<String>,

    /// Only show events of this peer
    #[arg(long, value_name = "NAME")]
    peer: Option<String>,

    /// Number of past events to show
    #[arg(long, short = 'n', value_name = "COUNT")]
    lines: Option<usize>,
}

impl LogsArgs {
    fn matches(&self, entry: &LogEntry) -> bool {
        let accepts =
            |filter: &Option<String>, value: &Option<String>| filter.is_none() || filter == value;
        accepts(&self.repo, &entry.repo) && accepts(&self.peer, &entry.peer)
    }
}

/// Runs the `logs` command.
pub fn run(args: &LogsArgs, dir: &ConfigDir) -> Result<()> {
    control::block_on(async {
        let mut client = ControlClient::connect_required(dir).await?;
        client
            .send(&Request::Logs {
                follow: args.follow,
            })
            .await?;

        let start = match client.recv(Some(CLIENT_TIMEOUT)).await? {
            Response::LogsStart(start) => start,
            other => bail!("unexpected response from the daemon: {other:?}"),
        };
        let mut backlog = Vec::new();
        for _ in 0..start.backlog {
            match client.recv(Some(CLIENT_TIMEOUT)).await? {
                Response::Log(entry) if args.matches(&entry) => backlog.push(entry),
                Response::Log(_) => {}
                other => bail!("unexpected response from the daemon: {other:?}"),
            }
        }

        // The history is in-memory and bounded: always say where it starts.
        let history = if start.dropped == 0 {
            format!(
                "since the daemon started {} ago",
                ui::format_duration(start.uptime_secs)
            )
        } else {
            format!("in the last {} events kept by the daemon", start.backlog)
        };
        if backlog.is_empty() {
            let matching = if args.repo.is_some() || args.peer.is_some() {
                "matching "
            } else {
                ""
            };
            println!("{}", ui::dim(format_args!("no {matching}events {history}")));
        } else {
            println!("{}", ui::dim(format_args!("(events {history})")));
        }
        let skip = args.lines.map_or(0, |n| backlog.len().saturating_sub(n));
        for entry in &backlog[skip..] {
            print_entry(entry);
        }

        if !args.follow {
            return Ok(());
        }
        loop {
            match client.recv(None).await {
                Ok(Response::Log(entry)) if args.matches(&entry) => print_entry(&entry),
                Ok(Response::Log(_)) => {}
                Ok(Response::LogsSkipped(count)) => {
                    println!("{}", ui::warn(format_args!("({count} events skipped)")));
                }
                Ok(other) => bail!("unexpected response from the daemon: {other:?}"),
                Err(err) if is_eof(&err) => bail!("the daemon stopped"),
                Err(err) => return Err(err),
            }
        }
    })
}

/// Whether a receive failed because the daemon closed the connection.
fn is_eof(err: &color_eyre::Report) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .is_some_and(|err| err.kind() == io::ErrorKind::UnexpectedEof)
    })
}

/// Prints one event: time, message, then its fields.
fn print_entry(entry: &LogEntry) {
    let time = DateTime::<Local>::from(entry.time).format("%b %d %H:%M:%S");
    let message = sanitize(&entry.message);
    let message = match entry.level {
        LogLevel::Info => message,
        LogLevel::Warn => ui::warn(message).to_string(),
        LogLevel::Error => ui::bad(message).to_string(),
    };

    let mut fields = Vec::new();
    if let Some(repo) = &entry.repo {
        fields.push(format!("repo={repo}"));
    }
    if let Some(peer) = &entry.peer {
        fields.push(format!("peer={peer}"));
    }
    if !entry.fields.is_empty() {
        fields.push(entry.fields.clone());
    }
    if fields.is_empty() {
        println!("{}  {message}", ui::dim(time));
    } else {
        let fields = sanitize(&fields.join(" "));
        println!("{}  {message}  {}", ui::dim(time), ui::dim(fields));
    }
}

//! The clone handler: pull a mesh repo from an announcing peer, register
//! it, and stream progress frames to the client while the pull runs.

use std::path::Path;

use color_eyre::eyre::{Result, bail};
use tokio::net::UnixStream;
use tracing::info;

use super::{
    protocol::{CLONE_PROGRESS_INTERVAL, CloneProgress, Response},
    server::{ControlContext, client_gone, respond},
};
use crate::{config::Repo, repo::transfer};

/// Pulls the mesh repo named `name` into the freshly initialized repo at
/// `path`, streaming progress frames while the pull runs. Stops the pull
/// when the client disconnects (or stops reading progress): the clone only
/// exists for the CLI that asked, and it must not register a repo behind
/// a gone user's back. Work already handed to a blocking thread (a pack
/// ingest, the apply with its index build — the longest such work on a
/// large clone) still finishes, so the abandoned directory may gain more
/// objects; it is never registered.
pub(super) async fn clone_repo(
    stream: &mut UnixStream,
    ctx: &ControlContext,
    name: &str,
    path: &Path,
) -> Result<()> {
    let (mut read_half, mut write_half) = stream.split();
    // Seeded before the pull so the heartbeat covers the whole handler,
    // validation and dialing included: the CLI treats a silent gap over
    // its idle budget as a dead daemon.
    let progress = tokio::sync::watch::Sender::new(CloneProgress {
        peer: String::new(),
        transfer: transfer::TransferProgress::start(transfer::TransferPhase::Ops),
    });

    let clone = clone_pull_and_register(ctx, name, path, &progress);
    tokio::pin!(clone);
    let mut heartbeat = tokio::time::interval(CLONE_PROGRESS_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let response = loop {
        tokio::select! {
            result = &mut clone => {
                break result.unwrap_or_else(|err| Response::Error(format!("{err:#}")));
            }
            _ = heartbeat.tick() => {
                let latest = progress.borrow().clone();
                if respond(&mut write_half, &Response::CloneProgress(latest)).await.is_err() {
                    // A client too stalled to drain cosmetic frames is as
                    // gone as a disconnected one (and the failed write may
                    // have desynced the framing anyway).
                    info!("clone cancelled: the client stopped reading");
                    return Ok(());
                }
            }
            () = client_gone(&mut read_half) => {
                info!("clone cancelled by the client");
                return Ok(());
            }
        }
    };
    respond(&mut write_half, &response).await
}

/// The clone work itself: validate, pull, register (see [`clone_repo`]).
async fn clone_pull_and_register(
    ctx: &ControlContext,
    name: &str,
    path: &Path,
    progress: &tokio::sync::watch::Sender<CloneProgress>,
) -> Result<Response> {
    let (repo_id, sources) = ctx.hub.clone_sources(name)?;

    // Fail before the (long) pull when the registration cannot succeed.
    // Only the re-validation inside the `store.update` below is
    // authoritative: the state may change during the pull.
    {
        let state = ctx.store.snapshot();
        state.validate_new_repo(name, path)?;
        state.ensure_mesh_id(name, &repo_id)?;
        if let Some(existing) = state.repo_name(&repo_id) {
            bail!("`{name}` is the repo already registered here as `{existing}`");
        }
    }

    // The progress publishes latest-wins into the watch; the connection
    // task samples and forwards it on its heartbeat.
    let sink = |peer: &str, transfer| {
        progress.send_replace(CloneProgress {
            peer: peer.to_owned(),
            transfer,
        });
    };
    let (ops, git_objects) = ctx.hub.pull(name, &repo_id, sources, path, &sink).await?;

    ctx.store.update(|state| {
        state.add_clone(
            name.to_owned(),
            Repo {
                id: repo_id.clone(),
                path: path.to_owned(),
                bare: false,
            },
        )
    })?;
    Ok(Response::Cloned { ops, git_objects })
}

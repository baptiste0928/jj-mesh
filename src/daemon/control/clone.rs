//! The clone handler: pull a mesh repo from an announcing peer, register
//! it, and stream progress frames to the client while the pull runs.

use std::{path::Path, sync::Arc};

use color_eyre::eyre::{Result, WrapErr as _, bail, eyre};
use tokio::net::UnixStream;
use tracing::{info, warn};

use super::{
    protocol::{CLONE_PROGRESS_INTERVAL, CLONE_PULL_TIMEOUT, CloneProgress, Response},
    server::{ControlContext, client_gone, respond},
};
use crate::{
    config::{Repo, RepoId},
    daemon::hub::CloneSource,
    net::fetch::GitTransferFormat,
    repo::{JjRepo, transfer},
};

/// Pulls the mesh repo named `name` into the freshly initialized repo at
/// `path`, streaming progress frames while the pull runs. Stops the pull
/// when the client disconnects (or stops reading progress): the clone only
/// exists for the CLI that asked, and it must not register a repo behind
/// a gone user's back. Work already handed to a blocking thread (a pack
/// ingest, the apply) still finishes, so the directory the CLI tells the
/// user to remove may gain more objects; it is never registered.
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

    let (ops, git_objects) = clone_pull(ctx, name, &repo_id, sources, path, progress).await?;

    ctx.store.update(|state| {
        state.add_repo(
            name.to_owned(),
            Repo {
                id: repo_id.clone(),
                path: path.to_owned(),
            },
        )
    })?;
    Ok(Response::Cloned { ops, git_objects })
}

async fn clone_pull(
    ctx: &ControlContext,
    name: &str,
    repo_id: &RepoId,
    sources: Vec<CloneSource>,
    path: &Path,
    progress: &tokio::sync::watch::Sender<CloneProgress>,
) -> Result<(u64, u64)> {
    use jj_lib::op_store::OperationId;

    let repo_path = path.to_owned();
    let repo = tokio::task::spawn_blocking(move || -> Result<_> {
        Ok(Arc::new(JjRepo::discover(&repo_path)?.open()?))
    })
    .await
    .wrap_err("repo open task failed")??;

    let mut last_error = eyre!("no usable source peer");
    for CloneSource { peer, heads } in sources {
        let wants: Vec<OperationId> = heads.into_iter().map(OperationId::new).collect();
        let Some(conn) = ctx.hub.connection(&peer) else {
            continue;
        };

        // The transfer sink publishes latest-wins into the watch; the
        // connection task samples and forwards it on its heartbeat. Reset
        // to zeroed counters before dialing, so a fallback to this source
        // is visible and a stalled dial still heartbeats fresh state.
        let peer_name = ctx
            .store
            .snapshot()
            .peer_name(&peer)
            .map_or_else(|| peer.to_string(), str::to_owned);
        let sink = |transfer: transfer::TransferProgress| {
            progress.send_replace(CloneProgress {
                peer: peer_name.clone(),
                transfer,
            });
        };
        sink(transfer::TransferProgress::start(
            transfer::TransferPhase::Ops,
        ));

        let pull = async {
            let (mut send, mut recv) = conn.open_bi().await?;
            // A clone pulls a whole history: the pack format reuses the
            // server's on-disk deltas and lands as one pack file here,
            // instead of writing every object loose.
            let outcome = transfer::fetch(
                &repo,
                transfer::RepoIdent { name, id: repo_id },
                &wants,
                GitTransferFormat::Pack,
                &mut send,
                &mut recv,
                transfer::ProgressSink::new(&sink),
            )
            .await?;
            let _ = send.finish();
            Ok::<_, color_eyre::Report>(outcome)
        };
        match tokio::time::timeout(CLONE_PULL_TIMEOUT, pull).await {
            Err(_) => last_error = eyre!("pull from {peer} timed out"),
            Ok(Err(err)) => last_error = err.wrap_err(format!("pull from {peer} failed")),
            Ok(Ok(outcome)) => return Ok((outcome.ops as u64, outcome.git_objects as u64)),
        }
        warn!("clone pull attempt failed: {last_error:#}");
    }
    Err(last_error)
}

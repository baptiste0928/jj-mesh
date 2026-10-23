//! The format 1 `mesh.json`, which keyed repos by name, and its migration.
//!
//! Its repo tombstones carry no id, so they cannot be migrated: a machine
//! that missed a removal before upgrading may bring the repo back, as a
//! clonable repo.

use std::{collections::BTreeMap, path::PathBuf};

use color_eyre::eyre::Result;
use iroh::EndpointId;
use serde::Deserialize;

use super::{
    FORMAT, Machine, MeshRepo, MeshRepoStatus, MeshState, ObjectHash, Peer, Repo, RepoId,
    WorkspaceClaims,
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct State {
    machine: Machine,
    peers: BTreeMap<EndpointId, Peer>,
    repos: BTreeMap<String, LocalRepo>,
    mesh_repos: BTreeMap<String, NamedRepo>,
    claims: WorkspaceClaims,
    peer_claims: BTreeMap<EndpointId, WorkspaceClaims>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalRepo {
    id: RepoId,
    path: PathBuf,
    #[serde(default)]
    bare: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NamedRepo {
    version: u64,
    status: NamedRepoStatus,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum NamedRepoStatus {
    Present {
        id: RepoId,
        #[serde(default)]
        object_hash: ObjectHash,
    },
    Removed,
}

/// Migrates a format 1 state to the current format.
pub(super) fn migrate(value: serde_json::Value) -> Result<MeshState> {
    let old: State = serde_json::from_value(value)?;

    let mut mesh_repos = BTreeMap::new();
    for (name, record) in old.mesh_repos {
        let NamedRepoStatus::Present { id, object_hash } = record.status else {
            continue;
        };
        let record = MeshRepo {
            version: record.version,
            status: MeshRepoStatus::Present { name, object_hash },
        };
        mesh_repos.insert(id, record);
    }

    let mut repos = BTreeMap::new();
    for (name, repo) in old.repos {
        // A repo registered under a name the mesh gave to another repo gets
        // its own record: it then shows as a name conflict.
        mesh_repos.entry(repo.id.clone()).or_insert(MeshRepo {
            version: 1,
            status: MeshRepoStatus::Present {
                name,
                object_hash: ObjectHash::default(),
            },
        });
        repos.insert(
            repo.id,
            Repo {
                path: repo.path,
                bare: repo.bare,
            },
        );
    }

    Ok(MeshState {
        format: FORMAT,
        machine: old.machine,
        peers: old.peers,
        repos,
        mesh_repos,
        claims: old.claims,
        peer_claims: old.peer_claims,
    })
}

//! Mesh state file (`mesh.json`).
//!
//! This file holds the machine's copy of the mesh state, in two parts:
//! what is replicated across the mesh by the membership gossip, and what
//! is strictly local (the repos registered here, with their paths).
//!
//! Repos are identified by their id everywhere; their name is part of
//! their mesh record. Names are unique when a repo is added or renamed,
//! but machines editing the mesh concurrently can still end up with two
//! repos under one name: [`MeshState::name_conflicts`] surfaces them.
//!
//! The daemon is the only writer; the CLI mutates it through the control
//! socket, and may read the file directly (for pre-checks and completion),
//! treating what it sees as advisory. Files of an older format are
//! migrated on load (see the `legacy` submodule).

mod claims;
mod legacy;
mod machine;
mod membership;
mod repo;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs, io,
    io::Write as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use tracing::warn;

pub use self::{
    claims::{
        MAX_MACHINE_WORKSPACES, MAX_MESH_CLAIMED_REPOS, MAX_MESH_WORKSPACES, RepoClaims,
        WorkspaceClaims,
    },
    machine::Machine,
    membership::{
        MAX_MESH_PEERS, MAX_MESH_REPO_RECORDS, MAX_MESH_REPOS, Membership, MeshRepo,
        MeshRepoStatus, Peer, PeerStatus,
    },
    repo::{ObjectHash, Repo, RepoId},
};
use self::{
    claims::{MAX_OTHER_REPOS, MAX_OTHER_WORKSPACES},
    membership::{
        MAX_OTHER_PEERS, MAX_RECORD_VERSION, Register as _, bumped_version, should_adopt,
    },
};
use super::{ConfigDir, validate_name};

/// Version of the `mesh.json` layout, bumped on incompatible changes.
const FORMAT: u32 = 2;

/// This machine's copy of the mesh state.
///
/// `repos` (with its local paths) never leaves this machine; the rest is
/// replicated across the mesh by the membership gossip.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MeshState {
    /// Layout version of the file, see [`FORMAT`].
    pub format: u32,
    /// This machine's own record.
    pub machine: Machine,
    /// The other machines of the mesh, keyed by endpoint id. Includes
    /// tombstones (removed peers), which gossip must remember so a removal
    /// cannot be undone by a machine that missed it.
    pub peers: BTreeMap<EndpointId, Peer>,
    /// Repos registered on this machine, which the mesh lists.
    pub repos: BTreeMap<RepoId, Repo>,
    /// Every repo the mesh knows, registered here or not. Includes
    /// tombstones (removed repos), which gossip must remember so a
    /// machine that missed the removal cannot resurrect the repo.
    pub mesh_repos: BTreeMap<RepoId, MeshRepo>,
    /// The workspaces this machine keeps fresh.
    pub claims: WorkspaceClaims,
    /// The workspaces the other alive machines keep fresh.
    pub peer_claims: BTreeMap<EndpointId, WorkspaceClaims>,
}

impl Default for MeshState {
    fn default() -> Self {
        MeshState {
            format: FORMAT,
            machine: Machine::default(),
            peers: BTreeMap::new(),
            repos: BTreeMap::new(),
            mesh_repos: BTreeMap::new(),
            claims: WorkspaceClaims::default(),
            peer_claims: BTreeMap::new(),
        }
    }
}

impl MeshState {
    /// Loads `mesh.json` from the configuration directory, defaulting to an
    /// empty state if the file does not exist yet. Older formats are
    /// migrated in memory.
    pub fn load(dir: &ConfigDir) -> Result<Self> {
        Ok(Self::load_migrated(dir)?.0)
    }

    /// [`Self::load`], also telling whether the file was migrated from an
    /// older format, for the daemon to save it in the current one.
    pub fn load_migrated(dir: &ConfigDir) -> Result<(Self, bool)> {
        let path = dir.mesh_file();
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok((Self::default(), false));
            }
            Err(err) => {
                return Err(err).wrap_err_with(|| format!("cannot read {}", path.display()));
            }
        };
        Self::parse(&bytes).wrap_err_with(|| format!("invalid mesh state in {}", path.display()))
    }

    /// Parses `mesh.json`, migrating older formats; tells whether it did.
    fn parse(bytes: &[u8]) -> Result<(Self, bool)> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;
        match value.get("format") {
            None => Ok((legacy::migrate(value)?, true)),
            Some(format) if *format == FORMAT => Ok((serde_json::from_value(value)?, false)),
            Some(format) => bail!(
                "format {format} is not supported by this jj-mesh (expected {FORMAT}). Update jj-mesh"
            ),
        }
    }

    /// Saves the state durably: written to a private temp file (0600,
    /// exclusive), synced, then renamed into place and the directory synced.
    /// Readers never see a partial file, and the state survives a crash once
    /// this returns; pairing relies on that to only confirm saved peers.
    pub fn save(&self, dir: &ConfigDir) -> Result<()> {
        let path = dir.mesh_file();
        let json = serde_json::to_vec_pretty(self).wrap_err("cannot encode mesh state")?;

        let mut tmp = tempfile::NamedTempFile::new_in(dir.path())
            .wrap_err("cannot create temporary state file")?;
        tmp.write_all(&json)
            .and_then(|()| tmp.as_file().sync_all())
            .wrap_err_with(|| format!("cannot write {}", tmp.path().display()))?;
        tmp.persist(&path)
            .wrap_err_with(|| format!("cannot write {}", path.display()))?;

        fs::File::open(dir.path())
            .and_then(|dir| dir.sync_all())
            .wrap_err("cannot sync the config directory")?;

        Ok(())
    }

    /// The name under which an endpoint is paired, if alive.
    pub fn peer_name(&self, endpoint: &EndpointId) -> Option<&str> {
        self.peers.get(endpoint).and_then(Peer::name)
    }

    /// The alive peers, as `(endpoint, name)` pairs.
    pub fn alive_peers(&self) -> impl Iterator<Item = (&EndpointId, &str)> {
        self.peers
            .iter()
            .filter_map(|(endpoint, peer)| Some((endpoint, peer.name()?)))
    }

    /// The name of a repo the mesh lists.
    pub fn repo_name(&self, id: &RepoId) -> Option<&str> {
        self.mesh_repos.get(id).and_then(MeshRepo::name)
    }

    /// The repos the mesh lists (not removed), as `(id, name)` pairs.
    pub fn mesh_repo_names(&self) -> impl Iterator<Item = (&RepoId, &str)> {
        self.mesh_repos
            .iter()
            .filter_map(|(id, repo)| Some((id, repo.name()?)))
    }

    /// The distinct names of the mesh repos.
    pub fn repo_names(&self) -> BTreeSet<&str> {
        self.mesh_repo_names().map(|(_, name)| name).collect()
    }

    /// The mesh repos not registered on this machine, i.e. the clonable
    /// ones.
    pub fn clonable_repos(&self) -> impl Iterator<Item = (&RepoId, &str)> {
        self.mesh_repo_names()
            .filter(|(id, _)| !self.repos.contains_key(*id))
    }

    /// The names several mesh repos carry, with their ids.
    pub fn name_conflicts(&self) -> BTreeMap<&str, Vec<&RepoId>> {
        let mut names: BTreeMap<&str, Vec<&RepoId>> = BTreeMap::new();
        for (id, name) in self.mesh_repo_names() {
            names.entry(name).or_default().push(id);
        }
        names.retain(|_, ids| ids.len() > 1);
        names
    }

    /// Resolves a mesh repo from its name or id. A name several repos
    /// carry resolves to the one registered here, if any is. Names are
    /// matched first: a repo could otherwise be named after another's id.
    pub fn resolve_repo(&self, selector: &str) -> Result<RepoId> {
        let ids: Vec<&RepoId> = self
            .mesh_repo_names()
            .filter(|(_, name)| *name == selector)
            .map(|(id, _)| id)
            .collect();
        let registered: Vec<&RepoId> = ids
            .iter()
            .copied()
            .filter(|id| self.repos.contains_key(*id))
            .collect();
        match (&ids[..], &registered[..]) {
            ([id], _) | (_, [id]) => return Ok((*id).clone()),
            ([], _) => {}
            _ => {
                let ids: Vec<String> = ids.iter().map(ToString::to_string).collect();
                bail!(
                    "several repos are named `{selector}` ({}). Use the id of the one you mean",
                    ids.join(", "),
                );
            }
        }

        match RepoId::try_from(selector.to_owned()) {
            Ok(id) if self.repo_name(&id).is_some() => Ok(id),
            _ => bail!("no repo named `{selector}` in the mesh"),
        }
    }

    /// The claims on the workspaces of a repo.
    pub fn repo_claims(&self, id: &RepoId) -> RepoClaims {
        let mut others: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (endpoint, claims) in &self.peer_claims {
            let (Some(peer), Some(names)) = (self.peer_name(endpoint), claims.repos.get(id)) else {
                continue;
            };
            for name in names {
                others
                    .entry(name.clone())
                    .or_default()
                    .push(peer.to_owned());
            }
        }
        let ours = self.claims.repos.get(id).cloned().unwrap_or_default();
        let limit = MAX_MACHINE_WORKSPACES - (self.claims.count() - ours.len());
        RepoClaims {
            ours,
            others,
            limit,
        }
    }

    /// Corrects the object hash the mesh records for a registered repo, as
    /// its stores report it; the bumped version propagates the fix.
    pub fn set_object_hash(&mut self, id: &RepoId, object_hash: ObjectHash) -> Result<()> {
        if !self.repos.contains_key(id) {
            return Ok(());
        }
        let Some((name, recorded)) = self.mesh_repos.get(id).and_then(MeshRepo::present) else {
            return Ok(());
        };
        if recorded == object_hash {
            return Ok(());
        }
        let name = name.to_owned();
        self.set_mesh_repo(id, MeshRepoStatus::Present { name, object_hash })
    }

    /// Sets the workspaces of a registered repo this machine claims. A repo
    /// unregistered meanwhile is left unclaimed.
    pub fn set_claims(&mut self, id: &RepoId, names: BTreeSet<String>) -> Result<()> {
        if !self.repos.contains_key(id) {
            return Ok(());
        }
        self.claims.set(id, names)
    }

    /// Checks that a peer can be paired under this name and endpoint.
    /// Duplicate names are allowed (machines are identified by their key;
    /// two laptops may both be called `laptop`), an alive endpoint is not.
    pub fn validate_new_peer(&self, name: &str, endpoint: &EndpointId) -> Result<()> {
        validate_name("peer", name)?;
        if let Some(existing) = self.peer_name(endpoint) {
            bail!("endpoint {endpoint} is already paired as `{existing}`");
        }

        Ok(())
    }

    /// Checks that a new repo can be added to the mesh under this name, with
    /// its root at `path`.
    pub fn validate_new_repo(&self, name: &str, path: &Path) -> Result<()> {
        validate_name("repo", name)?;
        if let Some((id, _)) = self.mesh_repo_names().find(|(_, other)| *other == name) {
            ensure!(
                !self.repos.contains_key(id),
                "a repo named `{name}` already exists"
            );
            bail!("a repo named `{name}` already exists in the mesh. Clone it instead");
        }
        self.validate_new_path(path)
    }

    /// Checks that no registered repo has its root at `path`. Two spellings
    /// of one directory (symlinks, `..`) must not register twice: stored
    /// paths are canonical (see [`Self::add_repo`]), so only the candidate
    /// needs resolving; the fallback covers paths that do not resolve right
    /// now.
    fn validate_new_path(&self, path: &Path) -> Result<()> {
        let path = canonical_path(path);
        if let Some((id, _)) = self.repos.iter().find(|(_, repo)| repo.path == path) {
            let name = self.repo_name(id).unwrap_or("?");
            bail!("{} is already added as `{name}`", path.display());
        }

        Ok(())
    }

    /// Registers a paired peer as alive, superseding any tombstone: the
    /// bumped version propagates the re-add through the gossip.
    pub fn add_peer(&mut self, endpoint: EndpointId, name: String) -> Result<()> {
        self.validate_new_peer(&name, &endpoint)?;
        // The same cap the merge and the wire enforce: going past it here
        // would make our own membership undecodable by every peer, which
        // silently stops us from gossiping anything at all.
        ensure!(
            self.peers.contains_key(&endpoint) || self.peers.len() < MAX_OTHER_PEERS,
            "the mesh already has {MAX_MESH_PEERS} machines",
        );

        let version = bumped_version(&self.peers, &endpoint)?;
        self.peers.insert(
            endpoint,
            Peer {
                version,
                status: PeerStatus::Alive {
                    name,
                    server: false,
                },
            },
        );

        Ok(())
    }

    /// Renames this machine; the bumped version propagates the rename
    /// through the gossip.
    pub fn rename_machine(&mut self, name: String) -> Result<()> {
        self.machine.rename(name)
    }

    /// Switches server mode on this machine; the bumped version propagates
    /// the role through the gossip.
    pub fn set_server(&mut self, server: bool) -> Result<()> {
        self.machine.set_server(server)
    }

    /// Tombstones an alive peer, resolved by name or full endpoint id.
    /// The tombstone propagates the removal through the gossip.
    pub fn remove_peer(&mut self, selector: &str) -> Result<EndpointId> {
        let endpoint = self.resolve_peer(selector)?;
        let version = bumped_version(&self.peers, &endpoint)?;

        let peer = self.peers.get_mut(&endpoint).expect("resolved above");
        peer.version = version;
        peer.status = PeerStatus::Removed;
        self.prune_claims();

        Ok(endpoint)
    }

    /// Resolves an alive peer from a name or a full endpoint id. Names are
    /// matched first: a peer could otherwise be named after another peer's
    /// endpoint id and shadow it.
    fn resolve_peer(&self, selector: &str) -> Result<EndpointId> {
        let matches: Vec<&EndpointId> = self
            .alive_peers()
            .filter(|(_, name)| *name == selector)
            .map(|(endpoint, _)| endpoint)
            .collect();
        match matches[..] {
            [endpoint] => return Ok(*endpoint),
            [_, _, ..] => bail!(
                "several peers are named `{selector}`. Rename one of them with \
                 `jj-mesh peer rename` on its machine"
            ),
            [] => {}
        }

        let Ok(endpoint) = selector.parse::<EndpointId>() else {
            bail!("no peer named `{selector}`");
        };
        ensure!(
            self.peer_name(&endpoint).is_some(),
            "endpoint {endpoint} is not a paired peer",
        );

        Ok(endpoint)
    }

    /// Adds a new repo to the mesh under `name`, using `object_hash`, and
    /// registers it here. The path is stored canonicalized, whatever the
    /// caller sent, so path comparisons never have to resolve stored
    /// entries again.
    pub fn add_repo(
        &mut self,
        id: RepoId,
        name: String,
        mut repo: Repo,
        object_hash: ObjectHash,
    ) -> Result<()> {
        repo.path = canonical_path(&repo.path);
        self.validate_new_repo(&name, &repo.path)?;
        ensure!(
            !self.mesh_repos.contains_key(&id),
            "repo {id} is already in the mesh"
        );
        ensure!(
            self.mesh_repo_names().count() < MAX_MESH_REPOS,
            "the mesh already has {MAX_MESH_REPOS} repos",
        );
        ensure!(
            self.mesh_repos.len() < MAX_MESH_REPO_RECORDS,
            "the mesh already recorded {MAX_MESH_REPO_RECORDS} repos, removed ones included",
        );

        self.set_mesh_repo(&id, MeshRepoStatus::Present { name, object_hash })?;
        self.repos.insert(id, repo);

        Ok(())
    }

    /// Registers a clone of the mesh repo `id` (see [`Self::check_clone`]).
    /// Unlike [`Self::add_repo`], the mesh record is left alone: a clone
    /// finishing after a removal must not undo it.
    pub fn add_clone(&mut self, id: RepoId, repo: Repo) -> Result<()> {
        let repo = self.check_clone(&id, repo)?;
        self.repos.insert(id, repo);

        Ok(())
    }

    /// Checks that `repo` can be registered as a clone of the mesh repo
    /// `id`, which the mesh must still list, canonicalizing its path.
    pub fn check_clone(&self, id: &RepoId, mut repo: Repo) -> Result<Repo> {
        let Some(name) = self.repo_name(id) else {
            bail!("the mesh no longer lists repo {id}");
        };
        ensure!(
            !self.repos.contains_key(id),
            "`{name}` is already registered on this machine"
        );
        repo.path = canonical_path(&repo.path);
        self.validate_new_path(&repo.path)?;
        Ok(repo)
    }

    /// Removes a repo from the mesh: tombstones its record and unregisters
    /// it here. The tombstone propagates the removal, so every machine stops
    /// syncing the repo; only servers delete their bare repo, unless a
    /// workspace uses it.
    /// Returns whether the repo was registered on this machine.
    pub fn remove_repo(&mut self, id: &RepoId) -> Result<bool> {
        ensure!(
            self.repo_name(id).is_some(),
            "the mesh no longer lists repo {id}"
        );
        self.set_mesh_repo(id, MeshRepoStatus::Removed)?;
        Ok(self.unregister_repo(id).is_some())
    }

    /// Renames a mesh repo; the bumped version propagates the name through
    /// the gossip. Renaming to the current name changes nothing.
    pub fn rename_repo(&mut self, id: &RepoId, new: String) -> Result<()> {
        validate_name("repo", &new)?;
        let Some((name, object_hash)) = self.mesh_repos.get(id).and_then(MeshRepo::present) else {
            bail!("the mesh no longer lists repo {id}");
        };
        if name == new {
            return Ok(());
        }
        ensure!(
            !self.repo_names().contains(new.as_str()),
            "a repo named `{new}` already exists"
        );
        let status = MeshRepoStatus::Present {
            name: new,
            object_hash,
        };
        self.set_mesh_repo(id, status)
    }

    /// Unregisters a repo on this machine only, returning its record. The
    /// mesh record is untouched: the other machines keep syncing the repo
    /// among themselves, and it stays clonable here.
    ///
    /// Bare repos cannot be forgotten: server mode would pull them again.
    pub fn forget_repo(&mut self, id: &RepoId) -> Result<Repo> {
        let name = self.repo_name(id).unwrap_or("?");
        match self.repos.get(id) {
            None => bail!("`{name}` is not registered on this machine"),
            Some(repo) if repo.bare => {
                bail!(
                    "`{name}` is a bare repo kept by server mode. Disable server mode or remove the repo from the mesh"
                )
            }
            Some(_) => Ok(self.unregister_repo(id).expect("registered")),
        }
    }

    /// Unregisters a bare repo (see `daemon::bare`); other repos are left
    /// alone.
    pub fn drop_bare(&mut self, id: &RepoId) {
        if self.repos.get(id).is_some_and(|repo| repo.bare) {
            self.unregister_repo(id);
        }
    }

    /// Writes the mesh record of `id`, outranking the copies the other
    /// machines hold.
    fn set_mesh_repo(&mut self, id: &RepoId, status: MeshRepoStatus) -> Result<()> {
        let version = bumped_version(&self.mesh_repos, id)?;
        self.mesh_repos
            .insert(id.clone(), MeshRepo { version, status });
        Ok(())
    }

    /// Unregisters a repo here, releasing its workspace claims.
    fn unregister_repo(&mut self, id: &RepoId) -> Option<Repo> {
        let repo = self.repos.remove(id)?;
        if let Err(err) = self.claims.set(id, BTreeSet::new()) {
            warn!(repo = %id, "cannot release workspace claims: {err:#}");
        }
        Some(repo)
    }

    /// The membership this machine gossips, its own records under `local`.
    pub fn membership(&self, local: EndpointId) -> Membership {
        let mut peers = self.peers.clone();
        peers.insert(local, self.machine.record());
        let mut claims = self.peer_claims.clone();
        claims.insert(local, self.claims.clone());
        Membership {
            peers,
            repos: self.mesh_repos.clone(),
            claims,
        }
    }

    /// Whether the gossiped part of the state (see [`Self::membership`])
    /// differs, without building it.
    pub fn membership_differs(&self, other: &Self) -> bool {
        self.machine != other.machine
            || self.peers != other.peers
            || self.mesh_repos != other.mesh_repos
            || self.claims != other.claims
            || self.peer_claims != other.peer_claims
    }

    /// Merges a peer's membership into ours. `local` is this machine's own
    /// endpoint.
    ///
    /// Records merge as versioned registers (see the `membership`
    /// submodule), so every machine converges on the same state without
    /// clocks. Adopting a removed repo also unregisters it here: that is
    /// how the removal reaches the machines that hold it.
    ///
    /// Copies of our own record are absorbed by [`Machine::observe`] and
    /// never stored as peers.
    ///
    /// New entries stop being adopted at the caps ([`MAX_MESH_PEERS`],
    /// [`MAX_MESH_REPOS`], [`MAX_MESH_REPO_RECORDS`]) while updates to
    /// known ones keep flowing.
    ///
    /// Returns the names of the repos unregistered here because the mesh
    /// removed them.
    pub fn merge_membership(&mut self, remote: &Membership, local: &EndpointId) -> Vec<String> {
        for (endpoint, record) in &remote.peers {
            // Strictly below the ceiling: a record *at* it could never be
            // superseded by a local change, freezing the machine's status.
            if record.version >= MAX_RECORD_VERSION {
                continue;
            }
            if endpoint == local {
                self.machine.observe(record);
                continue;
            }
            if let PeerStatus::Alive { name, .. } = &record.status
                && validate_name("peer", name).is_err()
            {
                continue;
            }
            if should_adopt(&self.peers, endpoint, record, MAX_OTHER_PEERS) {
                self.peers.insert(*endpoint, record.clone());
            }
        }

        // Only new ids can add live repos: a tombstone is final.
        let mut removed = Vec::new();
        let mut live = self.mesh_repo_names().count();
        for (id, record) in &remote.repos {
            if record.version >= MAX_RECORD_VERSION || !record.is_valid() {
                continue;
            }
            let Some(held) = self.mesh_repos.get(id) else {
                if self.mesh_repos.len() < MAX_MESH_REPO_RECORDS
                    && (record.name().is_none() || live < MAX_MESH_REPOS)
                {
                    live += usize::from(record.name().is_some());
                    self.mesh_repos.insert(id.clone(), record.clone());
                }
                continue;
            };
            if !record.outranks(held) {
                continue;
            }
            // A repo removed mesh-wide stops being synced here; its files
            // stay where they are.
            if record.name().is_none() {
                let name = held.name().map(str::to_owned);
                live -= usize::from(name.is_some());
                if self.unregister_repo(id).is_some() {
                    removed.push(name.unwrap_or_else(|| id.to_string()));
                }
            }
            self.mesh_repos.insert(id.clone(), record.clone());
        }

        self.merge_claims(remote, local);
        removed
    }

    /// Merges the gossiped workspace claims: only alive peers' are kept,
    /// and new or grown records stop being adopted at the caps.
    fn merge_claims(&mut self, remote: &Membership, local: &EndpointId) {
        let (mut names, mut repos) = WorkspaceClaims::tally(self.peer_claims.values());
        for (endpoint, record) in &remote.claims {
            if record.version >= MAX_RECORD_VERSION {
                continue;
            }
            if endpoint == local {
                self.claims.observe(record);
                continue;
            }
            let held = self.peer_claims.get(endpoint);
            if self.peer_name(endpoint).is_none()
                || held.is_some_and(|held| !record.outranks(held))
                || !record.is_valid()
            {
                continue;
            }
            let (held_names, held_repos) = WorkspaceClaims::tally(held);
            let (record_names, record_repos) = WorkspaceClaims::tally([record]);
            let adopted_names = names - held_names + record_names;
            let adopted_repos = repos - held_repos + record_repos;
            if adopted_names > MAX_OTHER_WORKSPACES || adopted_repos > MAX_OTHER_REPOS {
                continue;
            }
            (names, repos) = (adopted_names, adopted_repos);
            self.peer_claims.insert(*endpoint, record.clone());
        }
        self.prune_claims();
    }

    /// Drops the claims of machines no longer alive in the mesh.
    fn prune_claims(&mut self) {
        let peers = &self.peers;
        self.peer_claims
            .retain(|endpoint, _| peers.get(endpoint).and_then(Peer::name).is_some());
    }
}

/// Resolves a path to its canonical form, falling back to the path as
/// given when it does not resolve (unmounted disk, deleted repo): a path
/// that cannot be resolved cannot silently alias another either.
fn canonical_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_owned())
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    impl Machine {
        fn new(name: &str, version: u64) -> Self {
            Machine {
                name: name.to_owned(),
                server: false,
                version,
            }
        }
    }

    /// A mesh repo record in use.
    fn present(version: u64, name: &str) -> MeshRepo {
        MeshRepo {
            version,
            status: MeshRepoStatus::Present {
                name: name.to_owned(),
                object_hash: ObjectHash::Sha1,
            },
        }
    }

    /// Adds a new repo named `name`, at a path derived from the name.
    fn add(state: &mut MeshState, name: &str) -> RepoId {
        let id = RepoId::generate();
        let repo = Repo {
            path: format!("/tmp/{name}").into(),
            bare: false,
        };
        state
            .add_repo(id.clone(), name.to_owned(), repo, ObjectHash::Sha1)
            .unwrap();
        id
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        let endpoint = SecretKey::generate().public();

        let mut state = MeshState::load(&dir).unwrap();
        assert!(state.peers.is_empty());
        state.add_peer(endpoint, "laptop".to_owned()).unwrap();
        let id = add(&mut state, "project");
        state.save(&dir).unwrap();

        let mut state = MeshState::load(&dir).unwrap();
        assert_eq!(state.format, FORMAT);
        assert_eq!(state.peer_name(&endpoint), Some("laptop"));
        assert!(state.repos.contains_key(&id));
        assert_eq!(state.repo_name(&id), Some("project"));

        assert!(state.remove_peer("desktop").is_err());
        let removed = state.remove_peer("laptop").unwrap();
        assert_eq!(removed, endpoint);
        state.save(&dir).unwrap();

        // The removal leaves a tombstone, not a hole.
        let state = MeshState::load(&dir).unwrap();
        assert_eq!(state.alive_peers().count(), 0);
        assert_eq!(state.peers[&endpoint].status, PeerStatus::Removed);
    }

    #[test]
    fn repairing_supersedes_the_tombstone() {
        let mut state = MeshState::default();
        let endpoint = SecretKey::generate().public();

        state.add_peer(endpoint, "laptop".to_owned()).unwrap();
        state.remove_peer("laptop").unwrap();
        let tombstone_version = state.peers[&endpoint].version;

        state.add_peer(endpoint, "laptop".to_owned()).unwrap();
        assert_eq!(state.peer_name(&endpoint), Some("laptop"));
        assert!(state.peers[&endpoint].version > tombstone_version);
    }

    #[test]
    fn removal_resolves_ambiguous_names_by_endpoint() {
        let mut state = MeshState::default();
        let (a, b) = (
            SecretKey::generate().public(),
            SecretKey::generate().public(),
        );

        // Duplicate names are allowed: machines are keyed by endpoint.
        state.add_peer(a, "laptop".to_owned()).unwrap();
        state.add_peer(b, "laptop".to_owned()).unwrap();

        assert!(state.remove_peer("laptop").is_err());
        assert_eq!(state.remove_peer(&a.to_string()).unwrap(), a);
        assert_eq!(state.remove_peer("laptop").unwrap(), b);
    }

    #[test]
    fn merge_is_versioned_with_removed_winning_ties() {
        let local = SecretKey::generate().public();
        let peer = SecretKey::generate().public();
        let alive = |version, name: &str| Peer {
            version,
            status: PeerStatus::Alive {
                name: name.to_owned(),
                server: false,
            },
        };
        let removed = |version| Peer {
            version,
            status: PeerStatus::Removed,
        };
        let membership = |record: Peer| Membership {
            peers: BTreeMap::from([(peer, record)]),
            repos: BTreeMap::new(),
            ..Default::default()
        };

        let mut state = MeshState::default();
        state.merge_membership(&membership(alive(2, "laptop")), &local);
        assert_eq!(state.peer_name(&peer), Some("laptop"));

        // A lower version never downgrades the record...
        state.merge_membership(&membership(alive(1, "aaa")), &local);
        assert_eq!(state.peer_name(&peer), Some("laptop"));

        // ...an equal version only wins with a smaller name, so every
        // machine settles on the same one...
        state.merge_membership(&membership(alive(2, "zzz")), &local);
        assert_eq!(state.peer_name(&peer), Some("laptop"));
        state.merge_membership(&membership(alive(2, "aaa")), &local);
        assert_eq!(state.peer_name(&peer), Some("aaa"));

        // ...re-merging the same membership changes nothing, which is what
        // stops the gossip from echoing forever...
        let before = state.clone();
        state.merge_membership(&membership(alive(2, "aaa")), &local);
        assert_eq!(state, before);

        // ...a removal at the same version wins the tie...
        state.merge_membership(&membership(removed(2)), &local);
        assert_eq!(state.peer_name(&peer), None);

        // ...and a higher version supersedes the tombstone (re-pairing).
        state.merge_membership(&membership(alive(3, "laptop")), &local);
        assert_eq!(state.peer_name(&peer), Some("laptop"));
    }

    #[test]
    fn own_record_keeps_its_name_and_outranks_copies() {
        let local = SecretKey::generate().public();
        let alive = |version, name: &str| Peer {
            version,
            status: PeerStatus::Alive {
                name: name.to_owned(),
                server: false,
            },
        };
        let about_us = |record: Peer| Membership {
            peers: BTreeMap::from([(local, record)]),
            repos: BTreeMap::new(),
            ..Default::default()
        };

        let mut state = MeshState::default();
        state.rename_machine("desk".to_owned()).unwrap();
        assert_eq!(state.machine.version, 1);
        assert_eq!(state.membership(local).peers[&local], alive(1, "desk"));

        // A copy under our name is only matched...
        state.merge_membership(&about_us(alive(3, "desk")), &local);
        assert_eq!(state.machine, Machine::new("desk", 3));

        // ...one under another name is outranked, so ours wins the merge
        // everywhere...
        state.merge_membership(&about_us(alive(5, "stale")), &local);
        assert_eq!(state.machine, Machine::new("desk", 6));
        assert!(alive(6, "desk").outranks(&alive(5, "stale")));

        // ...as is one with another role.
        state.set_server(true).unwrap();
        assert_eq!(state.machine.version, 7);
        state.merge_membership(&about_us(alive(7, "desk")), &local);
        assert_eq!(state.machine.version, 8);
        assert!(state.membership(local).peers[&local].is_server());
        state.set_server(false).unwrap();
        assert_eq!(state.machine, Machine::new("desk", 9));

        // ...and a tombstone is matched, not outranked: the removed machine
        // cannot undo its removal.
        let removed = Peer {
            version: 10,
            status: PeerStatus::Removed,
        };
        state.merge_membership(&about_us(removed.clone()), &local);
        assert_eq!(state.machine, Machine::new("desk", 10));
        assert!(!state.membership(local).peers[&local].outranks(&removed));

        // Records about ourselves never land in the peer map.
        assert!(!state.peers.contains_key(&local));

        // A copy parked next to the version ceiling is clamped: what we
        // gossip stays below it (records at the ceiling are skipped by
        // every merge), and the parked record is reported on rename
        // instead of freezing us silently.
        state.merge_membership(&about_us(alive(MAX_RECORD_VERSION - 1, "pwned")), &local);
        assert_eq!(state.machine.version, MAX_RECORD_VERSION - 1);
        assert!(state.membership(local).peers[&local].version < MAX_RECORD_VERSION);
        assert!(state.rename_machine("frozen".to_owned()).is_err());
    }

    #[test]
    fn rename_validates_and_bumps_once_per_change() {
        let mut state = MeshState::default();
        assert!(state.rename_machine(String::new()).is_err());
        assert!(state.rename_machine("a\u{200B}b".to_owned()).is_err());

        state.rename_machine("desk".to_owned()).unwrap();
        state.rename_machine("desk".to_owned()).unwrap();
        assert_eq!(state.machine, Machine::new("desk", 1));
    }

    #[test]
    fn merge_bounds_the_state_it_adopts() {
        let local = SecretKey::generate().public();
        let mut state = MeshState::default();
        let repos = |status: MeshRepoStatus| -> BTreeMap<RepoId, MeshRepo> {
            (0..MAX_MESH_REPOS)
                .map(|_| {
                    let record = MeshRepo {
                        version: 1,
                        status: status.clone(),
                    };
                    (RepoId::generate(), record)
                })
                .collect()
        };

        // A peer cannot grow our state past the caps: beyond them, new
        // entries are dropped rather than making our own membership
        // ungossipable (it would exceed the wire limit).
        for _ in 0..3 {
            let peers = (0..MAX_MESH_PEERS)
                .map(|_| {
                    (
                        SecretKey::generate().public(),
                        Peer {
                            version: 1,
                            status: PeerStatus::Alive {
                                name: "flood".to_owned(),
                                server: false,
                            },
                        },
                    )
                })
                .collect();
            let repos = repos(present(1, "flood").status);
            let membership = Membership {
                peers,
                repos,
                ..Default::default()
            };
            state.merge_membership(&membership, &local);
        }
        assert_eq!(state.peers.len(), MAX_OTHER_PEERS);
        assert_eq!(state.mesh_repo_names().count(), MAX_MESH_REPOS);
        // With our own record added, the membership stays within the wire
        // cap.
        assert_eq!(state.membership(local).peers.len(), MAX_MESH_PEERS);

        // Tombstones fill the records up to their own cap.
        for _ in 0..MAX_MESH_REPO_RECORDS / MAX_MESH_REPOS {
            let repos = repos(MeshRepoStatus::Removed);
            let membership = Membership {
                repos,
                ..Default::default()
            };
            state.merge_membership(&membership, &local);
        }
        assert_eq!(state.mesh_repos.len(), MAX_MESH_REPO_RECORDS);
    }

    #[test]
    fn merge_rejects_versions_that_would_freeze_a_record() {
        let local = SecretKey::generate().public();
        let peer = SecretKey::generate().public();

        // A record parked at an unreachable version could never be
        // superseded, locking the machine out of the mesh for good. The
        // ceiling itself must be refused too: a record *at* it leaves the
        // local bump no headroom.
        for version in [u64::MAX, MAX_RECORD_VERSION] {
            let mut state = MeshState::default();
            state.merge_membership(
                &Membership {
                    peers: BTreeMap::from([(
                        peer,
                        Peer {
                            version,
                            status: PeerStatus::Removed,
                        },
                    )]),
                    repos: BTreeMap::new(),
                    ..Default::default()
                },
                &local,
            );
            assert!(state.peers.is_empty(), "version {version} must be refused");

            // Pairing therefore still works.
            state.add_peer(peer, "laptop".to_owned()).unwrap();
            assert_eq!(state.peer_name(&peer), Some("laptop"));
            // And so does removing it again.
            state.remove_peer("laptop").unwrap();
        }
    }

    /// The machines holding a repo correct the object hash the mesh
    /// records for it.
    #[test]
    fn object_hash_records_are_corrected() {
        let id = RepoId::generate();
        let record = present(3, "proj");
        let mut state = MeshState::default();
        state.mesh_repos.insert(id.clone(), record.clone());
        // Only a registered repo is corrected.
        state.set_object_hash(&id, ObjectHash::Sha256).unwrap();
        assert_eq!(state.mesh_repos[&id], record);

        let repo = Repo {
            path: "/p".into(),
            bare: false,
        };
        state.repos.insert(id.clone(), repo);
        state.set_object_hash(&id, ObjectHash::Sha256).unwrap();
        let corrected = state.mesh_repos[&id].clone();
        assert_eq!(corrected.present(), Some(("proj", ObjectHash::Sha256)));
        assert!(corrected.outranks(&record));
        state.set_object_hash(&id, ObjectHash::Sha256).unwrap();
        assert_eq!(state.mesh_repos[&id], corrected);
    }

    /// Concurrent changes to a repo's record settle on the same one
    /// everywhere, whichever each machine held first.
    #[test]
    fn concurrent_records_converge() {
        let local = SecretKey::generate().public();
        let id = RepoId::generate();
        let membership = |name| Membership {
            repos: BTreeMap::from([(id.clone(), present(2, name))]),
            ..Default::default()
        };

        let mut a = MeshState::default();
        a.mesh_repos.insert(id.clone(), present(2, "a"));
        a.merge_membership(&membership("b"), &local);
        let mut b = MeshState::default();
        b.mesh_repos.insert(id.clone(), present(2, "b"));
        b.merge_membership(&membership("a"), &local);

        assert_eq!(a.repo_name(&id), Some("a"));
        assert_eq!(b.repo_name(&id), Some("a"));
    }

    /// A removed repo stays removed, whatever version a concurrent change
    /// carries: ids are never reused.
    #[test]
    fn tombstones_are_final() {
        let local = SecretKey::generate().public();
        let mut state = MeshState::default();
        let id = add(&mut state, "proj");
        state.remove_repo(&id).unwrap();

        let membership = Membership {
            repos: BTreeMap::from([(id.clone(), present(9, "proj"))]),
            ..Default::default()
        };
        state.merge_membership(&membership, &local);
        assert_eq!(state.repo_name(&id), None);
    }

    /// Repos added concurrently under one name are all kept: each keeps
    /// syncing, and the name shows as a conflict.
    #[test]
    fn merge_keeps_repos_sharing_a_name() {
        let local = SecretKey::generate().public();
        let mut state = MeshState::default();
        let ours = add(&mut state, "proj");
        let theirs = RepoId::generate();
        let membership = Membership {
            repos: BTreeMap::from([
                (theirs.clone(), present(1, "proj")),
                (RepoId::generate(), present(1, "bad\u{202E}name")),
            ]),
            ..Default::default()
        };
        state.merge_membership(&membership, &local);

        assert_eq!(state.mesh_repos.len(), 2);
        let mut both = vec![&ours, &theirs];
        both.sort();
        assert_eq!(state.name_conflicts()["proj"], both);
        // The name selects the repo registered here, ids select either.
        assert_eq!(state.resolve_repo("proj").unwrap(), ours);
        state.forget_repo(&ours).unwrap();
        let err = state.resolve_repo("proj").unwrap_err();
        assert!(err.to_string().contains(&theirs.to_string()), "{err:#}");
        assert_eq!(state.resolve_repo(&theirs.to_string()).unwrap(), theirs);
    }

    #[test]
    fn adding_a_repo_known_to_the_mesh_requires_cloning() {
        let mut state = MeshState::default();
        state
            .mesh_repos
            .insert(RepoId::generate(), present(1, "proj"));

        let repo = Repo {
            path: "/p".into(),
            bare: false,
        };
        let err = state
            .add_repo(
                RepoId::generate(),
                "proj".to_owned(),
                repo,
                ObjectHash::Sha1,
            )
            .unwrap_err();
        assert!(err.to_string().contains("Clone it"), "{err:#}");
    }

    #[test]
    fn removing_a_repo_tombstones_it_and_frees_the_name() {
        let mut state = MeshState::default();
        let first = add(&mut state, "proj");
        assert!(state.remove_repo(&first).unwrap(), "it was registered here");

        // The record stays as a tombstone, so a machine that missed the
        // removal cannot resurrect the repo.
        assert!(state.repos.is_empty());
        assert_eq!(state.repo_name(&first), None);
        assert!(state.mesh_repos.contains_key(&first));
        assert!(state.remove_repo(&first).is_err());
        assert!(state.resolve_repo("proj").is_err());

        // And the name can be reused by another repo.
        let second = add(&mut state, "proj");
        assert_eq!(state.resolve_repo("proj").unwrap(), second);
    }

    #[test]
    fn forgetting_locally_keeps_the_mesh_record() {
        let mut state = MeshState::default();
        let id = add(&mut state, "proj");

        let repo = state.forget_repo(&id).unwrap();
        assert_eq!(repo.path, PathBuf::from("/tmp/proj"));
        assert!(state.repos.is_empty());
        assert!(state.forget_repo(&id).is_err());

        // The mesh still knows the repo: it stays clonable here, and a
        // plain re-add would fork the name.
        assert_eq!(state.clonable_repos().collect::<Vec<_>>(), [(&id, "proj")]);
        let err = state
            .add_repo(
                RepoId::generate(),
                "proj".to_owned(),
                repo,
                ObjectHash::Sha1,
            )
            .unwrap_err();
        assert!(err.to_string().contains("Clone it"), "{err:#}");
    }

    #[test]
    fn a_gossiped_tombstone_unregisters_the_repo() {
        let local = SecretKey::generate().public();
        let mut state = MeshState::default();
        let id = add(&mut state, "proj");

        // Another machine removed the repo: we stop syncing it, and a stale
        // copy of the old record cannot bring it back.
        let removed = MeshRepo {
            version: state.mesh_repos[&id].version + 1,
            status: MeshRepoStatus::Removed,
        };
        let membership = |record: MeshRepo| Membership {
            repos: BTreeMap::from([(id.clone(), record)]),
            ..Default::default()
        };
        let unregistered = state.merge_membership(&membership(removed), &local);
        assert_eq!(unregistered, ["proj"]);
        assert!(state.repos.is_empty());

        state.merge_membership(&membership(present(1, "proj")), &local);
        assert_eq!(state.repo_name(&id), None);
    }

    /// Registering the same directory under another spelling (here: a
    /// symlink) is rejected: paths compare canonicalized.
    #[test]
    fn duplicate_paths_are_rejected_canonically() {
        let tmp = tempfile::tempdir().unwrap();
        let real = tmp.path().join("repo");
        fs::create_dir_all(&real).unwrap();
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let mut state = MeshState::default();
        let repo = Repo {
            path: real,
            bare: false,
        };
        state
            .add_repo(RepoId::generate(), "a".to_owned(), repo, ObjectHash::Sha1)
            .unwrap();

        let err = state.validate_new_repo("b", &link).unwrap_err();
        assert!(err.to_string().contains("already added"), "{err:#}");
    }

    #[test]
    fn duplicate_repo_ids_are_rejected() {
        let mut state = MeshState::default();
        let id = add(&mut state, "a");
        let repo = Repo {
            path: "/b".into(),
            bare: false,
        };
        assert!(
            state
                .add_repo(id, "b".to_owned(), repo, ObjectHash::Sha1)
                .is_err()
        );
    }

    /// A format 1 file, keyed by repo name, migrates on load.
    #[test]
    fn legacy_states_migrate() {
        let (proj, other, local, mesh) = (
            RepoId::generate(),
            RepoId::generate(),
            RepoId::generate(),
            RepoId::generate(),
        );
        let json = format!(
            r#"{{
                "machine": {{ "name": "laptop", "version": 2 }},
                "repos": {{
                    "proj": {{ "id": "{proj}", "path": "/proj" }},
                    "clash": {{ "id": "{local}", "path": "/clash" }}
                }},
                "mesh_repos": {{
                    "proj": {{ "version": 3, "status": {{ "present": {{ "id": "{proj}" }} }} }},
                    "other": {{
                        "version": 1,
                        "status": {{ "present": {{ "id": "{other}", "object_hash": "sha256" }} }}
                    }},
                    "clash": {{ "version": 1, "status": {{ "present": {{ "id": "{mesh}" }} }} }},
                    "gone": {{ "version": 2, "status": "removed" }}
                }}
            }}"#
        );
        let (state, migrated) = MeshState::parse(json.as_bytes()).unwrap();
        assert!(migrated);

        assert_eq!(state.format, FORMAT);
        assert_eq!(state.machine.name, "laptop");
        assert_eq!(state.repos.len(), 2);
        assert_eq!(state.repos[&proj].path, PathBuf::from("/proj"));
        assert_eq!(state.mesh_repos[&proj], present(3, "proj"));
        assert_eq!(
            state.mesh_repos[&other].present(),
            Some(("other", ObjectHash::Sha256))
        );
        // The repo registered under a name the mesh gave another repo keeps
        // syncing, both showing as a name conflict; the tombstone is lost.
        assert_eq!(state.name_conflicts()["clash"].len(), 2);
        assert_eq!(state.mesh_repos.len(), 4);
        assert_eq!(state.resolve_repo("clash").unwrap(), local);

        let newer = br#"{ "format": 99 }"#;
        let err = MeshState::parse(newer).unwrap_err();
        assert!(err.to_string().contains("Update jj-mesh"), "{err:#}");
    }

    #[test]
    fn alive_endpoints_cannot_be_paired_twice() {
        let mut state = MeshState::default();
        let endpoint = SecretKey::generate().public();

        state.add_peer(endpoint, "laptop".to_owned()).unwrap();
        assert!(state.add_peer(endpoint, "desktop".to_owned()).is_err());
    }

    /// Claims of `names` on repo `id`, at `version`.
    fn claims(version: u64, id: &RepoId, names: &[&str]) -> WorkspaceClaims {
        let names = names.iter().map(|name| (*name).to_owned()).collect();
        WorkspaceClaims {
            version,
            repos: BTreeMap::from([(id.clone(), names)]),
        }
    }

    #[test]
    fn merges_claims_of_alive_peers_only() {
        let local = SecretKey::generate().public();
        let (peer, stranger) = (
            SecretKey::generate().public(),
            SecretKey::generate().public(),
        );
        let id = RepoId::generate();
        let mut state = MeshState::default();
        state.add_peer(peer, "desktop".to_owned()).unwrap();
        let membership = |endpoint, record| Membership {
            claims: BTreeMap::from([(endpoint, record)]),
            ..Default::default()
        };

        state.merge_membership(&membership(stranger, claims(1, &id, &["x"])), &local);
        assert!(state.peer_claims.is_empty());

        state.merge_membership(&membership(peer, claims(2, &id, &["x"])), &local);
        state.merge_membership(&membership(peer, claims(1, &id, &["y"])), &local);
        assert_eq!(state.repo_claims(&id).others["x"], ["desktop"]);
        assert!(!state.repo_claims(&id).others.contains_key("y"));

        // Invalid names poison the whole record.
        state.merge_membership(&membership(peer, claims(3, &id, &["bad\u{202E}"])), &local);
        assert!(state.repo_claims(&id).others.contains_key("x"));

        state.remove_peer("desktop").unwrap();
        assert!(state.peer_claims.is_empty());
    }

    #[test]
    fn own_claims_outrank_gossiped_copies_and_are_released() {
        let local = SecretKey::generate().public();
        let mut state = MeshState::default();
        let id = add(&mut state, "proj");
        state
            .set_claims(&id, BTreeSet::from(["laptop".to_owned()]))
            .unwrap();

        // A stale copy of our record with other content is outranked.
        let copy = Membership {
            claims: BTreeMap::from([(local, claims(5, &id, &["old"]))]),
            ..Default::default()
        };
        state.merge_membership(&copy, &local);
        assert_eq!(state.claims.version, 6);
        assert_eq!(
            state.repo_claims(&id).ours,
            BTreeSet::from(["laptop".to_owned()])
        );

        // Claims on unregistered repos are refused, and forgetting a repo
        // releases its claims.
        let other = RepoId::generate();
        state
            .set_claims(&other, BTreeSet::from(["x".to_owned()]))
            .unwrap();
        assert!(!state.claims.repos.contains_key(&other));
        state.forget_repo(&id).unwrap();
        assert!(state.claims.repos.is_empty());
    }

    #[test]
    fn caps_own_claims() {
        let mut state = MeshState::default();
        let id = add(&mut state, "proj");
        let names = |n: usize| (0..n).map(|i| format!("w{i}")).collect::<BTreeSet<_>>();

        assert!(state.set_claims(&id, names(MAX_MACHINE_WORKSPACES)).is_ok());
        assert!(
            state
                .set_claims(&id, names(MAX_MACHINE_WORKSPACES + 1))
                .is_err()
        );
    }
}

//! Mesh state file (`peers.json`).
//!
//! This file holds the machine's copy of the mesh state, in two parts:
//! what is replicated across the mesh by the membership gossip (the peer
//! records, tombstones included, and the mesh-wide repo list) and what is
//! strictly local (the repos registered here, with their paths).
//!
//! It is owned and written exclusively by the daemon; the CLI mutates it
//! through the control socket and only reads it directly as a fallback
//! when no daemon is running.

use std::{
    cmp,
    collections::BTreeMap,
    fmt, fs, io,
    io::Write as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};
use data_encoding::HEXLOWER;
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::ConfigDir;

/// Maximum length of a peer or repo name, in bytes.
///
/// Names can be announced by remote machines and end up as terminal output,
/// so they are kept short and free of control characters.
pub const MAX_NAME_LEN: usize = 64;

/// Cap on machines tracked in the mesh state, tombstones included. A
/// personal mesh is a handful of machines; the cap keeps a peer from
/// growing the state file (and the membership we must be able to gossip)
/// without bound.
pub const MAX_MESH_PEERS: usize = 256;

/// Cap on repos tracked in the mesh-wide repo list, same reasoning.
pub const MAX_MESH_REPOS: usize = 1024;

/// Cap on a peer record's version. Versions only ever advance by one per
/// local change, so a record beyond this is corrupted or hostile: without
/// the cap, a record parked at `u64::MAX` could never be superseded again,
/// permanently freezing a machine out of the mesh.
const MAX_PEER_VERSION: u64 = 1 << 32;

/// Checks that a peer or repo name is usable; `kind` names it in errors.
/// Also applied to names arriving from remote machines (announcements),
/// which must never carry unbounded length or confusable characters.
pub(crate) fn validate_name(kind: &str, name: &str) -> Result<()> {
    ensure!(!name.is_empty(), "{kind} name cannot be empty");
    ensure!(
        name.len() <= MAX_NAME_LEN,
        "{kind} name is longer than {MAX_NAME_LEN} bytes",
    );
    ensure!(
        !name.chars().any(is_confusable),
        "{kind} name contains control or invisible characters",
    );

    Ok(())
}

/// Whether a character can hide or reorder text in terminal output: controls
/// (`is_control` covers only `Cc`), zero-width and bidi formatting. Names and
/// other peer-supplied strings must never reach the terminal carrying one.
pub(crate) fn is_confusable(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}'
        )
}

/// This machine's copy of the mesh state.
///
/// `peers` and `mesh_repos` are replicated across the mesh by the
/// membership gossip; `repos` (with its local paths) never leaves this
/// machine.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MeshState {
    /// Machines of the mesh, keyed by endpoint id. Includes tombstones
    /// (removed peers), which gossip must remember so a removal cannot be
    /// undone by a machine that missed it.
    pub peers: BTreeMap<EndpointId, Peer>,
    /// Repos registered on this machine, keyed by their mesh-wide name.
    pub repos: BTreeMap<String, Repo>,
    /// Every repo the mesh knows, registered here or not. Includes
    /// tombstones (forgotten repos), which gossip must remember so a
    /// machine that missed the removal cannot resurrect the name.
    pub mesh_repos: BTreeMap<String, MeshRepo>,
}

impl MeshState {
    /// Loads `peers.json` from the configuration directory, defaulting to an
    /// empty state if the file does not exist yet.
    pub fn load(dir: &ConfigDir) -> Result<Self> {
        let path = dir.peers_file();

        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .wrap_err_with(|| format!("invalid mesh state in {}", path.display())),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Self::default()),
            Err(err) => Err(err).wrap_err_with(|| format!("cannot read {}", path.display())),
        }
    }

    /// Saves the state durably: written to a private temp file (0600,
    /// exclusive), synced, then renamed into place and the directory synced.
    /// Readers never see a partial file, and the state survives a crash once
    /// this returns; pairing relies on that to only confirm saved peers.
    pub fn save(&self, dir: &ConfigDir) -> Result<()> {
        let path = dir.peers_file();
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

    /// The name under which a repo id is registered, if any.
    pub fn repo_name(&self, id: &RepoId) -> Option<&str> {
        self.repos
            .iter()
            .find(|(_, repo)| &repo.id == id)
            .map(|(name, _)| name.as_str())
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

    /// Checks that a repo can be registered here under this name and path.
    pub fn validate_new_repo(&self, name: &str, path: &Path) -> Result<()> {
        validate_name("repo", name)?;
        ensure!(
            !self.repos.contains_key(name),
            "a repo named `{name}` already exists",
        );
        if let Some((existing, _)) = self.repos.iter().find(|(_, r)| r.path == path) {
            bail!("{} is already added as `{existing}`", path.display());
        }

        Ok(())
    }

    /// The id the mesh knows for a repo name, if the name is live there.
    pub fn mesh_repo_id(&self, name: &str) -> Option<&RepoId> {
        self.mesh_repos.get(name).and_then(MeshRepo::id)
    }

    /// The repo names the mesh knows, in use (not forgotten).
    pub fn mesh_repo_names(&self) -> impl Iterator<Item = &str> {
        self.mesh_repos
            .iter()
            .filter(|(_, repo)| repo.id().is_some())
            .map(|(name, _)| name.as_str())
    }

    /// Checks that registering `name` with this id agrees with the mesh: a
    /// name the mesh already knows may only be registered with its id (that
    /// is what a join does), since anything else forks the name into two
    /// unrelated repos.
    pub fn ensure_mesh_id(&self, name: &str, id: &RepoId) -> Result<()> {
        if let Some(mesh_id) = self.mesh_repo_id(name)
            && mesh_id != id
        {
            bail!("a repo named `{name}` already exists in the mesh; join it instead");
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
            self.peers.contains_key(&endpoint) || self.peers.len() < MAX_MESH_PEERS,
            "the mesh already has {MAX_MESH_PEERS} machines",
        );

        let version = self.bumped_version(&endpoint)?;
        self.peers.insert(
            endpoint,
            Peer {
                version,
                status: PeerStatus::Alive { name },
            },
        );

        Ok(())
    }

    /// Tombstones an alive peer, resolved by name or full endpoint id.
    /// The tombstone propagates the removal through the gossip.
    pub fn remove_peer(&mut self, selector: &str) -> Result<EndpointId> {
        let endpoint = self.resolve_peer(selector)?;
        let version = self.bumped_version(&endpoint)?;

        let peer = self.peers.get_mut(&endpoint).expect("resolved above");
        peer.version = version;
        peer.status = PeerStatus::Removed;

        Ok(endpoint)
    }

    /// The version a local change to this record must carry to outrank the
    /// stored one. Refuses to go past [`MAX_PEER_VERSION`]: a saturating
    /// bump would make the record unchangeable, so a record parked at the
    /// ceiling is reported instead of silently freezing the peer out.
    fn bumped_version(&self, endpoint: &EndpointId) -> Result<u64> {
        let version = self.peers.get(endpoint).map_or(0, |peer| peer.version);
        ensure!(
            version < MAX_PEER_VERSION,
            "the record of {endpoint} is corrupted (version {version}); \
             remove it from peers.json on every machine",
        );

        Ok(version + 1)
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
                "several peers are named `{selector}`; use the endpoint id \
                 (see `jj-mesh peers`)"
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

    /// Adds a repo, rejecting duplicate names, paths and ids, and records
    /// it in the mesh-wide repo list, superseding any tombstone.
    pub fn add_repo(&mut self, name: String, repo: Repo) -> Result<()> {
        self.validate_new_repo(&name, &repo.path)?;
        self.ensure_mesh_id(&name, &repo.id)?;
        if let Some(existing) = self.repo_name(&repo.id) {
            bail!("repo {} is already registered as `{existing}`", repo.id);
        }
        ensure!(
            self.mesh_repos.contains_key(&name) || self.mesh_repos.len() < MAX_MESH_REPOS,
            "the mesh already has {MAX_MESH_REPOS} repos",
        );

        let version = self.bumped_repo_version(&name)?;
        self.mesh_repos.insert(
            name.clone(),
            MeshRepo {
                version,
                status: MeshRepoStatus::Present {
                    id: repo.id.clone(),
                },
            },
        );
        self.repos.insert(name, repo);

        Ok(())
    }

    /// Retires a repo name from the mesh: tombstones the mesh record and
    /// unregisters the repo here. The tombstone propagates the removal, so
    /// every machine stops syncing the repo; none of them touch its files.
    /// Returns whether the repo was registered on this machine.
    pub fn forget_repo(&mut self, name: &str) -> Result<bool> {
        ensure!(
            self.mesh_repos.contains_key(name) || self.repos.contains_key(name),
            "no repo named `{name}` in the mesh",
        );
        ensure!(
            self.mesh_repo_id(name).is_some() || self.repos.contains_key(name),
            "repo `{name}` is already forgotten",
        );

        let version = self.bumped_repo_version(name)?;
        self.mesh_repos.insert(
            name.to_owned(),
            MeshRepo {
                version,
                status: MeshRepoStatus::Forgotten,
            },
        );

        Ok(self.repos.remove(name).is_some())
    }

    /// The version a local change to a mesh repo record must carry to
    /// outrank the stored one; see [`Self::bumped_version`].
    fn bumped_repo_version(&self, name: &str) -> Result<u64> {
        let version = self.mesh_repos.get(name).map_or(0, |repo| repo.version);
        ensure!(
            version < MAX_PEER_VERSION,
            "the mesh record of `{name}` is corrupted (version {version}); \
             remove it from peers.json on every machine",
        );

        Ok(version + 1)
    }

    /// The membership this machine gossips: every peer record (tombstones
    /// included) and the mesh-wide repo list.
    pub fn membership(&self) -> Membership {
        Membership {
            peers: self.peers.clone(),
            repos: self.mesh_repos.clone(),
        }
    }

    /// Merges a peer's membership into ours. `local` is this machine's own
    /// endpoint: records about ourselves are not ours to store.
    ///
    /// Peer and mesh repo records are versioned registers: the higher
    /// version wins, and ties resolve to the retired state (removed,
    /// forgotten), then to the smaller name or id, so every machine
    /// converges on the same record without clocks. Adopting a forgotten
    /// repo also unregisters it here: that is how the removal reaches the
    /// machines that hold it.
    ///
    /// The merged maps are capped: a peer is authenticated but not trusted
    /// to grow our state file (which we must keep gossipable) without
    /// bound, so *new* entries stop being accepted at the cap while updates
    /// to known ones keep flowing. At the cap machines can disagree on
    /// which entries they hold, which is the accepted trade: the caps sit
    /// far above a personal mesh, and an unbounded state file breaks
    /// gossip outright.
    pub fn merge_membership(&mut self, remote: &Membership, local: &EndpointId) {
        for (endpoint, record) in &remote.peers {
            // Strictly below the ceiling: a record *at* it could never be
            // superseded by a local change, freezing the machine's status.
            if endpoint == local || record.version >= MAX_PEER_VERSION {
                continue;
            }
            if let PeerStatus::Alive { name } = &record.status
                && validate_name("peer", name).is_err()
            {
                continue;
            }

            match self.peers.get(endpoint) {
                Some(ours) if record_rank(record) > record_rank(ours) => {
                    self.peers.insert(*endpoint, record.clone());
                }
                Some(_) => {}
                None if self.peers.len() < MAX_MESH_PEERS => {
                    self.peers.insert(*endpoint, record.clone());
                }
                None => debug!("dropping gossiped peer {endpoint}: the mesh is full"),
            }
        }

        for (name, record) in &remote.repos {
            if validate_name("repo", name).is_err() || record.version >= MAX_PEER_VERSION {
                continue;
            }

            let adopt = match self.mesh_repos.get(name) {
                // Two unrelated repos claiming one name is a conflict only
                // the user can settle, but the list must still converge, so
                // the ranking decides it everywhere the same way. The
                // sync-level conflict (see `daemon::hub`) surfaces it.
                Some(ours) => mesh_repo_rank(record) > mesh_repo_rank(ours),
                None if self.mesh_repos.len() < MAX_MESH_REPOS => true,
                None => {
                    debug!(repo = %name, "dropping gossiped repo: the mesh repo list is full");
                    false
                }
            };
            if !adopt {
                continue;
            }

            self.mesh_repos.insert(name.clone(), record.clone());
            // A repo forgotten mesh-wide stops being synced here; its
            // files stay where they are.
            if record.id().is_none() && self.repos.remove(name).is_some() {
                debug!(repo = %name, "repo forgotten on the mesh; no longer syncing it");
            }
        }
    }
}

/// Total order on peer records deciding which one a merge keeps: higher
/// version first, removal beating alive on ties, then the smaller name (so
/// equal-version renames converge mesh-wide instead of ping-ponging).
fn record_rank(peer: &Peer) -> (u64, bool, cmp::Reverse<&str>) {
    (
        peer.version,
        peer.name().is_none(),
        cmp::Reverse(peer.name().unwrap_or("")),
    )
}

/// The same total order for mesh repo records: higher version first,
/// forgetting beating presence on ties, then the smaller id.
fn mesh_repo_rank(repo: &MeshRepo) -> (u64, bool, cmp::Reverse<Option<&RepoId>>) {
    (repo.version, repo.id().is_none(), cmp::Reverse(repo.id()))
}

/// A machine's record in the mesh, replicated by the membership gossip as
/// a versioned register (see [`MeshState::merge_membership`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    /// Bumped on every local change to the record, so the change outranks
    /// the state every other machine holds.
    pub version: u64,
    pub status: PeerStatus,
}

impl Peer {
    /// The machine's name while it is part of the mesh; `None` for a
    /// tombstone.
    pub fn name(&self) -> Option<&str> {
        match &self.status {
            PeerStatus::Alive { name } => Some(name),
            PeerStatus::Removed => None,
        }
    }
}

/// Whether a machine is part of the mesh. `Removed` is a tombstone: it
/// must be remembered (and gossiped) so a machine that missed the removal
/// cannot resurrect the peer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Alive { name: String },
    Removed,
}

/// A repo's record in the mesh-wide list, replicated by the membership
/// gossip as a versioned register like [`Peer`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeshRepo {
    /// Bumped on every local change to the record.
    pub version: u64,
    pub status: MeshRepoStatus,
}

/// Whether a repo name is in use on the mesh. `Forgotten` is a tombstone:
/// remembering it is what keeps a machine that missed the removal from
/// resurrecting the name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeshRepoStatus {
    Present { id: RepoId },
    Forgotten,
}

impl MeshRepo {
    /// The repo's mesh id while the name is in use; `None` for a tombstone.
    pub fn id(&self) -> Option<&RepoId> {
        match &self.status {
            MeshRepoStatus::Present { id } => Some(id),
            MeshRepoStatus::Forgotten => None,
        }
    }
}

/// The membership one machine gossips: its whole view of the mesh. Sent as
/// idempotent latest-wins snapshots (on connect, on every change, and
/// periodically), so a lost message is healed by a later one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub peers: BTreeMap<EndpointId, Peer>,
    /// The mesh-wide repo list, tombstones included.
    pub repos: BTreeMap<String, MeshRepo>,
}

/// A repo registered on this machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Repo {
    /// Mesh-wide identifier of the repo.
    pub id: RepoId,
    /// Local repository root (the directory containing `.jj`).
    pub path: PathBuf,
}

/// Mesh-wide identifier of a repo, shared by all machines syncing it. Randomly
/// generated.
///
/// Ids also arrive from remote machines (sync announcements), so
/// deserialization enforces the generated form: names and ids crossing that
/// boundary must never carry control characters or unbounded length.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct RepoId(String);

/// Length of a repo id in hex characters (16 random bytes).
const REPO_ID_LEN: usize = 32;

impl RepoId {
    /// Generates a random id repo id.
    pub fn generate() -> Self {
        RepoId(HEXLOWER.encode(&rand::random::<[u8; 16]>()))
    }
}

impl TryFrom<String> for RepoId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let valid = value.len() == REPO_ID_LEN
            && value
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if valid {
            Ok(RepoId(value))
        } else {
            Err(format!(
                "repo ids are {REPO_ID_LEN} lowercase hex characters"
            ))
        }
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    /// A mesh repo record in use.
    fn present(version: u64, id: &RepoId) -> MeshRepo {
        MeshRepo {
            version,
            status: MeshRepoStatus::Present { id: id.clone() },
        }
    }

    #[test]
    fn repo_id_rejects_non_generated_forms() {
        let ok = postcard::to_stdvec(&RepoId::generate()).unwrap();
        assert!(postcard::from_bytes::<RepoId>(&ok).is_ok());

        for bad in ["", "short", &"a".repeat(33), &"Z".repeat(32), "e\x1b[2K\n"] {
            let bytes = postcard::to_stdvec(&bad).unwrap();
            assert!(
                postcard::from_bytes::<RepoId>(&bytes).is_err(),
                "{bad:?} must be rejected",
            );
        }
    }

    #[test]
    fn save_and_load_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ConfigDir::new(Some(tmp.path().to_owned())).unwrap();
        let endpoint = SecretKey::generate().public();

        let mut state = MeshState::load(&dir).unwrap();
        assert!(state.peers.is_empty());
        state.add_peer(endpoint, "laptop".to_owned()).unwrap();
        state
            .add_repo(
                "project".to_owned(),
                Repo {
                    id: RepoId::generate(),
                    path: tmp.path().join("project"),
                },
            )
            .unwrap();
        state.save(&dir).unwrap();

        let mut state = MeshState::load(&dir).unwrap();
        assert_eq!(state.peer_name(&endpoint), Some("laptop"));
        assert!(state.repos.contains_key("project"));
        assert_eq!(state.mesh_repos.len(), 1);

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
            },
        };
        let removed = |version| Peer {
            version,
            status: PeerStatus::Removed,
        };
        let membership = |record: Peer| Membership {
            peers: BTreeMap::from([(peer, record)]),
            repos: BTreeMap::new(),
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

        // Records about ourselves are ignored.
        state.merge_membership(
            &Membership {
                peers: BTreeMap::from([(local, removed(9))]),
                repos: BTreeMap::new(),
            },
            &local,
        );
        assert!(!state.peers.contains_key(&local));
    }

    #[test]
    fn merge_bounds_the_state_it_adopts() {
        let local = SecretKey::generate().public();
        let mut state = MeshState::default();

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
                            },
                        },
                    )
                })
                .collect();
            let repos = (0..MAX_MESH_REPOS)
                .map(|n| {
                    (
                        format!("repo{n}-{:?}", RepoId::generate()),
                        present(1, &RepoId::generate()),
                    )
                })
                .collect();
            state.merge_membership(&Membership { peers, repos }, &local);
        }

        assert_eq!(state.peers.len(), MAX_MESH_PEERS);
        assert_eq!(state.mesh_repos.len(), MAX_MESH_REPOS);
    }

    #[test]
    fn merge_rejects_versions_that_would_freeze_a_record() {
        let local = SecretKey::generate().public();
        let peer = SecretKey::generate().public();

        // A record parked at an unreachable version could never be
        // superseded, locking the machine out of the mesh for good. The
        // ceiling itself must be refused too: a record *at* it leaves the
        // local bump no headroom.
        for version in [u64::MAX, MAX_PEER_VERSION] {
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

    #[test]
    fn mesh_repo_id_disagreements_converge() {
        let local = SecretKey::generate().public();
        let (low, high) = {
            let (a, b) = (RepoId::generate(), RepoId::generate());
            if a < b { (a, b) } else { (b, a) }
        };
        let membership = |id: &RepoId| Membership {
            peers: BTreeMap::new(),
            repos: BTreeMap::from([("a".to_owned(), present(1, id))]),
        };

        // Both machines must settle on the same id whichever they held
        // first, or their "available repos" and add/join guards disagree
        // forever.
        let mut holding_low = MeshState::default();
        holding_low
            .mesh_repos
            .insert("a".to_owned(), present(1, &low));
        holding_low.merge_membership(&membership(&high), &local);

        let mut holding_high = MeshState::default();
        holding_high
            .mesh_repos
            .insert("a".to_owned(), present(1, &high));
        holding_high.merge_membership(&membership(&low), &local);

        assert_eq!(holding_low.mesh_repo_id("a"), Some(&low));
        assert_eq!(holding_high.mesh_repo_id("a"), Some(&low));
    }

    #[test]
    fn merge_grows_mesh_repos_and_keeps_ours_on_conflict() {
        let local = SecretKey::generate().public();
        let (ours, theirs) = {
            let (a, b) = (RepoId::generate(), RepoId::generate());
            if a < b { (a, b) } else { (b, a) }
        };

        let mut state = MeshState::default();
        state.mesh_repos.insert("a".to_owned(), present(1, &ours));
        state.merge_membership(
            &Membership {
                peers: BTreeMap::new(),
                repos: BTreeMap::from([
                    ("a".to_owned(), present(1, &theirs)),
                    ("b".to_owned(), present(1, &RepoId::generate())),
                    (
                        "bad\u{202E}name".to_owned(),
                        present(1, &RepoId::generate()),
                    ),
                ]),
            },
            &local,
        );

        assert_eq!(state.mesh_repo_id("a"), Some(&ours));
        assert!(state.mesh_repos.contains_key("b"));
        assert_eq!(state.mesh_repos.len(), 2);
    }

    #[test]
    fn adding_a_repo_known_to_the_mesh_requires_joining() {
        let mut state = MeshState::default();
        state
            .mesh_repos
            .insert("proj".to_owned(), present(1, &RepoId::generate()));

        let err = state
            .add_repo(
                "proj".to_owned(),
                Repo {
                    id: RepoId::generate(),
                    path: "/p".into(),
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("join"), "{err:#}");
    }

    #[test]
    fn forgetting_a_repo_tombstones_it_and_frees_the_name() {
        let mut state = MeshState::default();
        let repo = |id: &RepoId| Repo {
            id: id.clone(),
            path: "/p".into(),
        };

        let first = RepoId::generate();
        state.add_repo("proj".to_owned(), repo(&first)).unwrap();
        assert!(state.forget_repo("proj").unwrap(), "it was registered here");

        // The name is retired, not deleted: it stays as a tombstone so a
        // machine that missed the removal cannot resurrect it.
        assert!(state.repos.is_empty());
        assert_eq!(state.mesh_repo_id("proj"), None);
        assert_eq!(state.mesh_repo_names().count(), 0);
        assert!(state.mesh_repos.contains_key("proj"));
        assert!(state.forget_repo("proj").is_err());
        assert!(state.forget_repo("nope").is_err());

        // And the name can be reused, superseding the tombstone.
        let second = RepoId::generate();
        state.add_repo("proj".to_owned(), repo(&second)).unwrap();
        assert_eq!(state.mesh_repo_id("proj"), Some(&second));
        assert!(state.mesh_repos["proj"].version > 2);
    }

    #[test]
    fn a_gossiped_tombstone_unregisters_the_repo() {
        let local = SecretKey::generate().public();
        let id = RepoId::generate();

        let mut state = MeshState::default();
        state
            .add_repo(
                "proj".to_owned(),
                Repo {
                    id: id.clone(),
                    path: "/p".into(),
                },
            )
            .unwrap();

        // Another machine forgot the repo: we stop syncing it, and a stale
        // announcement of the old record cannot bring it back.
        let forgotten = MeshRepo {
            version: state.mesh_repos["proj"].version + 1,
            status: MeshRepoStatus::Forgotten,
        };
        let membership = |record: MeshRepo| Membership {
            peers: BTreeMap::new(),
            repos: BTreeMap::from([("proj".to_owned(), record)]),
        };
        state.merge_membership(&membership(forgotten), &local);
        assert!(state.repos.is_empty());
        assert_eq!(state.mesh_repo_id("proj"), None);

        state.merge_membership(&membership(present(1, &id)), &local);
        assert_eq!(state.mesh_repo_id("proj"), None);
    }

    #[test]
    fn duplicate_repo_ids_are_rejected() {
        let mut state = MeshState::default();
        let id = RepoId::generate();

        state
            .add_repo(
                "a".to_owned(),
                Repo {
                    id: id.clone(),
                    path: "/a".into(),
                },
            )
            .unwrap();
        assert!(
            state
                .add_repo(
                    "b".to_owned(),
                    Repo {
                        id,
                        path: "/b".into()
                    }
                )
                .is_err()
        );
    }

    #[test]
    fn alive_endpoints_cannot_be_paired_twice() {
        let mut state = MeshState::default();
        let endpoint = SecretKey::generate().public();

        state.add_peer(endpoint, "laptop".to_owned()).unwrap();
        assert!(state.add_peer(endpoint, "desktop".to_owned()).is_err());
    }
}

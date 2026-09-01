//! Mesh state file (`mesh.json`).
//!
//! This file holds the machine's copy of the mesh state, in two parts:
//! what is replicated across the mesh by the membership gossip (this
//! machine's own record, the peer records, tombstones included, and the
//! mesh-wide repo list) and what is strictly local (the repos registered
//! here, with their paths).
//!
//! The daemon is the only writer; the CLI mutates it through the control
//! socket, and may read the file directly (for pre-checks and completion),
//! treating what it sees as advisory.

mod machine;
mod membership;
mod repo;

use std::{
    collections::BTreeMap,
    fs, io,
    io::Write as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure};
use iroh::EndpointId;
use serde::{Deserialize, Serialize};
use tracing::debug;

use self::membership::{MAX_OTHER_PEERS, MAX_RECORD_VERSION, bumped_version, should_adopt};
pub use self::{
    machine::Machine,
    membership::{
        MAX_MESH_PEERS, MAX_MESH_REPOS, Membership, MeshRepo, MeshRepoStatus, Peer, PeerStatus,
    },
    repo::{Repo, RepoId},
};
use super::{ConfigDir, validate_name};

/// This machine's copy of the mesh state.
///
/// `machine`, `peers` and `mesh_repos` are replicated across the mesh by
/// the membership gossip; `repos` (with its local paths) never leaves this
/// machine.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MeshState {
    /// This machine's own record.
    pub machine: Machine,
    /// The other machines of the mesh, keyed by endpoint id. Includes
    /// tombstones (removed peers), which gossip must remember so a removal
    /// cannot be undone by a machine that missed it.
    pub peers: BTreeMap<EndpointId, Peer>,
    /// Repos registered on this machine, keyed by their mesh-wide name.
    pub repos: BTreeMap<String, Repo>,
    /// Every repo the mesh knows, registered here or not. Includes
    /// tombstones (removed repos), which gossip must remember so a
    /// machine that missed the removal cannot resurrect the name.
    pub mesh_repos: BTreeMap<String, MeshRepo>,
}

impl MeshState {
    /// Loads `mesh.json` from the configuration directory, defaulting to an
    /// empty state if the file does not exist yet.
    pub fn load(dir: &ConfigDir) -> Result<Self> {
        let path = dir.mesh_file();

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
        // Two spellings of one directory (symlinks, `..`) must not register
        // twice. Stored paths are canonical (see [`Self::add_repo`]), so
        // only the candidate needs resolving; the fallback covers paths
        // that do not resolve right now.
        let path = canonical_path(path);
        if let Some((existing, _)) = self.repos.iter().find(|(_, r)| r.path == path) {
            bail!("{} is already added as `{existing}`", path.display());
        }

        Ok(())
    }

    /// The id the mesh knows for a repo name, if the name is live there.
    pub fn mesh_repo_id(&self, name: &str) -> Option<&RepoId> {
        self.mesh_repos.get(name).and_then(MeshRepo::id)
    }

    /// The repo names the mesh knows, in use (not removed).
    pub fn mesh_repo_names(&self) -> impl Iterator<Item = &str> {
        self.mesh_repos
            .iter()
            .filter(|(_, repo)| repo.id().is_some())
            .map(|(name, _)| name.as_str())
    }

    /// Checks that registering `name` with this id agrees with the mesh: a
    /// name the mesh already knows may only be registered with its id (that
    /// is what a clone does), since anything else forks the name into two
    /// unrelated repos.
    pub fn ensure_mesh_id(&self, name: &str, id: &RepoId) -> Result<()> {
        if let Some(mesh_id) = self.mesh_repo_id(name)
            && mesh_id != id
        {
            bail!("a repo named `{name}` already exists in the mesh; clone it instead");
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
                status: PeerStatus::Alive { name },
            },
        );

        Ok(())
    }

    /// Renames this machine; the bumped version propagates the rename
    /// through the gossip.
    pub fn rename_machine(&mut self, name: String) -> Result<()> {
        self.machine.rename(name)
    }

    /// Tombstones an alive peer, resolved by name or full endpoint id.
    /// The tombstone propagates the removal through the gossip.
    pub fn remove_peer(&mut self, selector: &str) -> Result<EndpointId> {
        let endpoint = self.resolve_peer(selector)?;
        let version = bumped_version(&self.peers, &endpoint)?;

        let peer = self.peers.get_mut(&endpoint).expect("resolved above");
        peer.version = version;
        peer.status = PeerStatus::Removed;

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
                "several peers are named `{selector}`; use the endpoint id \
                 (see `jj-mesh status`)"
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
    /// it in the mesh-wide repo list, superseding any tombstone. The path
    /// is stored canonicalized, whatever the caller sent, so path
    /// comparisons never have to resolve stored entries again.
    pub fn add_repo(&mut self, name: String, mut repo: Repo) -> Result<()> {
        repo.path = canonical_path(&repo.path);
        self.validate_new_repo(&name, &repo.path)?;
        self.ensure_mesh_id(&name, &repo.id)?;
        if let Some(existing) = self.repo_name(&repo.id) {
            bail!("repo {} is already registered as `{existing}`", repo.id);
        }
        ensure!(
            self.mesh_repos.contains_key(&name) || self.mesh_repos.len() < MAX_MESH_REPOS,
            "the mesh already has {MAX_MESH_REPOS} repos",
        );

        let version = bumped_version(&self.mesh_repos, &name)?;
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
    pub fn remove_repo(&mut self, name: &str) -> Result<bool> {
        match (self.mesh_repos.get(name), self.repos.contains_key(name)) {
            (None, false) => bail!("no repo named `{name}` in the mesh"),
            (Some(mesh), false) if mesh.id().is_none() => {
                bail!("repo `{name}` is already removed")
            }
            _ => {}
        }

        let version = bumped_version(&self.mesh_repos, name)?;
        self.mesh_repos.insert(
            name.to_owned(),
            MeshRepo {
                version,
                status: MeshRepoStatus::Removed,
            },
        );

        Ok(self.repos.remove(name).is_some())
    }

    /// Unregisters a repo on this machine only, returning its record. The
    /// mesh record is untouched: the other machines keep syncing the repo
    /// among themselves, and it stays clonable here (that is also the only
    /// way back in, since re-adding it would fork the name).
    pub fn forget_repo(&mut self, name: &str) -> Result<Repo> {
        match self.repos.remove(name) {
            Some(repo) => Ok(repo),
            None => bail!("no repo named `{name}` is registered on this machine"),
        }
    }

    /// The membership this machine gossips: its own record under `local`,
    /// every peer record (tombstones included) and the mesh-wide repo
    /// list.
    pub fn membership(&self, local: EndpointId) -> Membership {
        let mut peers = self.peers.clone();
        peers.insert(local, self.machine.record());
        Membership {
            peers,
            repos: self.mesh_repos.clone(),
        }
    }

    /// Whether the gossiped part of the state (see [`Self::membership`])
    /// differs, without building it.
    pub fn membership_differs(&self, other: &Self) -> bool {
        self.machine != other.machine
            || self.peers != other.peers
            || self.mesh_repos != other.mesh_repos
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
    /// [`MAX_MESH_REPOS`]) while updates to known ones keep flowing.
    pub fn merge_membership(&mut self, remote: &Membership, local: &EndpointId) {
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
            if let PeerStatus::Alive { name } = &record.status
                && validate_name("peer", name).is_err()
            {
                continue;
            }
            if should_adopt(&self.peers, endpoint, record, MAX_OTHER_PEERS) {
                self.peers.insert(*endpoint, record.clone());
            }
        }

        for (name, record) in &remote.repos {
            if validate_name("repo", name).is_err() || record.version >= MAX_RECORD_VERSION {
                continue;
            }
            // Two unrelated repos claiming one name is a conflict only the
            // user can settle, but the list must still converge, so the
            // ranking decides it everywhere the same way. The sync-level
            // conflict (see `daemon::hub`) surfaces it.
            if !should_adopt(&self.mesh_repos, name, record, MAX_MESH_REPOS) {
                continue;
            }

            self.mesh_repos.insert(name.clone(), record.clone());
            // A repo removed mesh-wide stops being synced here; its
            // files stay where they are.
            if record.id().is_none() && self.repos.remove(name).is_some() {
                debug!(repo = %name, "repo removed from the mesh; no longer syncing it");
            }
        }
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

    use super::{membership::Register as _, *};

    impl Machine {
        fn new(name: &str, version: u64) -> Self {
            Machine {
                name: name.to_owned(),
                version,
            }
        }
    }

    /// A mesh repo record in use.
    fn present(version: u64, id: &RepoId) -> MeshRepo {
        MeshRepo {
            version,
            status: MeshRepoStatus::Present { id: id.clone() },
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
    }

    #[test]
    fn own_record_keeps_its_name_and_outranks_copies() {
        let local = SecretKey::generate().public();
        let alive = |version, name: &str| Peer {
            version,
            status: PeerStatus::Alive {
                name: name.to_owned(),
            },
        };
        let about_us = |record: Peer| Membership {
            peers: BTreeMap::from([(local, record)]),
            repos: BTreeMap::new(),
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

        // ...and a tombstone is matched, not outranked: the removed machine
        // cannot undo its removal.
        let removed = Peer {
            version: 8,
            status: PeerStatus::Removed,
        };
        state.merge_membership(&about_us(removed.clone()), &local);
        assert_eq!(state.machine, Machine::new("desk", 8));
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

        assert_eq!(state.peers.len(), MAX_OTHER_PEERS);
        assert_eq!(state.mesh_repos.len(), MAX_MESH_REPOS);
        // With our own record added, the membership stays within the wire
        // cap.
        assert_eq!(state.membership(local).peers.len(), MAX_MESH_PEERS);
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
        // first, or their "available repos" and add/clone guards disagree
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
    fn adding_a_repo_known_to_the_mesh_requires_cloning() {
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
        assert!(err.to_string().contains("clone"), "{err:#}");
    }

    #[test]
    fn removing_a_repo_tombstones_it_and_frees_the_name() {
        let mut state = MeshState::default();
        let repo = |id: &RepoId| Repo {
            id: id.clone(),
            path: "/p".into(),
        };

        let first = RepoId::generate();
        state.add_repo("proj".to_owned(), repo(&first)).unwrap();
        assert!(state.remove_repo("proj").unwrap(), "it was registered here");

        // The name is retired, not deleted: it stays as a tombstone so a
        // machine that missed the removal cannot resurrect it.
        assert!(state.repos.is_empty());
        assert_eq!(state.mesh_repo_id("proj"), None);
        assert_eq!(state.mesh_repo_names().count(), 0);
        assert!(state.mesh_repos.contains_key("proj"));
        assert!(state.remove_repo("proj").is_err());
        assert!(state.remove_repo("nope").is_err());

        // And the name can be reused, superseding the tombstone.
        let second = RepoId::generate();
        state.add_repo("proj".to_owned(), repo(&second)).unwrap();
        assert_eq!(state.mesh_repo_id("proj"), Some(&second));
        assert!(state.mesh_repos["proj"].version > 2);
    }

    #[test]
    fn forgetting_locally_keeps_the_mesh_record() {
        let mut state = MeshState::default();
        let id = RepoId::generate();
        state
            .add_repo(
                "proj".to_owned(),
                Repo {
                    id: id.clone(),
                    path: "/p".into(),
                },
            )
            .unwrap();

        let repo = state.forget_repo("proj").unwrap();
        assert_eq!(repo.path, PathBuf::from("/p"));
        assert!(state.repos.is_empty());
        assert!(state.forget_repo("proj").is_err());

        // The mesh still knows the repo: it stays clonable here, and only
        // a clone (bringing the mesh id) can register it again; a plain
        // re-add generates a fresh id and would fork the name.
        assert_eq!(state.mesh_repo_id("proj"), Some(&id));
        assert_eq!(state.mesh_repo_names().count(), 1);
        let err = state
            .add_repo(
                "proj".to_owned(),
                Repo {
                    id: RepoId::generate(),
                    path: repo.path,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("clone"), "{err:#}");
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

        // Another machine removed the repo: we stop syncing it, and a stale
        // announcement of the old record cannot bring it back.
        let removed = MeshRepo {
            version: state.mesh_repos["proj"].version + 1,
            status: MeshRepoStatus::Removed,
        };
        let membership = |record: MeshRepo| Membership {
            peers: BTreeMap::new(),
            repos: BTreeMap::from([("proj".to_owned(), record)]),
        };
        state.merge_membership(&membership(removed), &local);
        assert!(state.repos.is_empty());
        assert_eq!(state.mesh_repo_id("proj"), None);

        state.merge_membership(&membership(present(1, &id)), &local);
        assert_eq!(state.mesh_repo_id("proj"), None);
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
        state
            .add_repo(
                "a".to_owned(),
                Repo {
                    id: RepoId::generate(),
                    path: real,
                },
            )
            .unwrap();

        let err = state.validate_new_repo("b", &link).unwrap_err();
        assert!(err.to_string().contains("already added"), "{err:#}");
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

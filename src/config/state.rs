//! Mesh state file (`peers.json`).
//!
//! This file holds the machine's copy of the mesh state: the paired peers
//! and the registered repos. It is owned and written exclusively by the
//! daemon; the CLI mutates it through the control socket and only reads it
//! directly as a fallback when no daemon is running.

use std::{
    collections::BTreeMap,
    fmt, fs, io,
    io::Write as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, bail, ensure, eyre};
use data_encoding::HEXLOWER;
use iroh::EndpointId;
use serde::{Deserialize, Serialize};

use super::ConfigDir;

/// Maximum length of a peer or repo name, in bytes.
///
/// Names can be announced by remote machines and end up as terminal output,
/// so they are kept short and free of control characters.
const MAX_NAME_LEN: usize = 64;

/// Checks that a peer or repo name is usable; `kind` names it in errors.
fn validate_name(kind: &str, name: &str) -> Result<()> {
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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MeshState {
    pub peers: BTreeMap<String, Peer>,
    pub repos: BTreeMap<String, Repo>,
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

    /// The name under which an endpoint is paired, if any.
    pub fn peer_name(&self, endpoint: &EndpointId) -> Option<&str> {
        self.peers
            .iter()
            .find(|(_, peer)| &peer.endpoint == endpoint)
            .map(|(name, _)| name.as_str())
    }

    /// The name under which a repo id is registered, if any.
    pub fn repo_name(&self, id: &RepoId) -> Option<&str> {
        self.repos
            .iter()
            .find(|(_, repo)| &repo.id == id)
            .map(|(name, _)| name.as_str())
    }

    /// Checks that a peer can be registered under this name and endpoint,
    /// rejecting invalid names and duplicates of either.
    pub fn validate_new_peer(&self, name: &str, endpoint: &EndpointId) -> Result<()> {
        validate_name("peer", name)?;
        ensure!(
            !self.peers.contains_key(name),
            "a peer named `{name}` already exists",
        );

        if let Some(existing) = self.peer_name(endpoint) {
            bail!("endpoint {endpoint} is already paired as `{existing}`");
        }

        Ok(())
    }

    /// Checks that a repo can be registered under this name and path,
    /// rejecting invalid names and duplicates of either.
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

    /// Adds a peer, rejecting duplicate names and endpoints.
    pub fn add_peer(&mut self, name: String, peer: Peer) -> Result<()> {
        self.validate_new_peer(&name, &peer.endpoint)?;
        self.peers.insert(name, peer);

        Ok(())
    }

    /// Removes a peer by name.
    pub fn remove_peer(&mut self, name: &str) -> Result<Peer> {
        self.peers
            .remove(name)
            .ok_or_else(|| eyre!("no peer named `{name}`"))
    }

    /// Adds a repo, rejecting duplicate names, paths and ids.
    pub fn add_repo(&mut self, name: String, repo: Repo) -> Result<()> {
        self.validate_new_repo(&name, &repo.path)?;
        if let Some(existing) = self.repo_name(&repo.id) {
            bail!("repo {} is already registered as `{existing}`", repo.id);
        }
        self.repos.insert(name, repo);

        Ok(())
    }
}

/// A paired machine. This represents remote peers that we are allowed to
/// exchange data with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    /// The peer's iroh endpoint id (its public key).
    pub endpoint: EndpointId,
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
        state
            .add_peer("laptop".to_owned(), Peer { endpoint })
            .unwrap();
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
        assert_eq!(state.peers["laptop"].endpoint, endpoint);
        assert!(state.repos.contains_key("project"));

        assert!(state.remove_peer("desktop").is_err());
        let removed = state.remove_peer("laptop").unwrap();
        assert_eq!(removed.endpoint, endpoint);
        state.save(&dir).unwrap();

        assert!(MeshState::load(&dir).unwrap().peers.is_empty());
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
    fn duplicate_names_and_endpoints_are_rejected() {
        let mut state = MeshState::default();
        let endpoint = SecretKey::generate().public();

        state
            .add_peer("laptop".to_owned(), Peer { endpoint })
            .unwrap();
        assert!(
            state
                .add_peer("laptop".to_owned(), Peer { endpoint })
                .is_err()
        );
        assert!(
            state
                .add_peer("desktop".to_owned(), Peer { endpoint })
                .is_err()
        );
    }
}

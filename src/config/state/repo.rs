//! Repos registered on this machine and their mesh-wide identity.

use std::{fmt, path::PathBuf};

use data_encoding::HEXLOWER;
use serde::{Deserialize, Serialize};

/// A repo registered on this machine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Repo {
    /// Mesh-wide identifier of the repo.
    pub id: RepoId,
    /// Local repository root (the directory containing `.jj`), stored
    /// canonicalized by `MeshState::add_repo`.
    pub path: PathBuf,
}

/// Mesh-wide identifier of a repo, shared by all machines syncing it.
/// Randomly generated.
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
    /// Generates a random repo id.
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
}

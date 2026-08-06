//! Invoking the user's jj binary.
//!
//! [`run_jj`] runs one command against a repo, [`local_jj_version`]
//! detects the binary's version, and [`jj_version_warning`] words the
//! warning when that version is not from the supported series.

use std::path::Path;

use color_eyre::eyre::{Result, WrapErr as _, ensure, eyre};

use super::SUPPORTED_JJ_SERIES;

/// Cap on the child's diagnostics kept in memory. jj reports one line
/// per file it refuses to snapshot, so on a hostile or merely enormous
/// working copy its stderr is neither small nor ours to trust; the tail
/// past the cap is dropped, and only the start ever reaches a log.
const MAX_JJ_STDERR: u64 = 64 * 1024;

/// The jj binary to invoke: `$JJ_BIN` when set and non-empty, otherwise
/// `jj` from PATH. Resolved on every call so a daemon inherits its
/// service environment and tests can override it per process.
pub fn jj_bin() -> std::ffi::OsString {
    std::env::var_os("JJ_BIN")
        .filter(|bin| !bin.is_empty())
        .unwrap_or_else(|| "jj".into())
}

/// Runs a jj command against the repo at `root` through the [`jj_bin`]
/// binary: it applies the user's jj configuration and takes the proper
/// locks, which jj-mesh must not reimplement. The child is killed when
/// `timeout` fires.
pub async fn run_jj(root: &Path, args: &[&str], timeout: std::time::Duration) -> Result<()> {
    tokio::time::timeout(timeout, spawn_jj(root, args))
        .await
        .map_err(|_| eyre!("jj {} timed out", args.join(" ")))?
}

/// Spawns one jj command and waits for it, failing with its (bounded,
/// sanitized) diagnostics.
async fn spawn_jj(root: &Path, args: &[&str]) -> Result<()> {
    use tokio::io::AsyncReadExt as _;

    let mut child = tokio::process::Command::new(jj_bin())
        // jj resolves the current directory even when given a repo, so
        // it must be one that exists: the daemon's own cwd is whatever
        // it was started in and may be long gone.
        .current_dir(root)
        .arg("--repository")
        .arg(root)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .wrap_err_with(|| {
            format!(
                "cannot run {} (is it on PATH? see JJ_BIN)",
                jj_bin().display(),
            )
        })?;

    let mut stderr = Vec::new();
    if let Some(pipe) = child.stderr.take() {
        // A full pipe would block the child forever, so it is drained
        // even past the cap; only the head is kept.
        let mut pipe = pipe;
        let mut head = (&mut pipe).take(MAX_JJ_STDERR);
        head.read_to_end(&mut stderr).await.ok();
        tokio::io::copy(&mut pipe, &mut tokio::io::sink())
            .await
            .ok();
    }
    let status = child.wait().await.wrap_err("cannot wait for jj")?;

    ensure!(
        status.success(),
        "jj {} failed: {}",
        args.join(" "),
        // Diagnostics embed bytes read from repo files: stripped of
        // control characters and capped before they can reach a log.
        crate::config::sanitize_bounded(String::from_utf8_lossy(&stderr).trim()),
    );
    Ok(())
}

/// Whether a jj repo exists at `root` (its `.jj` marker is present).
/// Errors other than absence (permissions, I/O) count as present: the repo
/// may be fine, only we cannot look right now.
pub fn repo_present(root: &Path) -> bool {
    match std::fs::metadata(root.join(".jj")) {
        Ok(_) => true,
        Err(err) => err.kind() != std::io::ErrorKind::NotFound,
    }
}

/// The version of the [`jj_bin`] binary (`X.Y.Z`), or `None` when jj is
/// not runnable or its output is unrecognized. This is a heuristic: the
/// daemon cannot know which jj binary actually writes the repos, so the
/// answer is only ever used to warn, never to refuse.
pub fn local_jj_version() -> Option<String> {
    let output = std::process::Command::new(jj_bin())
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    let stdout = String::from_utf8(output.stdout).ok()?;
    parse_jj_version(&stdout)
}

/// Extracts the version from `jj --version` output (`jj 0.44.0[-hash]`).
fn parse_jj_version(output: &str) -> Option<String> {
    let version = output.trim().strip_prefix("jj ")?.split('-').next()?;
    let plausible = !version.is_empty() && version.chars().all(|c| c.is_ascii_digit() || c == '.');
    plausible.then(|| version.to_owned())
}

/// Whether a jj version belongs to the [`SUPPORTED_JJ_SERIES`].
fn jj_version_supported(version: &str) -> bool {
    version
        .strip_prefix(SUPPORTED_JJ_SERIES)
        .is_some_and(|rest| rest.starts_with('.'))
}

/// The warning a detected jj version deserves, `None` when it is
/// supported. One wording, shared by the daemon log and the CLI.
pub fn jj_version_warning(version: Option<&str>) -> Option<String> {
    match version {
        Some(version) if jj_version_supported(version) => None,
        Some(version) => Some(format!(
            "unsupported jj {version} found (supported: {SUPPORTED_JJ_SERIES})"
        )),
        None => Some("jj not found (on PATH or via JJ_BIN)".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_matches_jj_versions() {
        assert_eq!(parse_jj_version("jj 0.44.0\n"), Some("0.44.0".to_owned()));
        assert_eq!(
            parse_jj_version("jj 0.44.0-dev+abcdef\n"),
            Some("0.44.0".to_owned()),
        );
        assert_eq!(parse_jj_version("command not found: jj"), None);
        assert_eq!(parse_jj_version("jj whatever"), None);
        assert_eq!(parse_jj_version(""), None);

        assert!(jj_version_supported("0.44.0"));
        assert!(jj_version_supported("0.44.12"));
        assert!(!jj_version_supported("0.43.0"));
        assert!(!jj_version_supported("0.4.40"));
        assert!(!jj_version_supported("0.440.0"));
        assert!(!jj_version_supported("0.44"));
    }
}

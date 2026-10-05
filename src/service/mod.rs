//! The daemon's user service: a systemd user unit on Linux, a launchd
//! agent on macOS.
//!
//! A [`Service`] is identified by its definition file (unit or plist),
//! looked up under the labels in the platform's `LABELS`. `jj-mesh service
//! install` writes a regular file under the first one; external managers
//! (Home Manager) use another label or install a symlink. The CLI still
//! starts and stops an external service, but refuses to replace or remove
//! it.
//!
//! Definitions are rendered from a [`Spec`] by the platform module, which
//! also drives the service manager (`systemctl --user`, `launchctl`). The
//! programs a spec runs are found with [`which`], since the service's own
//! PATH is minimal.

#[cfg(target_os = "macos")]
#[path = "launchd.rs"]
mod platform;
#[cfg(target_os = "linux")]
#[path = "systemd.rs"]
mod platform;

use std::{
    ffi::OsStr,
    fs,
    io::ErrorKind,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use color_eyre::eyre::{Result, WrapErr as _, ensure};

/// What the service runs.
#[derive(Debug)]
pub struct Spec {
    /// The absolute program path, then its arguments.
    pub command: Vec<String>,
    pub env: Vec<(String, String)>,
}

/// Whether the service process is up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    Stopped,
}

/// A user service, installed or about to be.
#[derive(Debug, PartialEq, Eq)]
pub struct Service {
    label: &'static str,
    path: PathBuf,
}

impl Service {
    /// The installed service: the last label whose definition exists, so
    /// a leftover install of ours never hides an external manager's.
    pub fn find() -> Result<Option<Self>> {
        for label in platform::LABELS.iter().rev() {
            let service = Self::new(label)?;
            match fs::symlink_metadata(&service.path) {
                Ok(_) => return Ok(Some(service)),
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err)
                        .wrap_err_with(|| format!("cannot read {}", service.path.display()));
                }
            }
        }
        Ok(None)
    }

    /// The service `jj-mesh service install` writes.
    pub fn ours() -> Result<Self> {
        Self::new(platform::LABELS[0])
    }

    fn new(label: &'static str) -> Result<Self> {
        Ok(Service {
            label,
            path: platform::path(label)?,
        })
    }

    /// Path of the definition file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether another program manages the service: it has another label
    /// than ours, or its definition is a symlink (as installed by Home
    /// Manager and other nix-based managers).
    pub fn is_external(&self) -> bool {
        self.label != platform::LABELS[0] || self.path.is_symlink()
    }

    /// Writes the definition, then enables and (re)starts the service.
    pub fn install(&self, spec: &Spec) -> Result<()> {
        ensure!(
            spec.command
                .first()
                .is_some_and(|p| Path::new(p).is_absolute()),
            "the program path must be absolute",
        );
        let values = spec
            .command
            .iter()
            .chain(spec.env.iter().flat_map(|(key, value)| [key, value]));
        for value in values {
            ensure!(
                !value.chars().any(char::is_control),
                "{value:?} contains control characters",
            );
        }

        if let Some(dir) = self.path.parent() {
            fs::create_dir_all(dir).wrap_err_with(|| format!("cannot create {}", dir.display()))?;
        }
        fs::write(&self.path, platform::render(self, spec)?)
            .wrap_err_with(|| format!("cannot write {}", self.path.display()))?;
        platform::install(self)
    }

    /// Stops and disables the service, then removes its definition.
    pub fn uninstall(&self) -> Result<()> {
        platform::uninstall(self)
    }

    pub fn start(&self) -> Result<()> {
        platform::start(self)
    }

    pub fn stop(&self) -> Result<()> {
        platform::stop(self)
    }

    pub fn restart(&self) -> Result<()> {
        platform::restart(self)
    }

    pub fn status(&self) -> Result<Status> {
        platform::status(self)
    }
}

/// Resolves `bin` to the executable the shell would run, without following
/// symlinks so a path such as `~/.nix-profile/bin/jj` survives updates. A
/// bare name is looked up in the absolute entries of `path` (a `PATH`
/// value): relative ones depend on the current directory.
pub fn which(bin: &Path, path: &OsStr) -> Option<PathBuf> {
    let executable = |path: &Path| {
        path.metadata()
            .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
    };
    if bin.components().count() > 1 {
        return executable(bin).then(|| bin.to_owned());
    }
    std::env::split_paths(path)
        .filter(|dir| dir.is_absolute())
        .map(|dir| dir.join(bin))
        .find(|path| executable(path))
}

/// Removes a definition file.
fn remove(path: &Path) -> Result<()> {
    fs::remove_file(path).wrap_err_with(|| format!("cannot remove {}", path.display()))
}

/// Runs a service manager command, failing with its stderr.
fn run(program: &str, args: &[&str]) -> Result<()> {
    let output = output(program, args)?;
    ensure!(
        output.status.success(),
        "{program} {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim(),
    );
    Ok(())
}

/// Runs a service manager command, whatever its exit status.
fn output(program: &str, args: &[&str]) -> Result<Output> {
    Command::new(program)
        .args(args)
        .output()
        .wrap_err_with(|| format!("cannot run {program}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_programs_like_the_shell() {
        use std::os::unix::fs::symlink;

        let tmp = tempfile::tempdir().unwrap();
        let dir = |name: &str| {
            let dir = tmp.path().join(name);
            std::fs::create_dir(&dir).unwrap();
            dir
        };
        let (plain, exec, linked) = (dir("plain"), dir("exec"), dir("linked"));
        std::fs::write(plain.join("jj"), "").unwrap();
        std::fs::write(exec.join("jj"), "").unwrap();
        std::fs::set_permissions(exec.join("jj"), std::fs::Permissions::from_mode(0o755)).unwrap();
        symlink(exec.join("jj"), linked.join("jj")).unwrap();
        std::fs::create_dir(dir("folder").join("jj")).unwrap();
        let path = |dirs: &[&Path]| std::env::join_paths(dirs).unwrap();
        let jj = Path::new("jj");

        // Non-executables and directories are skipped.
        let folder = tmp.path().join("folder");
        assert_eq!(
            which(jj, &path(&[&plain, &folder, &exec])),
            Some(exec.join("jj"))
        );
        // Symlinks are kept as-is.
        assert_eq!(which(jj, &path(&[&linked, &exec])), Some(linked.join("jj")));
        // Relative entries are skipped, even when they resolve.
        let cwd = std::env::current_dir().unwrap();
        let up: PathBuf = cwd.components().skip(1).map(|_| "..").collect();
        let relative = up.join(exec.strip_prefix("/").unwrap());
        assert!(relative.join("jj").is_file());
        assert_eq!(which(jj, &path(&[&relative])), None);
        assert_eq!(which(jj, &path(&[])), None);
        // A path is checked, not looked up.
        assert_eq!(which(&exec.join("jj"), &path(&[])), Some(exec.join("jj")));
        assert_eq!(which(&plain.join("jj"), &path(&[&exec])), None);
    }
}

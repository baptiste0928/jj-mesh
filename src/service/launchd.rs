//! launchd agents, driven through `launchctl` in the `gui/<uid>` domain.

use std::{
    fmt::Write as _,
    path::{Path, PathBuf},
};

use color_eyre::eyre::{Result, WrapErr as _, eyre};

use super::{Service, Spec, Status, output, remove, run};

/// Agent labels, ours first, then Home Manager's.
pub(super) const LABELS: &[&str] = &["jj-mesh", "org.nix-community.home.jj-mesh"];

/// Daemon log file, relative to the home directory; the Home Manager
/// module uses the same.
const LOG_FILE: &str = "Library/Logs/jj-mesh.log";

/// `~/Library/LaunchAgents/<label>.plist`.
pub(super) fn path(label: &str) -> Result<PathBuf> {
    Ok(home()?
        .join("Library/LaunchAgents")
        .join(format!("{label}.plist")))
}

/// The agent plist for `spec`. `KeepAlive` restarts the daemon only when
/// it fails, like systemd's `Restart=on-failure`.
pub(super) fn render(service: &Service, spec: &Spec) -> Result<String> {
    let label = service.label;
    let mut args = String::new();
    for word in &spec.command {
        let _ = writeln!(args, "    <string>{}</string>", escape(word));
    }
    let mut env = String::new();
    for (key, value) in &spec.env {
        let _ = writeln!(
            env,
            "    <key>{}</key>\n    <string>{}</string>",
            escape(key),
            escape(value),
        );
    }
    let log = escape(&home()?.join(LOG_FILE).to_string_lossy());

    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{args}  </array>
  <key>EnvironmentVariables</key>
  <dict>
{env}  </dict>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>ProcessType</key>
  <string>Background</string>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
</dict>
</plist>
"#
    ))
}

/// Loads the written plist, replacing a loaded older version; `RunAtLoad`
/// starts it.
pub(super) fn install(service: &Service) -> Result<()> {
    stop(service)?;
    bootstrap(&domain()?, &service.path)
}

pub(super) fn uninstall(service: &Service) -> Result<()> {
    stop(service)?;
    remove(&service.path)
}

pub(super) fn start(service: &Service) -> Result<()> {
    kickstart(service, false)
}

/// Unloads the agent: a plain kill would leave stopping to `KeepAlive`
/// and the daemon's exit status.
pub(super) fn stop(service: &Service) -> Result<()> {
    let domain = domain()?;
    if loaded(&domain, service.label)? {
        launchctl(&["bootout", &target(&domain, service.label)])?;
    }
    Ok(())
}

pub(super) fn restart(service: &Service) -> Result<()> {
    kickstart(service, true)
}

pub(super) fn status(service: &Service) -> Result<Status> {
    let output = output("launchctl", &["print", &target(&domain()?, service.label)])?;
    // `print` output has no stable format: only this line is relied on.
    let running = output.status.success()
        && String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line.trim() == "state = running");
    Ok(if running {
        Status::Running
    } else {
        Status::Stopped
    })
}

/// Starts a loaded agent, killing it first when `kill`, or loads it.
fn kickstart(service: &Service, kill: bool) -> Result<()> {
    let domain = domain()?;
    if !loaded(&domain, service.label)? {
        return bootstrap(&domain, &service.path);
    }
    let target = target(&domain, service.label);
    if kill {
        launchctl(&["kickstart", "-k", &target])
    } else {
        launchctl(&["kickstart", &target])
    }
}

/// Whether the agent is loaded in `domain`.
fn loaded(domain: &str, label: &str) -> Result<bool> {
    Ok(output("launchctl", &["print", &target(domain, label)])?
        .status
        .success())
}

fn bootstrap(domain: &str, path: &Path) -> Result<()> {
    let path = path
        .to_str()
        .ok_or_else(|| eyre!("{} is not valid UTF-8", path.display()))?;
    launchctl(&["bootstrap", domain, path])
}

fn launchctl(args: &[&str]) -> Result<()> {
    run("launchctl", args)
}

/// The current user's GUI domain, `gui/<uid>`.
fn domain() -> Result<String> {
    let output = output("id", &["-u"])?;
    let uid = String::from_utf8_lossy(&output.stdout);
    let uid = uid.trim();
    if !output.status.success() || uid.is_empty() {
        return Err(eyre!("cannot determine the user id"));
    }
    Ok(format!("gui/{uid}"))
}

fn target(domain: &str, label: &str) -> String {
    format!("{domain}/{label}")
}

fn home() -> Result<PathBuf> {
    etcetera::home_dir().wrap_err("cannot determine the home directory")
}

/// Escapes XML text.
fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ => escaped.push(c),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_escaped_values() {
        let spec = Spec {
            command: vec!["/opt/a&b/jj-mesh".into(), "run-daemon".into()],
            env: vec![("JJ_BIN".into(), "/x/<jj>".into())],
        };
        let plist = render(&Service::ours().unwrap(), &spec).unwrap();
        assert!(plist.contains("    <string>/opt/a&amp;b/jj-mesh</string>\n"));
        assert!(plist.contains("    <key>JJ_BIN</key>\n    <string>/x/&lt;jj&gt;</string>\n"));
    }
}

//! systemd user units, driven through `systemctl --user`.

use std::path::PathBuf;

use color_eyre::eyre::{Result, WrapErr as _};
use etcetera::BaseStrategy as _;

use super::{Service, Spec, Status, output, remove, run};

/// Unit names, ours first. Home Manager uses the same name.
pub(super) const LABELS: &[&str] = &["jj-mesh"];

/// Seconds systemd waits before restarting a failed daemon.
const RESTART_DELAY_SECS: u32 = 5;

/// `$XDG_CONFIG_HOME/systemd/user/<label>.service`.
pub(super) fn path(label: &str) -> Result<PathBuf> {
    Ok(etcetera::choose_base_strategy()
        .wrap_err("cannot determine the config directory")?
        .config_dir()
        .join("systemd/user")
        .join(unit(label)))
}

/// The unit file for `spec`. `Type=exec` makes `systemctl start` fail when
/// the program cannot run.
#[allow(clippy::unnecessary_wraps, reason = "same signature as on launchd")]
pub(super) fn render(_service: &Service, spec: &Spec) -> Result<String> {
    let exec = spec
        .command
        .iter()
        .map(|word| quote(word, true))
        .collect::<Vec<_>>()
        .join(" ");
    let env = spec
        .env
        .iter()
        .map(|(key, value)| quote(&format!("{key}={value}"), false))
        .collect::<Vec<_>>()
        .join(" ");

    Ok(format!(
        "[Unit]\n\
         Description=jj-mesh sync daemon\n\
         After=network.target\n\
         \n\
         [Service]\n\
         Type=exec\n\
         ExecStart={exec}\n\
         Environment={env}\n\
         Restart=on-failure\n\
         RestartSec={RESTART_DELAY_SECS}\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n"
    ))
}

/// Reloads the written unit, then enables and (re)starts it: a reinstall
/// over a running service must replace the old process.
pub(super) fn install(service: &Service) -> Result<()> {
    systemctl(&["daemon-reload"])?;
    systemctl(&["enable", &unit(service.label)])?;
    systemctl(&["restart", &unit(service.label)])
}

pub(super) fn uninstall(service: &Service) -> Result<()> {
    systemctl(&["disable", "--now", &unit(service.label)])?;
    remove(&service.path)?;
    systemctl(&["daemon-reload"])
}

pub(super) fn start(service: &Service) -> Result<()> {
    systemctl(&["start", &unit(service.label)])
}

pub(super) fn stop(service: &Service) -> Result<()> {
    systemctl(&["stop", &unit(service.label)])
}

pub(super) fn restart(service: &Service) -> Result<()> {
    systemctl(&["restart", &unit(service.label)])
}

pub(super) fn status(service: &Service) -> Result<Status> {
    // Exits non-zero for every state but active, so only stdout counts.
    let output = output("systemctl", &["--user", "is-active", &unit(service.label)])?;
    Ok(if output.stdout.trim_ascii() == b"active" {
        Status::Running
    } else {
        Status::Stopped
    })
}

fn unit(label: &str) -> String {
    format!("{label}.service")
}

fn systemctl(args: &[&str]) -> Result<()> {
    run("systemctl", &[&["--user"], args].concat())
}

/// Quotes one unit file word: C escapes apply inside quotes, `%` starts a
/// specifier, and `ExecStart` (`exec`) also expands `$` variables.
fn quote(word: &str, exec: bool) -> String {
    let mut quoted = String::from('"');
    for c in word.chars() {
        match c {
            '\\' | '"' => quoted.push('\\'),
            '%' => quoted.push('%'),
            '$' if exec => quoted.push('$'),
            _ => {}
        }
        quoted.push(c);
    }
    quoted.push('"');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_escaped_words() {
        let spec = Spec {
            command: vec!["/opt/my bin/jj-mesh".into(), r#"a"b\%$c"#.into()],
            env: vec![("JJ_BIN".into(), "/x/$HOME %h".into())],
        };
        let unit = render(&Service::ours().unwrap(), &spec).unwrap();
        assert!(unit.contains(r#"ExecStart="/opt/my bin/jj-mesh" "a\"b\\%%$$c""#));
        assert!(unit.contains(r#"Environment="JJ_BIN=/x/$HOME %%h""#));
    }
}

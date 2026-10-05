//! Shared presentation helpers for the CLI output and prompts.
//!
//! Styling goes through the semantic wrappers below so every command
//! renders the same palette; `console` disables the colors on its own
//! when stdout is not a terminal or `NO_COLOR` is set. Peer and repo
//! names are validated at the daemon boundary and print as-is; free-form
//! peer- or daemon-provided strings (error messages) must pass through
//! [`crate::config::sanitize`] before they are printed or styled.
//!
//! Prompts render on stderr, and have no answer (`None`) when stdin or
//! stderr is not a terminal: callers decide what that means.

use std::{
    fmt::Display,
    io::{ErrorKind, IsTerminal as _},
    path::Path,
};

use color_eyre::eyre::{Result, WrapErr as _};
use console::{StyledObject, Term, style};
use dialoguer::{Confirm, Input, Select, theme::ColorfulTheme};

/// Bold section heading.
pub(super) fn heading<D: Display>(text: D) -> StyledObject<D> {
    style(text).bold()
}

/// Healthy or successful state.
pub(super) fn good<D: Display>(text: D) -> StyledObject<D> {
    style(text).green()
}

/// State that needs attention without being an error.
pub(super) fn warn<D: Display>(text: D) -> StyledObject<D> {
    style(text).yellow()
}

/// Failing state.
pub(super) fn bad<D: Display>(text: D) -> StyledObject<D> {
    style(text).red()
}

/// De-emphasized detail.
pub(super) fn dim<D: Display>(text: D) -> StyledObject<D> {
    style(text).dim()
}

/// Displays a path with the home directory shortened to `~`.
pub(super) fn display_path(path: &Path) -> String {
    if let Ok(home) = etcetera::home_dir()
        && let Ok(rest) = path.strip_prefix(&home)
    {
        return if rest.as_os_str().is_empty() {
            "~".to_owned()
        } else {
            format!("~/{}", rest.display())
        };
    }
    path.display().to_string()
}

/// Formats a duration in seconds compactly (`43s`, `12m 3s`, `2h 4m`...).
pub(super) fn format_duration(secs: u64) -> String {
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3600 => format!("{}m {}s", s / 60, s % 60),
        s if s < 86400 => format!("{}h {}m", s / 3600, (s % 3600) / 60),
        s => format!("{}d {}h", s / 86400, (s % 86400) / 3600),
    }
}

/// Width of the longest name, for column alignment. Counts chars to match
/// the `{:width$}` padding, which is not byte-based.
pub(super) fn name_width<'a>(names: impl Iterator<Item = &'a str>) -> usize {
    names.map(|name| name.chars().count()).max().unwrap_or(0)
}

/// Whether prompts can be shown.
fn interactive() -> bool {
    std::io::stdin().is_terminal() && std::io::stderr().is_terminal()
}

fn theme() -> ColorfulTheme {
    ColorfulTheme::default()
}

/// Asks a yes/no question.
pub(super) fn confirm(prompt: &str, default: bool) -> Result<Option<bool>> {
    if !interactive() {
        return Ok(None);
    }
    answer(
        Confirm::with_theme(&theme())
            .with_prompt(prompt)
            .default(default)
            .interact(),
    )
    .map(Some)
}

/// Asks for a line of text, pre-filled with `initial` and re-asked until
/// `validate` accepts it.
pub(super) fn input(
    prompt: &str,
    initial: &str,
    validate: impl Fn(&str) -> Result<()>,
) -> Result<Option<String>> {
    if !interactive() {
        return Ok(None);
    }
    answer(
        Input::with_theme(&theme())
            .with_prompt(prompt)
            .with_initial_text(initial)
            .validate_with(|text: &String| validate(text).map_err(|err| format!("{err:#}")))
            .interact_text(),
    )
    .map(Some)
}

/// Asks to pick one of `items`.
pub(super) fn select<'a, T: Display>(prompt: &str, items: &'a [T]) -> Result<Option<&'a T>> {
    if !interactive() {
        return Ok(None);
    }
    let index = answer(
        Select::with_theme(&theme())
            .with_prompt(prompt)
            .items(items)
            .default(0)
            .interact(),
    )?;
    Ok(Some(&items[index]))
}

/// Unwraps a prompt's answer. Ctrl-C reaches prompts as an interrupted
/// read rather than a signal: restore the cursor they hide and exit as
/// the signal would.
fn answer<T>(result: dialoguer::Result<T>) -> Result<T> {
    result.or_else(|dialoguer::Error::IO(err)| {
        if err.kind() == ErrorKind::Interrupted {
            let _ = Term::stderr().show_cursor();
            std::process::exit(130);
        }
        Err(err).wrap_err("cannot read the answer")
    })
}

//! Shared presentation helpers for the CLI output.
//!
//! Styling goes through the semantic wrappers below so every command
//! renders the same palette; `console` disables the colors on its own
//! when stdout is not a terminal or `NO_COLOR` is set. Peer-provided
//! text must pass through [`sanitize`] before it is printed or styled.

use std::{fmt::Display, path::Path};

use console::{StyledObject, style};

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

/// Replaces control and invisible characters with `?` in daemon-provided
/// text before it reaches the terminal: error messages embed bytes read from
/// repo files, which could otherwise smuggle escape sequences.
pub(super) fn sanitize(text: &str) -> String {
    text.chars()
        .map(|c| {
            if crate::config::is_confusable(c) {
                '?'
            } else {
                c
            }
        })
        .collect()
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

/// Width of the longest name, for column alignment.
pub(super) fn name_width<'a>(names: impl Iterator<Item = &'a str>) -> usize {
    names.map(str::len).max().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_replaces_escape_sequences() {
        assert_eq!(sanitize("plain text"), "plain text");
        assert_eq!(sanitize("a\x1b[2Kb\r\nc"), "a?[2Kb??c");
    }
}

//! Hygiene for peer-supplied strings.
//!
//! Names and messages arriving from remote machines end up as terminal
//! output, so everything crossing that boundary is bounded in length and
//! stripped of characters that could hide or reorder what the user sees.

use color_eyre::eyre::{Result, ensure};

/// Maximum length of a peer or repo name, in bytes.
pub const MAX_NAME_LEN: usize = 64;

/// Checks that a peer or repo name is usable; `kind` names it in errors.
/// Also applied to names arriving from remote machines (announcements).
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

/// Makes a peer-supplied string safe to surface to the user: drops the
/// characters that could hide or reorder terminal output, and bounds the
/// length (a peer's string is not ours to trust for size).
pub(crate) fn sanitize(text: &str) -> String {
    const MAX_SANITIZED_LEN: usize = 200;
    text.chars()
        .filter(|c| !is_confusable(*c))
        .take(MAX_SANITIZED_LEN)
        .collect()
}

/// Whether a character can hide or reorder text in terminal output: controls
/// (`is_control` covers only `Cc`), zero-width and bidi formatting.
pub(crate) fn is_confusable(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{200B}'..='\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{FEFF}'
        )
}

//! Errors carrying a hint: what the user can do about them.
//!
//! A [`Hinted`] error, or context (see [`WrapHint`]), displays as its
//! message alone, so the chain reads as usual. The CLI prints its hint on a line of its own
//! (see `cli::ui::error`), and the daemon sends it apart in its error
//! responses (see `daemon::control::Response::error`).

use std::fmt;

use anyhow::{Error, Result};

/// An error with a hint.
#[derive(Debug)]
pub struct Hinted {
    pub message: String,
    pub hint: String,
}

impl fmt::Display for Hinted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Hinted {}

/// An error report of `message`, hinting at `hint`.
pub fn hinted(message: impl fmt::Display, hint: impl fmt::Display) -> Error {
    Error::new(Hinted {
        message: message.to_string(),
        hint: hint.to_string(),
    })
}

/// Wraps errors with a [`Hinted`] context, keeping their cause.
pub trait WrapHint<T> {
    fn wrap_hint(self, message: impl fmt::Display, hint: impl fmt::Display) -> Result<T>;
}

impl<T> WrapHint<T> for Result<T> {
    fn wrap_hint(self, message: impl fmt::Display, hint: impl fmt::Display) -> Result<T> {
        self.map_err(|err| {
            err.context(Hinted {
                message: message.to_string(),
                hint: hint.to_string(),
            })
        })
    }
}

/// The hint of the outermost [`Hinted`] error or context of `err`.
pub fn of(err: &Error) -> Option<&str> {
    err.downcast_ref::<Hinted>()
        .map(|hinted| hinted.hint.as_str())
}

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::*;

    #[test]
    fn hints_survive_context() {
        let err = Err::<(), _>(hinted("no repo", "Add one"))
            .context("cannot clone")
            .unwrap_err();
        assert_eq!(format!("{err:#}"), "cannot clone: no repo");
        assert_eq!(of(&err), Some("Add one"));

        let err = Err::<(), _>(Error::msg("eof"))
            .wrap_hint("no answer", "Restart it")
            .context("cannot status")
            .unwrap_err();
        assert_eq!(format!("{err:#}"), "cannot status: no answer: eof");
        assert_eq!(of(&err), Some("Restart it"));
        assert!(err.chain().any(|cause| cause.to_string() == "eof"));
    }
}

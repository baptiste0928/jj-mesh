//! Commands acting on several repos at once (`repo forget`, `repo remove`).
//!
//! ```text
//! selectors ──check()──▶ checked repos ──confirm()──▶ Batch::run() ──▶ Batch::finish()
//!            all or none                              each repo,
//!                                                     failures kept
//! ```

use std::collections::BTreeSet;

use color_eyre::eyre::{Report, Result, bail, ensure, eyre};

use crate::{
    cli::ui,
    config::{MeshState, RepoId},
    repo::Workspace,
};

/// Failures collected while acting on a batch of repos.
pub(super) struct Batch {
    total: usize,
    errors: Vec<Report>,
}

impl Batch {
    /// A batch of `total` repos.
    pub fn new(total: usize) -> Self {
        Batch {
            total,
            errors: Vec::new(),
        }
    }

    /// Runs `act` on every item without stopping at failures. Returns the
    /// items it succeeded on.
    pub fn run<'a, T>(
        &mut self,
        items: impl IntoIterator<Item = &'a T>,
        act: impl Fn(&T) -> Result<()>,
    ) -> Vec<&'a T> {
        items
            .into_iter()
            .filter(|item| act(item).map_err(|err| self.errors.push(err)).is_ok())
            .collect()
    }

    /// Fails with the collected errors, each reported.
    pub fn finish(mut self) -> Result<()> {
        if self.total == 1 {
            return self.errors.pop().map_or(Ok(()), Err);
        }
        for err in &self.errors {
            crate::cli::report_error(err);
        }
        ensure!(
            self.errors.is_empty(),
            "{} of {} repos failed",
            self.errors.len(),
            self.total
        );
        Ok(())
    }
}

/// Resolves repo selectors and checks each distinct repo, failing with
/// every error so that a batch acts on all of its repos or none.
pub(super) fn check<T>(
    state: &MeshState,
    selectors: &[String],
    each: impl Fn(RepoId) -> Result<T>,
) -> Result<Vec<T>> {
    let mut seen = BTreeSet::new();
    let mut values = Vec::new();
    let mut errors = Vec::new();
    for selector in selectors {
        let checked = match state.resolve_repo(selector) {
            Ok(id) if !seen.insert(id.clone()) => continue,
            resolved => resolved.and_then(&each),
        };
        match checked {
            Ok(value) => values.push(value),
            Err(err) => errors.push(format!("{err:#}")),
        }
    }
    match &errors[..] {
        [] => Ok(values),
        [error] => bail!("{error}"),
        _ => bail!("nothing done:\n  {}", errors.join("\n  ")),
    }
}

/// Asks to confirm "{verb} {names}{scope}", and deleting the workspaces
/// when `delete`. Without a terminal, `--yes` is required.
pub(super) fn confirm<'a>(
    verb: &str,
    names: impl IntoIterator<Item = &'a str>,
    scope: &str,
    delete: bool,
) -> Result<bool> {
    let names = names.into_iter().collect::<Vec<_>>().join(", ");
    let delete = if delete {
        " and delete their workspaces"
    } else {
        ""
    };
    let confirmed = ui::confirm(&format!("{verb} {names}{scope}{delete}?"), false)?
        .ok_or_else(|| eyre!("pass --yes to confirm"))?;
    if !confirmed {
        println!("Aborted");
    }
    Ok(confirmed)
}

/// Lists workspaces on stderr, where the prompts render.
pub(super) fn show_workspaces(workspaces: &[Workspace]) {
    let rows: Vec<[String; 2]> = workspaces
        .iter()
        .map(|workspace| {
            [
                workspace.name().unwrap_or_else(|_| "?".to_owned()),
                ui::dim(ui::display_path(workspace.root()))
                    .for_stderr()
                    .to_string(),
            ]
        })
        .collect();
    for line in ui::table(&rows) {
        eprintln!("  {line}");
    }
}

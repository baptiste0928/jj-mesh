//! Shared plumbing for the notify backend: signal type and callback
//! construction common to both watchers.

use color_eyre::eyre::{Result, WrapErr as _};
use notify::{Event, RecommendedWatcher};

/// What a notify callback reports to the async side.
#[derive(Debug)]
pub(super) enum Signal<T> {
    /// A relevant event, with whatever payload the watcher needs.
    Event(T),
    /// The backend failed or the watch died; carries the error text.
    Failed(String),
}

/// Creates the notify backend. `map` decides what each event means and
/// `deliver` hands the signal to the async side; backend errors become
/// [`Signal::Failed`]. Access events never signal: the consumer's own
/// reads (snapshots, syncs) must not feed back into the watch.
pub(super) fn watcher<T>(
    mut map: impl FnMut(Event) -> Option<Signal<T>> + Send + 'static,
    mut deliver: impl FnMut(Signal<T>) + Send + 'static,
) -> Result<RecommendedWatcher> {
    notify::recommended_watcher(move |event: notify::Result<Event>| {
        let signal = match event {
            Err(err) => Some(Signal::Failed(format!("watch backend error: {err}"))),
            Ok(event) if event.kind.is_access() => None,
            Ok(event) => map(event),
        };
        if let Some(signal) = signal {
            deliver(signal);
        }
    })
    .wrap_err("cannot create filesystem watcher")
}

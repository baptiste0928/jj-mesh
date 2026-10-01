//! Recent daemon events, kept in memory for `jj-mesh logs`.
//!
//! A `tracing` layer copies the daemon's own info-and-above events, with
//! the fields of their enclosing spans, into a bounded buffer and
//! broadcasts them to followers; the control server serves both. Nothing
//! is persisted: history starts at daemon start.
//!
//! ```text
//! info!/warn! ──► LogLayer ──► LogBuffer ──► backlog  ──► control server
//!                                        └─► broadcast ─┘   (follow)
//! ```

use std::{
    collections::VecDeque,
    fmt::{self, Write as _},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use tokio::sync::broadcast;
use tracing::{Event, Level, Subscriber, field::Field, span};
use tracing_subscriber::{Layer, filter::Targets, layer::Context, registry::LookupSpan};

use super::control::{LogEntry, LogLevel};

/// Entries kept for the backlog.
const CAPACITY: usize = 1000;

/// Entries a follower may lag behind before it misses some.
const FOLLOW_QUEUE: usize = 256;

/// Maximum length of a recorded field value, in bytes: values embed
/// peer-provided errors, and the buffer outlives them.
const MAX_VALUE_LEN: usize = 4096;

/// The recent daemon events, shared by the tracing layer and the control
/// server.
#[derive(Debug, Clone)]
pub struct LogBuffer(Arc<Mutex<Inner>>);

#[derive(Debug)]
struct Inner {
    entries: VecDeque<LogEntry>,
    /// Entries evicted since the daemon started.
    dropped: u64,
    live: broadcast::Sender<LogEntry>,
}

/// The buffered entries and the receiver of those recorded after them.
#[derive(Debug)]
pub struct Subscription {
    pub entries: Vec<LogEntry>,
    /// Entries evicted before `entries`.
    pub dropped: u64,
    pub live: broadcast::Receiver<LogEntry>,
}

impl Default for LogBuffer {
    fn default() -> Self {
        LogBuffer(Arc::new(Mutex::new(Inner {
            entries: VecDeque::with_capacity(CAPACITY),
            dropped: 0,
            live: broadcast::Sender::new(FOLLOW_QUEUE),
        })))
    }
}

impl LogBuffer {
    /// The layer feeding this buffer with the daemon's info-and-above
    /// events, whatever the output filter.
    pub fn layer<S>(&self) -> impl Layer<S> + use<S>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let targets = Targets::new().with_target(env!("CARGO_CRATE_NAME"), Level::INFO);
        LogLayer(self.clone()).with_filter(targets)
    }

    /// Snapshots the buffer and subscribes to new entries, atomically: a
    /// follower misses or repeats nothing.
    pub fn subscribe(&self) -> Subscription {
        let inner = self.0.lock().expect("log buffer poisoned");
        Subscription {
            entries: inner.entries.iter().cloned().collect(),
            dropped: inner.dropped,
            live: inner.live.subscribe(),
        }
    }

    fn push(&self, entry: LogEntry) {
        let mut inner = self.0.lock().expect("log buffer poisoned");
        if inner.entries.len() == CAPACITY {
            inner.entries.pop_front();
            inner.dropped += 1;
        }
        if inner.live.receiver_count() > 0 {
            let _ = inner.live.send(entry.clone());
        }
        inner.entries.push_back(entry);
    }
}

/// The `tracing` layer behind [`LogBuffer::layer`].
struct LogLayer(LogBuffer);

impl<S> Layer<S> for LogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        // Enclosing spans (repo, workspace) fill in what the event lacks,
        // innermost first.
        for span in ctx.event_scope(event).into_iter().flatten() {
            if let Some(outer) = span.extensions().get::<Fields>() {
                fields.inherit(outer);
            }
        }

        let level = match *event.metadata().level() {
            Level::ERROR => LogLevel::Error,
            Level::WARN => LogLevel::Warn,
            // Lower levels are filtered out by `LogBuffer::layer`.
            _ => LogLevel::Info,
        };
        let mut entry = LogEntry {
            time: SystemTime::now(),
            level,
            repo: None,
            peer: None,
            message: String::new(),
            fields: String::new(),
        };
        for (name, value) in fields.0 {
            match name {
                "message" => entry.message = value,
                "repo" => entry.repo = Some(value),
                "peer" => entry.peer = Some(value),
                name => {
                    if !entry.fields.is_empty() {
                        entry.fields.push(' ');
                    }
                    let _ = write!(entry.fields, "{name}={value}");
                }
            }
        }
        self.0.push(entry);
    }
}

/// The recorded fields of an event or span, in order.
#[derive(Debug, Default)]
struct Fields(Vec<(&'static str, String)>);

impl Fields {
    /// Appends the fields of an enclosing span not already set.
    fn inherit(&mut self, outer: &Fields) {
        for (name, value) in &outer.0 {
            if !self.0.iter().any(|(own, _)| own == name) {
                self.0.push((name, value.clone()));
            }
        }
    }

    fn record(&mut self, field: &Field, mut value: String) {
        if value.len() > MAX_VALUE_LEN {
            value.truncate(value.floor_char_boundary(MAX_VALUE_LEN));
            value.push('…');
        }
        self.0.push((field.name(), value));
    }
}

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record(field, format!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
    use tracing::Instrument as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    /// Runs `f` with `logs` recording its events.
    fn record(logs: &LogBuffer, f: impl FnOnce()) {
        let subscriber = tracing_subscriber::registry().with(logs.layer());
        tracing::subscriber::with_default(subscriber, f);
    }

    #[test]
    fn records_daemon_events() {
        let logs = LogBuffer::default();
        record(&logs, || {
            tracing::debug!("too verbose");
            tracing::info!(repo = "r", peer = %"p", ops = 3, "synced");
            tracing::warn!(target: "iroh", "not ours");
        });

        let Subscription {
            entries, dropped, ..
        } = logs.subscribe();
        assert_eq!(dropped, 0);
        let [entry] = entries.as_slice() else {
            panic!("expected one entry: {entries:?}");
        };
        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.message, "synced");
        assert_eq!(entry.repo.as_deref(), Some("r"));
        assert_eq!(entry.peer.as_deref(), Some("p"));
        assert_eq!(entry.fields, "ops=3");
    }

    #[test]
    fn inherits_span_fields() {
        let logs = LogBuffer::default();
        record(&logs, || {
            let _repo = tracing::info_span!("repo", repo = "r", workspace = "outer").entered();
            let _workspace = tracing::info_span!("workspace", workspace = "w").entered();
            tracing::info!("updated");
            tracing::info!(workspace = "own", "snapshotted");
        });

        let entries = logs.subscribe().entries;
        assert_eq!(entries[0].repo.as_deref(), Some("r"));
        assert_eq!(entries[0].fields, "workspace=w");
        assert_eq!(entries[1].fields, "workspace=own");
    }

    #[tokio::test]
    async fn blocking_work_keeps_the_span() {
        let logs = LogBuffer::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(logs.layer()));
        let _guard = tracing::dispatcher::set_default(&dispatch);

        // Blocking threads see the global default, not this thread's: the
        // closure installs the dispatch itself, as production's global one.
        let work = async {
            crate::spawn_blocking(move || {
                tracing::dispatcher::with_default(&dispatch, || tracing::warn!("failed"));
            })
            .await
            .unwrap();
        };
        work.instrument(tracing::info_span!("repo", repo = "r"))
            .await;

        assert_eq!(logs.subscribe().entries[0].repo.as_deref(), Some("r"));
    }

    #[test]
    fn bounds_values() {
        let logs = LogBuffer::default();
        let long = "é".repeat(MAX_VALUE_LEN);
        record(&logs, || tracing::warn!("sync failed: {long}"));

        let message = &logs.subscribe().entries[0].message;
        assert!(message.len() <= MAX_VALUE_LEN + '…'.len_utf8());
        assert!(message.ends_with('…'));
    }

    #[test]
    fn evicts_oldest_and_broadcasts() {
        let logs = LogBuffer::default();
        record(&logs, || {
            for i in 0..CAPACITY + 2 {
                tracing::info!("{i}");
            }
        });
        let Subscription {
            entries,
            dropped,
            mut live,
        } = logs.subscribe();
        assert_eq!(dropped, 2);
        assert_eq!(entries.len(), CAPACITY);
        assert_eq!(entries[0].message, "2");

        record(&logs, || tracing::info!("new"));
        assert_eq!(live.try_recv().unwrap().message, "new");
    }
}

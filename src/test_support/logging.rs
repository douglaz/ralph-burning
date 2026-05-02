//! Structured tracing capture helpers for tests.
//!
//! # Examples
//!
//! ```ignore
//! use ralph_burning::test_support::logging::log_capture;
//!
//! let capture = log_capture();
//! capture.in_scope(|| {
//!     tracing::info!(operation = "sync_flush", bead_id = "bead-1", "mutation finished");
//! });
//! capture.assert_event_has_fields(&[
//!     ("operation", "sync_flush"),
//!     ("bead_id", "bead-1"),
//! ]);
//! ```

use std::collections::BTreeMap;
use std::fmt::Write;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::{Dispatch, Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

/// A captured tracing event with structured fields preserved as strings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CapturedLogEvent {
    pub level: String,
    pub target: String,
    pub fields: BTreeMap<String, String>,
}

impl CapturedLogEvent {
    /// Return a captured field by name.
    pub fn field(&self, key: &str) -> Option<&str> {
        self.fields.get(key).map(String::as_str)
    }
}

/// Shared sink that records tracing events for deterministic assertions.
#[derive(Clone, Debug, Default)]
pub struct StructuredLogCapture(Arc<Mutex<Vec<CapturedLogEvent>>>);

impl StructuredLogCapture {
    /// Clear all previously captured events.
    pub fn clear(&self) {
        self.0.lock().expect("log capture lock poisoned").clear();
    }

    fn dispatch(&self) -> Dispatch {
        Dispatch::new(tracing_subscriber::registry().with(CaptureLayer { sink: self.clone() }))
    }

    /// Run synchronous test code inside this capture's tracing scope.
    pub fn in_scope<T>(&self, body: impl FnOnce() -> T) -> T {
        self.clear();
        let dispatch = self.dispatch();
        tracing::dispatcher::with_default(&dispatch, body)
    }

    /// Run async test code inside this capture's tracing scope.
    ///
    /// Installs the capture's dispatch via both `set_default` (thread-local
    /// guard, covers events emitted on the polling thread between awaits)
    /// AND `with_subscriber` (per-poll wrap, covers events emitted from
    /// poll-time work on multi-threaded runtimes). Either alone is
    /// theoretically sufficient on the corresponding runtime, but the
    /// `service_emits_invocation_completed_trace_with_token_fields` test
    /// has historically been flaky in CI — combining both is belt-and-
    /// braces and matches the patterns used elsewhere in the tracing
    /// ecosystem when reliability matters more than minimal ceremony.
    pub async fn in_scope_async<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.clear();
        let dispatch = self.dispatch();
        let _guard = tracing::dispatcher::set_default(&dispatch);
        future.with_subscriber(dispatch).await
    }

    /// Return a snapshot of all captured events.
    pub fn snapshot(&self) -> Vec<CapturedLogEvent> {
        self.0.lock().expect("log capture lock poisoned").clone()
    }

    /// Assert that some captured event contains every expected field/value
    /// pair. Returns the matching event, or panics with the captured-event
    /// list for quick diagnosis.
    ///
    /// **Bounded retry semantics.** The implementation polls the snapshot
    /// up to [`ASSERT_EVENT_MAX_WAIT`] in [`ASSERT_EVENT_POLL_INTERVAL`]
    /// increments before declaring the event missing. The happy path —
    /// the event is already in the sink when the caller asserts — pays
    /// at most one snapshot clone and zero waits. The retry exists to
    /// absorb a class of CI-only flakes (bead `ii1n`) where on heavily
    /// contended runners the trace event lands a few milliseconds after
    /// the awaited future resolves, despite both `set_default` and
    /// `with_subscriber` being installed via `in_scope_async`.
    ///
    /// 6v3w's dispatcher belt-and-braces fix in PR #220 reduced but did
    /// not eliminate this race. We don't have a reliable local repro
    /// (30 isolated + 5 full-suite runs all pass), so a structural fix
    /// (e.g. tracing the actual spawn that escapes both subscribers)
    /// is blocked on better evidence. Until then this bounded poll is
    /// the conservative mitigation — fast on the happy path, tolerant
    /// on the flaky path, and clearly named so the next reader knows
    /// what it's compensating for.
    pub fn assert_event_has_fields(&self, expected: &[(&str, &str)]) -> CapturedLogEvent {
        let started = std::time::Instant::now();
        loop {
            let snapshot = self.snapshot();
            let event = snapshot
                .iter()
                .find(|event| {
                    expected
                        .iter()
                        .all(|(key, value)| event.field(key) == Some(*value))
                })
                .cloned();

            if let Some(event) = event {
                return event;
            }
            if started.elapsed() >= ASSERT_EVENT_MAX_WAIT {
                panic!(
                    "missing structured log event with fields [{}] after waiting {}ms\nCaptured events:\n{}",
                    expected
                        .iter()
                        .map(|(key, value)| format!("{key}={value}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    ASSERT_EVENT_MAX_WAIT.as_millis(),
                    render_events(&snapshot),
                );
            }
            std::thread::sleep(ASSERT_EVENT_POLL_INTERVAL);
        }
    }
}

/// Total wall time `assert_event_has_fields` will spend polling for a
/// matching event before declaring it missing. 100ms is plenty of slack
/// for any plausible "trace lands shortly after the awaited future
/// resolves" race while staying well under typical test runtimes.
const ASSERT_EVENT_MAX_WAIT: std::time::Duration = std::time::Duration::from_millis(100);
/// Per-poll snapshot interval inside `assert_event_has_fields`. 5ms gives
/// ~20 attempts within the 100ms window — fine-grained enough to catch
/// late events almost the moment they land, without spinning the CPU.
const ASSERT_EVENT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(5);

#[derive(Default)]
struct CaptureVisitor {
    fields: BTreeMap<String, String>,
}

impl Visit for CaptureVisitor {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), value.to_string());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), format!("{value:?}"));
    }
}

#[derive(Clone, Debug)]
struct CaptureLayer {
    sink: StructuredLogCapture,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = CaptureVisitor::default();
        event.record(&mut visitor);

        let mut captured = CapturedLogEvent {
            level: event.metadata().level().as_str().to_owned(),
            target: event.metadata().target().to_owned(),
            fields: visitor.fields,
        };
        captured
            .fields
            .entry("level".to_owned())
            .or_insert_with(|| captured.level.clone());
        captured
            .fields
            .entry("target".to_owned())
            .or_insert_with(|| captured.target.clone());

        self.sink
            .0
            .lock()
            .expect("log capture lock poisoned")
            .push(captured);
    }
}

fn render_events(events: &[CapturedLogEvent]) -> String {
    if events.is_empty() {
        return "<none>".to_owned();
    }

    let mut rendered = String::new();
    for (index, event) in events.iter().enumerate() {
        let _ = writeln!(
            rendered,
            "{index}: level={} target={} fields={:?}",
            event.level, event.target, event.fields
        );
    }
    rendered
}

/// Return a fresh structured tracing capture for a single test scope.
pub fn log_capture() -> StructuredLogCapture {
    StructuredLogCapture::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_capture_asserts_on_structured_fields() {
        let capture = log_capture();
        capture.in_scope(|| {
            tracing::info!(
                operation = "update_bead_status",
                bead_id = "bead-2",
                duration_ms = 7u64,
                "br mutation completed"
            );
        });

        let event = capture.assert_event_has_fields(&[
            ("operation", "update_bead_status"),
            ("bead_id", "bead-2"),
            ("duration_ms", "7"),
            ("message", "br mutation completed"),
        ]);

        assert_eq!(event.level, "INFO");
    }

    #[test]
    fn log_capture_scopes_events_per_capture() {
        let outer = log_capture();
        let inner = log_capture();

        outer.in_scope(|| {
            tracing::info!(scope = "outer", "outer event");

            inner.in_scope(|| {
                tracing::info!(scope = "inner", "inner event");
            });

            tracing::info!(scope = "outer", "outer event 2");
        });

        assert_eq!(outer.snapshot().len(), 2);
        outer.assert_event_has_fields(&[("scope", "outer"), ("message", "outer event")]);
        inner.assert_event_has_fields(&[("scope", "inner"), ("message", "inner event")]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn log_capture_supports_async_scopes() {
        let capture = log_capture();

        capture
            .in_scope_async(async {
                tracing::info!(scope = "async", attempt = 1u64, "async event");
            })
            .await;

        capture.assert_event_has_fields(&[
            ("scope", "async"),
            ("attempt", "1"),
            ("message", "async event"),
        ]);
    }
}

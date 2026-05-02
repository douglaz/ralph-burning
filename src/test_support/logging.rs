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
    /// Synchronous — does not yield to any runtime. Use this from sync
    /// tests, or from async tests where you know the trace is emitted
    /// synchronously inside the awaited future and is in the sink by the
    /// time you call this. For tests that may have post-await background
    /// work emitting the event (and need to give the runtime a chance to
    /// poll those tasks), use [`Self::assert_event_has_fields_within`].
    pub fn assert_event_has_fields(&self, expected: &[(&str, &str)]) -> CapturedLogEvent {
        let snapshot = self.snapshot();
        find_matching_event(&snapshot, expected)
            .unwrap_or_else(|| panic_missing_event(expected, &snapshot, None))
    }

    /// Async-friendly variant of [`Self::assert_event_has_fields`] that
    /// polls the snapshot up to `timeout`, yielding to the runtime via
    /// `tokio::time::sleep` between attempts. Use this when the event
    /// is plausibly emitted by a tokio task that hasn't been polled yet
    /// at the moment the test asserts — a `std::thread::sleep` would
    /// block the runtime and starve the very task that needs to fire.
    ///
    /// Reserved for the `ii1n` flake class: `service_emits_invocation_completed_trace_with_token_fields`
    /// occasionally fails on contended CI runners because the trace
    /// event arrives a few milliseconds after `service.invoke().await`
    /// resolves. Polling with `tokio::time::sleep` lets the runtime
    /// progress those late tasks during the wait window.
    pub async fn assert_event_has_fields_within(
        &self,
        expected: &[(&str, &str)],
        timeout: std::time::Duration,
    ) -> CapturedLogEvent {
        let started = std::time::Instant::now();
        let poll_interval = std::time::Duration::from_millis(5);
        loop {
            let snapshot = self.snapshot();
            if let Some(event) = find_matching_event(&snapshot, expected) {
                return event;
            }
            if started.elapsed() >= timeout {
                panic_missing_event(expected, &snapshot, Some(timeout));
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

fn find_matching_event(
    snapshot: &[CapturedLogEvent],
    expected: &[(&str, &str)],
) -> Option<CapturedLogEvent> {
    snapshot
        .iter()
        .find(|event| {
            expected
                .iter()
                .all(|(key, value)| event.field(key) == Some(*value))
        })
        .cloned()
}

fn panic_missing_event(
    expected: &[(&str, &str)],
    snapshot: &[CapturedLogEvent],
    waited: Option<std::time::Duration>,
) -> ! {
    let waited_msg = waited
        .map(|t| format!(" after waiting {}ms", t.as_millis()))
        .unwrap_or_default();
    panic!(
        "missing structured log event with fields [{}]{waited_msg}\nCaptured events:\n{}",
        expected
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(", "),
        render_events(snapshot),
    );
}

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

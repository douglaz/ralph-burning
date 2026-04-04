//! Structured tracing capture helpers for tests.
//!
//! # Examples
//!
//! ```ignore
//! use ralph_burning::test_support::logging::log_capture;
//!
//! let capture = log_capture();
//! capture.clear();
//! tracing::info!(operation = "sync_flush", bead_id = "bead-1", "mutation finished");
//! capture.assert_event_has_fields(&[
//!     ("operation", "sync_flush"),
//!     ("bead_id", "bead-1"),
//! ]);
//! ```

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex, OnceLock};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
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

    /// Return a snapshot of all captured events.
    pub fn snapshot(&self) -> Vec<CapturedLogEvent> {
        self.0.lock().expect("log capture lock poisoned").clone()
    }

    /// Assert that some captured event contains every expected field/value pair.
    ///
    /// Failure output includes the full captured event list for quick diagnosis.
    pub fn assert_event_has_fields(&self, expected: &[(&str, &str)]) -> CapturedLogEvent {
        let snapshot = self.snapshot();
        let event = snapshot
            .iter()
            .find(|event| {
                expected
                    .iter()
                    .all(|(key, value)| event.field(key) == Some(*value))
            })
            .cloned();

        match event {
            Some(event) => event,
            None => panic!(
                "missing structured log event with fields [{}]\nCaptured events:\n{}",
                expected
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                render_events(&snapshot),
            ),
        }
    }
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

/// Return the process-wide tracing capture sink used by tests.
pub fn log_capture() -> StructuredLogCapture {
    static LOG_CAPTURE: OnceLock<StructuredLogCapture> = OnceLock::new();

    LOG_CAPTURE
        .get_or_init(|| {
            let capture = StructuredLogCapture::default();
            let subscriber = tracing_subscriber::registry().with(CaptureLayer {
                sink: capture.clone(),
            });
            tracing::subscriber::set_global_default(subscriber)
                .expect("initialize global test log capture");
            capture
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_capture_asserts_on_structured_fields() {
        let capture = log_capture();
        capture.clear();

        tracing::info!(
            operation = "update_bead_status",
            bead_id = "bead-2",
            duration_ms = 7u64,
            "br mutation completed"
        );

        let event = capture.assert_event_has_fields(&[
            ("operation", "update_bead_status"),
            ("bead_id", "bead-2"),
            ("duration_ms", "7"),
            ("message", "br mutation completed"),
        ]);

        assert_eq!(event.level, "INFO");
    }
}

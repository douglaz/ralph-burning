//! Mock `br` adapter support for tests.
//!
//! The mock implements the same runner interface as the production
//! [`crate::adapters::br_process::BrAdapter`], so tests can inject it directly.
//!
//! # Examples
//!
//! ```ignore
//! use ralph_burning::adapters::br_process::BrCommand;
//! use ralph_burning::test_support::br::{MockBrAdapter, MockBrResponse};
//!
//! let mock = MockBrAdapter::from_responses([
//!     MockBrResponse::success(r#"{"id":"bead-1"}"#),
//! ]);
//! let adapter = mock.as_br_adapter();
//! let _ = tokio_test::block_on(adapter.exec_json::<serde_json::Value>(&BrCommand::show("bead-1")));
//! assert_eq!(mock.calls()[0].args, vec!["show", "bead-1", "--json"]);
//! ```

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::adapters::br_process::{BrAdapter, BrError, BrMutationAdapter, BrOutput, ProcessRunner};

type MockBrDispatch = dyn Fn(&MockBrCall) -> Option<MockBrResponse> + Send + Sync;

/// Recorded `br` invocation metadata for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockBrCall {
    pub args: Vec<String>,
    pub timeout: Duration,
    pub working_dir: Option<PathBuf>,
}

/// A queued `br` response with optional simulated latency.
#[derive(Debug)]
pub struct MockBrResponse {
    pub latency: Duration,
    pub result: Result<BrOutput, BrError>,
}

impl MockBrResponse {
    /// Successful `br` response with exit code `0`.
    pub fn success(stdout: impl Into<String>) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Ok(BrOutput {
                stdout: stdout.into(),
                stderr: String::new(),
                exit_code: 0,
            }),
        }
    }

    /// Non-zero process exit that the real adapter will map into `BrExitError`.
    pub fn exit_failure(exit_code: i32, stderr: impl Into<String>) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Ok(BrOutput {
                stdout: String::new(),
                stderr: stderr.into(),
                exit_code,
            }),
        }
    }

    /// Simulate a missing `br` binary.
    pub fn not_found(details: impl Into<String>) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Err(BrError::BrNotFound {
                details: details.into(),
            }),
        }
    }

    /// Simulate a timeout raised by the runner itself.
    pub fn timeout(command: impl Into<String>, timeout_ms: u64) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Err(BrError::BrTimeout {
                command: command.into(),
                timeout_ms,
            }),
        }
    }

    /// Attach latency to an otherwise configured response.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }
}

#[derive(Default)]
struct MockBrState {
    calls: Mutex<Vec<MockBrCall>>,
    responses: Mutex<VecDeque<MockBrResponse>>,
    dispatch: Option<Arc<MockBrDispatch>>,
    default_working_dir: Mutex<Option<PathBuf>>,
}

impl std::fmt::Debug for MockBrState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBrState")
            .field("calls", &self.calls)
            .field("responses", &self.responses)
            .field("dispatch", &self.dispatch.as_ref().map(|_| "<fn>"))
            .field(
                "default_working_dir",
                &self.default_working_dir.lock().map(|dir| dir.clone()).ok(),
            )
            .finish()
    }
}

/// Cloneable mock runner for `br` operations with full call tracking.
#[derive(Debug, Clone, Default)]
pub struct MockBrAdapter {
    state: Arc<MockBrState>,
}

impl MockBrAdapter {
    /// Create an empty mock runner.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mock preloaded with queued responses.
    pub fn from_responses<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = MockBrResponse>,
    {
        let mock = Self::new();
        for response in responses {
            mock.push_response(response);
        }
        mock
    }

    /// Create a mock that can synthesize responses from the incoming command.
    pub fn from_dispatch<F>(dispatch: F) -> Self
    where
        F: Fn(&MockBrCall) -> Option<MockBrResponse> + Send + Sync + 'static,
    {
        Self {
            state: Arc::new(MockBrState {
                dispatch: Some(Arc::new(dispatch)),
                ..MockBrState::default()
            }),
        }
    }

    /// Queue another response.
    pub fn push_response(&self, response: MockBrResponse) {
        self.state
            .responses
            .lock()
            .expect("mock br response lock poisoned")
            .push_back(response);
    }

    /// Return the full call history.
    pub fn calls(&self) -> Vec<MockBrCall> {
        self.state
            .calls
            .lock()
            .expect("mock br call lock poisoned")
            .clone()
    }

    /// Bind a default working directory for convenience adapters.
    pub fn set_default_working_dir(&self, path: PathBuf) {
        *self
            .state
            .default_working_dir
            .lock()
            .expect("mock br working-dir lock poisoned") = Some(path);
    }

    fn default_working_dir(&self) -> Option<PathBuf> {
        self.state
            .default_working_dir
            .lock()
            .expect("mock br working-dir lock poisoned")
            .clone()
    }

    /// Build a read-only adapter backed by this mock.
    pub fn as_br_adapter(&self) -> BrAdapter<Self> {
        let adapter = BrAdapter::with_runner(self.clone());
        if let Some(path) = self.default_working_dir() {
            adapter.with_working_dir(path)
        } else {
            adapter
        }
    }

    /// Build a mutation adapter backed by this mock.
    pub fn as_mutation_adapter(&self) -> BrMutationAdapter<Self> {
        BrMutationAdapter::with_adapter(self.as_br_adapter())
    }
}

impl ProcessRunner for MockBrAdapter {
    async fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        working_dir: Option<&std::path::Path>,
    ) -> Result<BrOutput, BrError> {
        let call = MockBrCall {
            args,
            timeout,
            working_dir: working_dir.map(std::path::Path::to_path_buf),
        };
        self.state
            .calls
            .lock()
            .expect("mock br call lock poisoned")
            .push(call.clone());

        let response = if let Some(dispatch) = self.state.dispatch.as_ref() {
            dispatch(&call)
        } else {
            None
        }
        .or_else(|| {
            self.state
                .responses
                .lock()
                .expect("mock br response lock poisoned")
                .pop_front()
        })
        .expect("mock br runner exhausted");

        if !response.latency.is_zero() {
            tokio::time::sleep(response.latency).await;
        }

        response.result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::br_process::BrCommand;

    #[tokio::test]
    async fn mock_br_adapter_tracks_calls_and_simulates_latency() {
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success("[]").with_latency(Duration::from_millis(5))
        ]);
        let adapter = mock.as_br_adapter();

        let started = std::time::Instant::now();
        let _ = adapter
            .exec_read(&BrCommand::ready())
            .await
            .expect("ready succeeds");

        assert!(started.elapsed() >= Duration::from_millis(5));
        assert_eq!(mock.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn mock_br_adapter_supports_runner_errors() {
        let mock = MockBrAdapter::from_responses([MockBrResponse::not_found("could not find br")]);
        let adapter = mock.as_br_adapter();

        let error = adapter
            .exec_read(&BrCommand::ready())
            .await
            .expect_err("runner error should surface");

        assert!(matches!(error, BrError::BrNotFound { .. }));
    }

    #[tokio::test]
    async fn mock_br_adapter_can_dispatch_by_command() {
        let mock = MockBrAdapter::from_dispatch(|call| match call.args.as_slice() {
            [command, flag] if command == "ready" && flag == "--json" => {
                Some(MockBrResponse::success("[]"))
            }
            _ => None,
        });
        let adapter = mock.as_br_adapter();

        let ready = adapter
            .exec_read(&BrCommand::ready())
            .await
            .expect("ready succeeds via dispatch");

        assert_eq!(ready.stdout, "[]");
        assert_eq!(mock.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn mock_br_adapter_applies_default_working_dir_to_convenience_adapter() {
        let mock = MockBrAdapter::from_responses([MockBrResponse::success("[]")]);
        let working_dir = std::env::temp_dir().join("mock-br-default-working-dir");
        mock.set_default_working_dir(working_dir.clone());

        let adapter = mock.as_br_adapter();
        let _ = adapter
            .exec_read(&BrCommand::ready())
            .await
            .expect("ready succeeds");

        assert_eq!(mock.calls()[0].working_dir, Some(working_dir));
    }
}

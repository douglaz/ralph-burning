//! Mock `bv` adapter support for tests.
//!
//! # Examples
//!
//! ```ignore
//! use ralph_burning::adapters::bv_process::BvCommand;
//! use ralph_burning::test_support::bv::{MockBvAdapter, MockBvResponse};
//!
//! let mock = MockBvAdapter::from_responses([
//!     MockBvResponse::success(r#"{"id":"bead-7","title":"Pick next"}"#),
//! ]);
//! let adapter = mock.as_bv_adapter();
//! let _ = tokio_test::block_on(adapter.exec_json::<serde_json::Value>(&BvCommand::robot_next()));
//! assert_eq!(mock.calls()[0].args, vec!["--robot-next"]);
//! ```

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::adapters::bv_process::{BvAdapter, BvError, BvOutput, BvProcessRunner};

type MockBvDispatch = dyn Fn(&MockBvCall) -> Option<MockBvResponse> + Send + Sync;

/// Recorded `bv` invocation metadata for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockBvCall {
    pub args: Vec<String>,
    pub timeout: Duration,
    pub working_dir: Option<PathBuf>,
}

/// A queued `bv` response with optional simulated latency.
#[derive(Debug)]
pub struct MockBvResponse {
    pub latency: Duration,
    pub result: Result<BvOutput, BvError>,
}

impl MockBvResponse {
    /// Successful `bv` response with exit code `0`.
    pub fn success(stdout: impl Into<String>) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Ok(BvOutput {
                stdout: stdout.into(),
                stderr: String::new(),
                exit_code: 0,
            }),
        }
    }

    /// Non-zero process exit that the real adapter will map into `BvExitError`.
    pub fn exit_failure(exit_code: i32, stderr: impl Into<String>) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Ok(BvOutput {
                stdout: String::new(),
                stderr: stderr.into(),
                exit_code,
            }),
        }
    }

    /// Simulate a missing `bv` binary.
    pub fn not_found(details: impl Into<String>) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Err(BvError::BvNotFound {
                details: details.into(),
            }),
        }
    }

    /// Simulate a timeout raised by the runner itself.
    pub fn timeout(command: impl Into<String>, timeout_ms: u64) -> Self {
        Self {
            latency: Duration::ZERO,
            result: Err(BvError::BvTimeout {
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
struct MockBvState {
    calls: Mutex<Vec<MockBvCall>>,
    responses: Mutex<VecDeque<MockBvResponse>>,
    dispatch: Option<Arc<MockBvDispatch>>,
    default_working_dir: Mutex<Option<PathBuf>>,
}

impl std::fmt::Debug for MockBvState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockBvState")
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

/// Cloneable mock runner for `bv` operations with full call tracking.
#[derive(Debug, Clone, Default)]
pub struct MockBvAdapter {
    state: Arc<MockBvState>,
}

impl MockBvAdapter {
    /// Create an empty mock runner.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a mock preloaded with queued responses.
    pub fn from_responses<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = MockBvResponse>,
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
        F: Fn(&MockBvCall) -> Option<MockBvResponse> + Send + Sync + 'static,
    {
        Self {
            state: Arc::new(MockBvState {
                dispatch: Some(Arc::new(dispatch)),
                ..MockBvState::default()
            }),
        }
    }

    /// Queue another response.
    pub fn push_response(&self, response: MockBvResponse) {
        self.state
            .responses
            .lock()
            .expect("mock bv response lock poisoned")
            .push_back(response);
    }

    /// Return the full call history.
    pub fn calls(&self) -> Vec<MockBvCall> {
        self.state
            .calls
            .lock()
            .expect("mock bv call lock poisoned")
            .clone()
    }

    /// Bind a default working directory for convenience adapters.
    pub fn set_default_working_dir(&self, path: PathBuf) {
        *self
            .state
            .default_working_dir
            .lock()
            .expect("mock bv working-dir lock poisoned") = Some(path);
    }

    fn default_working_dir(&self) -> Option<PathBuf> {
        self.state
            .default_working_dir
            .lock()
            .expect("mock bv working-dir lock poisoned")
            .clone()
    }

    /// Build a read-only adapter backed by this mock.
    pub fn as_bv_adapter(&self) -> BvAdapter<Self> {
        let adapter = BvAdapter::with_runner(self.clone());
        if let Some(path) = self.default_working_dir() {
            adapter.with_working_dir(path)
        } else {
            adapter
        }
    }
}

impl BvProcessRunner for MockBvAdapter {
    async fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        working_dir: Option<&std::path::Path>,
    ) -> Result<BvOutput, BvError> {
        let call = MockBvCall {
            args,
            timeout,
            working_dir: working_dir.map(std::path::Path::to_path_buf),
        };
        self.state
            .calls
            .lock()
            .expect("mock bv call lock poisoned")
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
                .expect("mock bv response lock poisoned")
                .pop_front()
        })
        .expect("mock bv runner exhausted");

        if !response.latency.is_zero() {
            tokio::time::sleep(response.latency).await;
        }

        response.result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::bv_process::BvCommand;

    #[tokio::test]
    async fn mock_bv_adapter_tracks_calls() {
        let mock = MockBvAdapter::from_responses([MockBvResponse::success("{}")]);
        let adapter = mock.as_bv_adapter();

        let _ = adapter
            .exec_read(&BvCommand::robot_triage())
            .await
            .expect("triage succeeds");

        assert_eq!(mock.calls()[0].args, vec!["--robot-triage"]);
    }

    #[tokio::test]
    async fn mock_bv_adapter_supports_runner_errors() {
        let mock =
            MockBvAdapter::from_responses([MockBvResponse::timeout("bv --robot-next", 30000)]);
        let adapter = mock.as_bv_adapter();

        let error = adapter
            .exec_read(&BvCommand::robot_next())
            .await
            .expect_err("runner timeout should surface");

        assert!(matches!(error, BvError::BvTimeout { .. }));
    }

    #[tokio::test]
    async fn mock_bv_adapter_can_dispatch_by_command() {
        let mock = MockBvAdapter::from_dispatch(|call| match call.args.as_slice() {
            [command] if command == "--robot-next" => Some(MockBvResponse::success("{}")),
            _ => None,
        });
        let adapter = mock.as_bv_adapter();

        let next = adapter
            .exec_read(&BvCommand::robot_next())
            .await
            .expect("robot next succeeds via dispatch");

        assert_eq!(next.stdout, "{}");
        assert_eq!(mock.calls()[0].args, vec!["--robot-next"]);
    }

    #[tokio::test]
    async fn mock_bv_adapter_applies_default_working_dir_to_convenience_adapter() {
        let mock = MockBvAdapter::from_responses([MockBvResponse::success("{}")]);
        let working_dir = std::env::temp_dir().join("mock-bv-default-working-dir");
        mock.set_default_working_dir(working_dir.clone());

        let adapter = mock.as_bv_adapter();
        let _ = adapter
            .exec_read(&BvCommand::robot_next())
            .await
            .expect("robot next succeeds");

        assert_eq!(mock.calls()[0].working_dir, Some(working_dir));
    }
}

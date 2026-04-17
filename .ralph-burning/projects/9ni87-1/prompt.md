# Add sequential runtime tests for happy path, blocked path, restart, and tool failure

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add new test functions only. Do NOT modify existing source code or test files.

## Background — what already exists

### Test infrastructure (`src/test_support/`):
- `MockBrAdapter` with `MockBrResponse::success()`, `.exit_failure()`, `.not_found()`, `.timeout()`
- `MockBvAdapter` with same pattern
- `TempWorkspaceBuilder::new().with_milestone().with_bead_graph().build()`
- `MilestoneFixtureBuilder::new().with_name().add_bead().with_task_run().with_journal_event()`
- `StructuredLogCapture` via `log_capture()` with `.in_scope()`, `.assert_event_has_fields()`

### Controller state machine (`src/contexts/milestone_record/controller.rs`):
- States: `Idle, Selecting, Claimed, Running, Reconciling, Blocked, NeedsOperator, Completed`
- Key functions:
  - `initialize_controller()` — create controller in Idle
  - `sync_controller_task_claimed()` — transition to Claimed
  - `sync_controller_task_running()` — transition to Running
  - `sync_controller_task_reconciling()` — transition to Reconciling
  - `load_controller()` — read current state
  - `resume_controller()` — resume from checkpoint
  - `checkpoint_controller_stop()` — checkpoint without transition

### Existing test patterns:
- Tests use `#[tokio::test]` for async
- Tests return `Result<()>` with `?` operator
- Use `TempWorkspaceBuilder` for workspace setup
- Use `MockBrAdapter` for deterministic br/bv responses

## What to implement

### Create `tests/unit/controller_runtime_test.rs`

Add these test scenarios:

#### 1. Happy path: `test_happy_path_idle_through_reconcile_to_completed`
- Initialize controller in Idle state
- Transition through: Idle → Selecting → Claimed → Running → Reconciling → Idle (or Completed)
- Verify each state transition is recorded
- Verify journal events are appended for each transition
- Verify milestone progress advances

#### 2. Blocked path: `test_blocked_when_no_ready_beads`
- Initialize controller
- Simulate `bv --robot-next` returning no beads (empty/no-work response)
- Verify controller transitions to Blocked state
- Verify the reason field explains what's blocking
- Verify controller can resume when a bead becomes ready

#### 3. Restart/resume: `test_restart_resumes_from_persisted_state`
- Initialize controller and advance to Running state
- Call `checkpoint_controller_stop()` to persist state
- Create a new controller instance and call `resume_controller()`
- Verify it resumes in Running state without re-doing prior transitions
- Verify state continuity (same bead_id, task_id)

#### 4. Tool failure: `test_tool_failure_transitions_to_needs_operator`
- Initialize controller and advance to Running state
- Simulate br command failure (using MockBrAdapter with exit_failure response)
- Verify controller transitions to NeedsOperator
- Verify error context is recorded in journal
- Verify the reason field contains the error message

#### 5. One-at-a-time invariant: `test_sequential_execution_enforced`
- Initialize controller with a bead claimed
- Attempt to claim a second bead while first is still running
- Verify the second claim is rejected
- Verify only one active bead at a time

### Wire into test suite

Add `mod controller_runtime_test;` to `tests/unit/mod.rs` if it exists, or ensure the test file is discovered by cargo test.

## Implementation guidance

- Follow existing test patterns in `tests/unit/` — look at `workflow_engine_test.rs` or `automation_runtime_test.rs` for style
- Use `MockBrAdapter` queue pattern: push responses in reverse order (`.pop()` consumes from end)
- Each test should be independent — create its own workspace and fixtures
- Use `StructuredLogCapture` to verify tracing events contain expected fields (milestone_id, bead_id, etc.)
- Tests must be deterministic — no timing dependencies, no shared state

## Files to create/modify

- `tests/unit/controller_runtime_test.rs` (new) — all runtime tests
- `tests/unit/mod.rs` — add module declaration if needed

## Acceptance Criteria
- All 5 test scenarios implemented and passing
- Tests use mock adapters (no real br/bv calls)
- Tests verify state transitions, journal events, and error handling
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

# Implementation Response (Iteration 3)

## Changes Made
1. **Start and stage-entry failure boundaries** (`engine.rs`): Wrapped `run_started` journal append (line 193), `stage_entered` journal append (line 230), and stage-entry snapshot cursor update (line 256) in error handling that routes through `fail_run`. Failures at any of these boundaries now persist `failed` state instead of bubbling as raw errors that leave the run in an ambiguous `running` state. If the journal append fails, the sequence counter is decremented before calling `fail_run` so the unpersisted event is not counted.

2. **Resilient `fail_run` with snapshot-first ordering** (`engine.rs`): Restructured `fail_run` to write the failed snapshot **first** (critical path), then append the `run_failed` journal event as best-effort. This ensures the run is never left in `running` state even if the journal store is completely broken. Previously, journal was written first with `?` propagation, which could skip the snapshot write entirely.

3. **Snapshot-first `run_completed` terminal boundary** (`engine.rs`): Reordered the completion path to write the completed snapshot before appending `run_completed` to the journal, consistent with snapshot-first ordering used throughout the engine. This addresses the recommended improvement about terminal boundary durability.

4. **CLI integration test: backend preflight failure** (`tests/cli.rs`): Added `run_start_backend_preflight_failure_leaves_state_unchanged` which uses `RALPH_BURNING_TEST_BACKEND_UNAVAILABLE=1` to make the stub backend unavailable at preflight. Verifies byte-identical `run.json` and journal, zero payloads, and zero artifacts after failure.

5. **CLI integration test: mid-stage failure** (`tests/cli.rs`): Added `run_start_mid_stage_failure_no_partial_durable_history` which uses `RALPH_BURNING_TEST_FAIL_INVOKE_STAGE=prompt_review` to fail the first stage's invocation. Verifies `failed` snapshot status, `active_run=null`, zero payloads/artifacts, no `stage_completed` events, and journal ending with `run_failed`.

6. **Test injection seam** (`cli/run.rs`, `stub_backend.rs`, `domain.rs`): Added `StageId::FromStr` implementation, `StubBackendAdapter::with_invoke_failure()` method for stages that pass preflight but fail on invoke, and env var injection seam in `build_agent_execution_service()` (`RALPH_BURNING_TEST_BACKEND_UNAVAILABLE`, `RALPH_BURNING_TEST_FAIL_INVOKE_STAGE`).

7. **Unit tests for entry boundary failures** (`workflow_engine_test.rs`): Added `stage_entered_journal_failure_persists_failed_state` (journal fails on stage_entered append) and `run_started_journal_failure_persists_failed_state` (journal fails on run_started append). Both verify the run ends in `Failed` state with no payloads and no leaked events.

## Could Not Address
- None

## Verification
- `cargo test`: **256 tests passed** (67 CLI + 189 unit), 0 failed
- `cargo clippy --all-targets --all-features -- -D warnings`: **clean**

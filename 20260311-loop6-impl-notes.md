# Loop 6 Implementation Notes: Standard Preset Run Start Orchestration

## Decisions

### Engine Architecture
- The orchestration engine lives in `workflow_composition::engine` as a set of free functions rather than a struct, keeping it stateless and easy to test. The engine receives all dependencies as trait-object references.
- `execute_standard_run` accepts 9 parameters (with `#[allow(clippy::too_many_arguments)]`). This was a deliberate trade-off: a config struct would add indirection without reducing complexity, and the engine is called from exactly one CLI entry point.

### Preflight Check Strategy
- Preflight validates capability + availability for **all** stages before any mutation. This means a single missing backend capability fails the entire run before touching any durable state.
- The `AgentExecutionService` exposes its inner adapter via `adapter()` so the engine can call `check_capability` / `check_availability` directly, avoiding an unnecessary wrapper layer.

### Atomic Stage Commit
- Each stage commit writes: payload -> artifact -> journal event -> snapshot update.
- If artifact write fails, the payload file is cleaned up (rollback). If journal append fails after payload/artifact, a `StageCommitFailed` error is returned.
- Runtime logs are best-effort (`let _ = ...`) and never affect durable state.

### Stage Plan Derivation
- `standard_stage_plan(prompt_review_enabled)` filters the flow definition's stage list, removing `PromptReview` when disabled.
- `role_for_stage` delegates to `BackendRole::for_stage` for deterministic stage-to-role mapping.

### Run ID Generation
- Format: `run-YYYYMMDDHHMMSS` using `Utc::now()`. Simple and human-readable. No UUID dependency needed for this slice.

### Only Standard Flow
- Non-standard flows return `UnsupportedFlow` error immediately in the CLI handler, before any engine code runs.

## Deviations

### None
All 17 acceptance criteria are addressed. No deviations from the spec.

## Files Modified

| File | Change |
|------|--------|
| `src/shared/error.rs` | +4 error variants: `RunStartFailed`, `UnsupportedFlow`, `PreflightFailed`, `StageCommitFailed` |
| `src/contexts/project_run_record/model.rs` | +`FailedStageSummary` struct, +`Display` impl for `RunStatus` |
| `src/contexts/project_run_record/journal.rs` | +5 lifecycle event builders: `run_started_event`, `stage_entered_event`, `stage_completed_event`, `run_completed_event`, `run_failed_event` |
| `src/contexts/project_run_record/service.rs` | +3 port traits: `RunSnapshotWritePort`, `PayloadArtifactWritePort`, `RuntimeLogWritePort` |
| `src/contexts/project_run_record/mod.rs` | Updated exports for new port traits |
| `src/adapters/fs.rs` | +3 adapter structs: `FsRunSnapshotWriteStore`, `FsPayloadArtifactWriteStore`, `FsRuntimeLogWriteStore` |
| `src/contexts/agent_execution/service.rs` | +`adapter()` method on `AgentExecutionService` |
| `src/cli/run.rs` | Rewrote `handle_start()` from stub to full orchestration |

## Files Created

| File | Purpose |
|------|---------|
| `src/contexts/workflow_composition/engine.rs` | Core orchestration engine with stage plan derivation, preflight checks, and stage execution loop |
| `tests/unit/workflow_engine_test.rs` | 10 unit tests for engine functions |
| `tests/conformance/features/run_start_standard.feature` | 17 Gherkin scenarios for run start conformance |

## Testing

### Unit Tests (185 total, all pass)
- `workflow_engine_test`: 10 tests covering stage plan derivation, role mapping, preflight checks, happy path execution, and failure handling
- `project_run_record_test`: 4 new tests for `FailedStageSummary` serialization, `RunStatus::Display`, and terminal snapshot states
- All pre-existing tests continue to pass

### CLI Integration Tests (63 total, all pass)
- 10 new `run start` tests:
  - `run_start_completes_standard_flow_end_to_end` - happy path
  - `run_start_produces_completed_snapshot` - verifies run.json state
  - `run_start_persists_journal_events` - verifies all lifecycle events
  - `run_start_persists_payload_and_artifact_records` - verifies 8 payload + 8 artifact files
  - `run_start_status_shows_completed_after_run` - post-run query validation
  - `run_start_rejects_non_standard_flow` - UnsupportedFlow error
  - `run_start_rejects_already_completed_project` - precondition check
  - `run_start_rejects_already_running_project` - precondition check
  - `run_start_without_active_project_fails` - no active project error

### Gherkin Conformance
- 17 scenarios in `run_start_standard.feature` covering all acceptance criteria

### Build Verification
- `cargo check --all-targets`: pass
- `cargo clippy --all-targets --all-features -- -D warnings`: pass
- `cargo test`: 248 tests pass (185 unit + 63 CLI integration)

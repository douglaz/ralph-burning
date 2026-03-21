# Implementation Notes — Loop 8: Completion Rounds, Late-Stage Acceptance, and Durable Amendments

## Decisions Made

- **Late-stage vs remediation-stage separation**: Created a clean branching structure in `execute_standard_run_internal` that routes validation outcomes through two distinct paths based on `is_late_stage()`. Late stages (completion_panel, acceptance_qa, final_review) use completion-round semantics; non-late stages (qa, review) use the existing remediation-cycle behavior from loop 7.

- **Durable amendment lifecycle**: Late-stage amendments are persisted to disk first (atomic write via `FsAmendmentQueueStore`), then journal events are emitted, then the run snapshot is updated. This ordering ensures failure at any step leaves the system in a recoverable state. Amendments are drained from disk only after successful planning commit in the next completion round.

- **Typed `QueuedAmendment` model**: Replaced the untyped `Vec<serde_json::Value>` in `AmendmentQueueState.pending` with `Vec<QueuedAmendment>`, a struct carrying `amendment_id`, `source_stage`, `source_cycle`, `source_completion_round`, `body`, and `created_at`. This enables typed amendment injection into planning context and deterministic ordering.

- **Non-late-stage `ConditionallyApproved` no longer queues amendments**: The loop-7 behavior of appending non-late-stage conditional amendments to `amendment_queue.pending` was removed because it conflicts with the completion guard. Non-late-stage amendments are already captured in stage payloads/artifacts for observability.

- **Amendment injection into planning**: When planning runs with pending amendments (completion_round > 1), the invocation context includes a `pending_amendments` array under the `remediation` key, providing the planning agent with typed amendment records to incorporate.

- **Completion guard dual-check**: The guard checks both `snapshot.amendment_queue.pending.is_empty()` and `amendment_queue_port.has_pending_amendments()` (disk) before allowing `run_completed`. This defense-in-depth catches inconsistencies between snapshot state and filesystem.

## Files Modified

### Source
- `src/contexts/project_run_record/model.rs` — Added `QueuedAmendment` struct, updated `AmendmentQueueState.pending` from `Vec<serde_json::Value>` to `Vec<QueuedAmendment>`
- `src/contexts/project_run_record/journal.rs` — Added `completion_round_advanced_event()` and `amendment_queued_event()` builder functions
- `src/contexts/project_run_record/service.rs` — Added `AmendmentQueuePort` trait (5 methods: write_amendment, list_pending_amendments, remove_amendment, drain_amendments, has_pending_amendments)
- `src/contexts/workflow_composition/engine.rs` — Major restructure: added `is_late_stage()`, `build_queued_amendments()`, `completion_guard()`, `reconcile_amendments_from_disk()` helpers; late-stage validation handling with completion-round advancement; amendment injection into planning; amendment drain after planning commit; cycle_advanced emission for completion-round restarts; `CompletionRoundAdvanced` handling in `derive_resume_state`; amendment reconciliation from disk during resume
- `src/adapters/fs.rs` — Added `FsAmendmentQueueStore` implementing `AmendmentQueuePort` with atomic filesystem persistence under `projects/<id>/amendments/`
- `src/cli/run.rs` — Updated `handle_start` and `handle_resume` to construct `FsAmendmentQueueStore` and pass to engine calls
- `src/shared/error.rs` — Added `AmendmentQueueError` and `CompletionBlocked` error variants

### Tests
- `tests/unit/workflow_engine_test.rs` — Updated 24 existing call sites with new `amendment_queue_port` parameter; updated `conditionally_approved_queues_amendments_and_proceeds` to assert non-late-stage ConditionallyApproved no longer queues; added 5 new tests:
  - `late_stage_conditionally_approved_triggers_completion_round_advancement`
  - `late_stage_rejected_causes_terminal_failure`
  - `late_stage_approved_advances_to_next_late_stage`
  - `late_stage_request_changes_triggers_completion_round_like_conditional`
  - `cycle_advanced_emitted_when_entering_implementation_from_completion_round`
- `tests/unit/journal_test.rs` — Added 2 tests for new journal event builders:
  - `completion_round_advanced_event_builder_serializes_round_metadata`
  - `amendment_queued_event_builder_serializes_amendment_metadata`
- `tests/unit/adapter_contract_test.rs` — Added 8 tests for `FsAmendmentQueueStore`:
  - `amendment_queue_write_and_list_round_trip`
  - `amendment_queue_empty_returns_empty_list`
  - `amendment_queue_has_pending_returns_true_when_present`
  - `amendment_queue_remove_deletes_single_amendment`
  - `amendment_queue_remove_nonexistent_is_ok`
  - `amendment_queue_drain_removes_all_and_returns_count`
  - `amendment_queue_drain_empty_returns_zero`
  - `amendment_queue_corrupt_json_returns_error`
- `tests/conformance/features/run_completion_rounds.feature` — New conformance feature with 12 scenarios (pre-existing from planning)

## Spec Deviations

- **Non-late-stage `ConditionallyApproved` amendment queuing removed**: The spec describes amendments being queued for all conditionally_approved outcomes. The implementation restricts amendment queuing to late stages only, because the completion guard would block completion for non-late-stage amendments that have no drain path. Non-late-stage conditional amendments are still captured in stage payloads/artifacts.

## Testing

- All 230 unit tests pass (`cargo test --test unit`)
- All 67 CLI tests pass (`cargo test --test cli`)
- Full test suite: 297 tests pass, 0 failures
- Compilation clean with `cargo check --tests`

---

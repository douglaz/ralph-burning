# Implementation Response (Iteration 3)

## Changes Made
1. **Shared `release()` contract (Required Change 1)**: Fixed `LeaseService::release()` so non-reconcile callers get an accurate `resources_released` signal:
   - **`lease_service.rs` (ReleaseResult)**: Changed `resources_released` from unconditionally `true` to computed: it is `true` only when every sub-step (worktree, lease file, writer lock) positively succeeded — no errors, no already-absent states. Added `has_cleanup_failures()` convenience method. Updated doc comments to reflect the contract.
   - **`lease_service.rs` (release)**: `LeaseReleased` journal event is now only emitted when `resources_released` is `true`. Partial cleanup failure no longer records a release event, keeping durable lease state visible for operator recovery.
   - **`daemon.rs` (cleanup_aborted_task)**: Replaced the unconditional `clear_lease_reference` call with a conditional check: lease reference is only cleared when `resources_released` is `true`. Partial failure returns `AppError::LeaseCleanupPartialFailure` so the task's `lease_id` stays intact.
   - **`daemon_loop.rs` (release_task_lease)**: Same fix: lease reference is only cleared when `resources_released` is `true`. Partial failure returns `LeaseCleanupPartialFailure` instead of silently clearing state.
   - **`task_service.rs` (claim_task)**: Already checked `resources_released` at lines 193–195 and 283–285. With the field now correctly computed, partial cleanup during claim rollback correctly enters the `else` branch (marks task Failed, preserves `lease_id`). No code change needed.
   - **`error.rs`**: Added `LeaseCleanupPartialFailure { task_id }` variant for non-reconcile callers to report partial release failures.

2. **Regression coverage for non-reconcile release callers (Required Change 2)**: Added six focused tests in `automation_runtime_test.rs`:
   - `release_with_lease_file_error_sets_resources_released_false` — direct `release()` with lease-file error → `resources_released: false`, `LeaseReleased` journal NOT emitted.
   - `release_with_writer_lock_error_sets_resources_released_false` — direct `release()` with writer-lock error → `resources_released: false`, `LeaseReleased` journal NOT emitted.
   - `release_full_success_sets_resources_released_true_and_emits_journal` — direct `release()` with all sub-steps succeeding → `resources_released: true`, `LeaseReleased` journal emitted.
   - `abort_cleanup_preserves_lease_reference_on_partial_failure` — simulates abort cleanup path with lease-file error → task's `lease_id` NOT cleared.
   - `daemon_loop_cleanup_preserves_lease_reference_on_partial_failure` — simulates daemon-loop release path with writer-lock error → task's `lease_id` NOT cleared.
   - All tests use the existing `SubStepErrorStore` test adapter for injection.

## Could Not Address
- None — all required changes have been addressed.

## Pending Changes (Pre-Commit)
- `src/contexts/automation_runtime/lease_service.rs` — fixed `ReleaseResult` semantics, `release()` journal gating, added `has_cleanup_failures()` method
- `src/shared/error.rs` — added `LeaseCleanupPartialFailure` variant
- `src/cli/daemon.rs` — fixed `cleanup_aborted_task` to conditionally clear lease reference
- `src/contexts/automation_runtime/daemon_loop.rs` — fixed `release_task_lease` to conditionally clear lease reference
- `tests/unit/automation_runtime_test.rs` — 6 new regression tests for non-reconcile release callers

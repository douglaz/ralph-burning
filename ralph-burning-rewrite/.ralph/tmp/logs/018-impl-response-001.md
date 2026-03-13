# Implementation Response (Iteration 1)

## Changes Made
1. **Claim/reconcile cleanup atomicity** (Required Change 1)
   - `lease_service.rs`: Rewrote `release()` to attempt worktree removal *first*. If worktree removal fails, the lease file, writer lock, and `LeaseReleased` journal event are all preserved intact, keeping the lease durable for a later `reconcile` pass. Only when worktree removal succeeds do we proceed with lease file deletion, writer lock release, and journal event.
   - `lease_service.rs`: Rewrote `reconcile()` cleanup accounting. When `release()` fails, the cleanup failure is recorded and the lease remains durable (AC 8: task remains terminal but recoverable). When `release()` succeeds but `clear_lease_reference` fails, the lease is NOT reported as released and the task remains visibly inconsistent for operator repair (AC 8).
   - `task_service.rs`: Fixed `claim_task` LeaseAcquired journal failure branch to: (a) capture the `release()` error instead of ignoring it, (b) append a compensating `ClaimRollback` journal event documenting the rollback target and whether the lease was released, (c) on rollback failure, write the task to `Failed` state with `claim_journal_failed` class AND clear `lease_id`.
   - `task_service.rs`: Fixed `claim_task` TaskClaimed journal failure branch to: (a) capture `release()` error, (b) transition the task to `Failed` with `claim_journal_failed` class AND clear `lease_id`, (c) append a compensating `ClaimRollback` journal event.
   - `model.rs`: Added `ClaimRollback` variant to `DaemonJournalEventType` for compensating journal evidence.

2. **Daemon same-cycle scanning** (Required Change 2)
   - `daemon_loop.rs`: Replaced the single-task `.find()` + `process_task()` pattern with a `filter().collect()` that gathers all pending tasks, then iterates over each one. Per-task claim failures or writer-lock contention are logged but do not stop the scan — the daemon continues with remaining eligible tasks. This applies to both `--single-iteration` and continuous mode (AC 6).

3. **Regression coverage and conformance alignment** (Required Change 3)
   - `automation_runtime_test.rs`: Added `FailingWorktreeAdapter` (WorktreePort impl whose `remove_worktree` always fails) and test `reconcile_partial_cleanup_failure_keeps_lease_durable` — verifies that when worktree removal fails, the lease remains durable on disk, `released_lease_ids` is empty, and cleanup failures are reported.
   - `automation_runtime_test.rs`: Added `FailingJournalStore` (DaemonStorePort wrapper that makes `append_daemon_journal_event` fail after N calls) and two tests: `claim_journal_failure_rolls_back_to_pending_not_stranded_claimed` (LeaseAcquired journal fails → task ends Pending or Failed, never Claimed, lease_id cleared) and `claim_task_claimed_journal_failure_marks_failed_with_cleared_lease` (TaskClaimed journal fails → task ends Failed with `claim_journal_failed`, lease_id cleared).
   - `automation_runtime_test.rs`: Added `cli_writer_lock_guard_releases_on_drop` test for panic-safe RAII lock release.
   - `automation_runtime_test.rs`: Added `daemon_loop_process_cycle_does_not_call_set_current_dir` structural assertion test via `include_str!`.
   - `scenarios.rs` DAEMON-LIFECYCLE-006: Now creates a stale lease with a non-existent worktree path, runs `daemon reconcile --ttl-seconds 0`, and asserts: non-zero exit, stdout contains "Cleanup Failures", stdout contains the failing lease and task IDs.
   - `scenarios.rs` DAEMON-LIFECYCLE-007: Now creates two pending tasks where the first task's project writer lock is externally held, runs `daemon start --single-iteration`, and asserts the daemon attempted the second task.
   - `scenarios.rs` DAEMON-LIFECYCLE-008: Now performs a source-level structural assertion that `daemon_loop.rs` does not contain `set_current_dir`, plus a runtime CWD preservation check.

## Could Not Address
- None

## Pending Changes (Pre-Commit)
- `src/contexts/automation_runtime/model.rs` — added `ClaimRollback` journal event variant
- `src/contexts/automation_runtime/lease_service.rs` — fixed `release()` and `reconcile()` cleanup ordering and failure accounting
- `src/contexts/automation_runtime/task_service.rs` — fixed claim rollback atomicity with compensating journal evidence and lease_id cleanup
- `src/contexts/automation_runtime/daemon_loop.rs` — iterate all pending tasks per cycle
- `src/contexts/conformance_spec/scenarios.rs` — real DAEMON-LIFECYCLE-006/007/008 implementations
- `tests/unit/automation_runtime_test.rs` — 5 new regression tests

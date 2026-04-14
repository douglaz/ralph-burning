# Make status reconciliation attempt-aware

## Problem

Status reconciliation in `reconcile_snapshot_status()` uses the journal's global last terminal event (`run_failed`/`run_completed`). But resumed attempts reuse the same `run_id`. A live resumed run still has an older `run_failed` for that `run_id`, so `handle_status` can incorrectly patch the current Running snapshot to Failed.

## Fix

In `src/contexts/project_run_record/queries.rs`, update `reconcile_snapshot_status()` and `terminal_status_for_running_attempt()` to be attempt-aware:

1. Find the most recent `run_started` or `run_resumed` event in the journal for the current run_id
2. Only consider terminal events (`run_failed`/`run_completed`) that occurred AFTER that boundary
3. If no terminal event exists after the boundary, the run is genuinely running — don't patch
4. Handle the edge case where `run_started` is missing but a terminal event exists

Search for `reconcile_snapshot_status`, `terminal_status_for_running_attempt`, `terminal_status_for_attempt` in the codebase to find all relevant code.

Also check `handle_status` in `src/cli/run.rs` and `handle_resume` for any additional reconciliation logic that needs the same fix.

## Acceptance Criteria
- Reconciliation ignores terminal events before the most recent run_started/run_resumed boundary
- A resumed Running run is not incorrectly patched to Failed due to stale terminal events
- Edge case: missing run_started with terminal event still reconciles correctly
- Regression test covering: resume after failure, status shows Running (not Failed)
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass

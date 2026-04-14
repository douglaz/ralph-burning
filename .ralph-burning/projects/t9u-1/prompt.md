# Make try_reconcile_success fail-closed when workflow_run_id is missing

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Problem

In `src/contexts/automation_runtime/daemon_loop.rs` (~line 1770-1825), `try_reconcile_success` falls back to the latest `RunStarted` journal event when `workflow_run_id` is missing on the task. Since `persist_workflow_run_id` is best-effort, a post-completion retry can legitimately reach this branch.

If someone manually reran the same project between the original failure and the retry, this code binds the daemon task to the newer run, then uses its `RunCompleted` timestamp and run ID for reconciliation. This can close/sync the bead and rewrite milestone lineage for the wrong attempt.

## Fix

When `workflow_run_id` is None and `persist_workflow_run_id` failed or was skipped:
1. Transition the milestone to needs-operator (or Failed) with a descriptive reason
2. Return an error that marks the task as failed (retryable after operator intervention)
3. Do NOT guess the latest RunStarted — this is not safe

Search for `try_reconcile_success`, `workflow_run_id`, `persist_workflow_run_id`, `RunStarted` in `daemon_loop.rs`.

## Acceptance Criteria
- Missing workflow_run_id does not silently bind to latest RunStarted
- Operator is signaled when run identity cannot be determined
- Task is marked failed so it can be retried after operator fixes the binding
- New test covers the missing-run-id path
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

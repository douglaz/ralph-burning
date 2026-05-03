# ralph-burning-ftx — fix ignored flaky tests

## Problem description

Seven tests in the suite are currently `#[ignore]`d due to flakiness or
missing test infrastructure. CI doesn't run them, so they shouldn't be
flagged as failed — but the tradeoff is that real regressions in the
covered code path go undetected. We've also seen a related-but-distinct
flake (`agent_execution_test::service_emits_invocation_completed_trace_with_token_fields`)
fail intermittently in CI on PR #194; structural improvements from this
work may help that case too.

## Specific tests to stabilize

### Flaky signal/process tests (4)
- `cli::run::tests::kill_tracked_descendant_processes_survives_parent_exit`
  — process spawn race, pid file never written before assertion
- `run_stop_sigkill_finalizes_snapshot_even_when_backend_cleanup_fails`
  — signal timing race
- `run_stop_reconciles_running_snapshot_after_sigterm_handoff_removes_pid`
  — SIGTERM handler doesn't complete before assertion
- panel dispatch test in `tests/unit/workflow_engine_test.rs` — needs
  update for supporting record cleanup; check the existing `#[ignore]`
  attributes to find the exact name

### Missing worktree fixture tests (3)
- `process_cycle_multi_repo_repairs_orphaned_lease_reference_after_metadata_failure`
- `process_cycle_multi_repo_reclaims_dead_daemon_pid_record_during_orphaned_repair`
- `process_cycle_multi_repo_quarantines_corrupt_terminal_lease_records`

These fail because the test fixtures don't actually create the git
worktree directories that `LeaseService::release()` expects to clean up.

## Implementation hints

For the **signal/process flakes**, the fix is structural — replace
sleep-based assumptions with explicit synchronization:
- Poll for the pid file with a bounded retry/backoff loop instead of a
  single fixed sleep.
- Use `tokio::time::timeout` with a generous deadline (5–10s) and
  `until <condition>` polling at small intervals (e.g. 50ms).
- Where the test asserts that a SIGTERM handler completed, wait on the
  child process exit explicitly rather than on a wall-clock delay.

For the **worktree fixture tests**, two options:
- Build real `git init` worktree directories inside `tempfile::tempdir()`
  in the test setup so `LeaseService::release()` can do the actual
  filesystem cleanup it expects.
- OR mock the worktree-removal hook so the lease release path doesn't
  need the directory to exist at all.

Prefer the real-fixtures approach if the cleanup logic is meaningful to
exercise; mock if the test is purely about the in-memory state machine.

For the **panel dispatch test**, look at how `supporting_record_cleanup`
changed since the test was ignored — the fix may just be updating the
expected ordering of cleanup calls.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged. Only review code under `src/` and `tests/`.

## Acceptance criteria

- All 7 listed `#[ignore]`d tests are un-ignored and pass reliably.
- Run `nix develop -c cargo test --features test-stub --locked` 3
  times back-to-back; no failures across all runs (this catches
  inter-test ordering flakes).
- No new `#[ignore]` attributes added.
- Existing `#[ignore]` attributes that are NOT in the seven listed
  above (e.g. on tests truly impossible to run in CI without a TTY)
  are left alone, but each remaining ignore must have a comment
  explaining why it can't run.
- `nix build` passes; `cargo test --features test-stub --locked && cargo clippy --locked -- -D warnings && cargo fmt --check` pass.

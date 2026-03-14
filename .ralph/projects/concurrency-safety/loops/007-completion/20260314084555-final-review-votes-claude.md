---
artifact: final-review-votes
loop: 7
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T08:45:55Z
---

# Vote Results

## Amendment: CLI-RECONCILE-TESTS-001

### Vote
REJECT

### Rationale
The planner's position is correct — all three claimed-missing tests already exist and have been independently verified:

1. `reconcile_stale_cli_lease_cleans_lease_and_writer_lock` at `automation_runtime_test.rs:3720` — injects a stale CLI lease + writer lock, calls `LeaseService::reconcile`, asserts `stale_lease_ids.len() == 1`, `released_lease_ids.len() == 1`, `cleanup_failures.is_empty()`. Direct match for requested test #1.

2. `reconcile_stale_cli_lease_missing_writer_lock_reports_cleanup_failure` at `automation_runtime_test.rs:3793` — creates stale CLI lease without writer lock, asserts `cleanup_failures.len() == 1` containing `"writer_lock_absent"` and `released_lease_ids.is_empty()`. Direct match for requested test #2.

3. `cli_daemon_reconcile_cleans_stale_cli_lease` at `cli.rs:4570` — end-to-end conformance test that injects stale CLI lease + lock, verifies `run start` is blocked, runs `daemon reconcile`, asserts counters, then verifies `run start` succeeds afterward. Direct match for requested test #3. Additionally, a unit-level equivalent exists at `automation_runtime_test.rs:4407`.

The amendment's factual premise is wrong. It appears the reviewer only looked at `cli_writer_lease.rs:399` and missed the extensive test suite in `automation_runtime_test.rs` (lines 3720–4454) and `cli.rs` (lines 4489–4706).

## Amendment: CLI-RECONCILE-TESTS-002

### Vote
REJECT

### Rationale
The test already exists at `cli.rs:4489` as `cli_run_start_close_failure_exits_nonzero`. Verified that it sets `RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE=1`, runs `run start`, asserts non-zero exit, checks stderr for `"writer_lock_absent"` or `"guard close failed"`, and confirms the CLI lease record remains durable after the close failure. This is exactly the test the amendment claims is missing. The injection seam is exercised and verified; there is nothing to add.

## Amendment: CONC-REV-001

### Vote
ACCEPT

### Rationale
Independently verified the ordering bug is real. In `lease_service.rs` `release()`, the sequence is:

1. Line 187–192: `store.remove_lease()` — deletes the lease file
2. Line 196–202: `store.release_writer_lock()` — releases the writer lock

If step 2 fails (I/O error or `OwnerMismatch`), the lease file is already gone. Since `reconcile()` discovers stale leases via `store.list_leases()` (line 276) and `store.list_lease_records()` (line 445), both of which scan for on-disk lease files, the orphaned writer lock becomes invisible to reconcile and blocks future writers indefinitely with no automated recovery.

The fix — releasing the writer lock before deleting the lease file — is low-risk and correct. If lock release fails, the lease file remains durable for reconcile to discover. If lock release succeeds but lease file deletion fails, the orphaned lease is benign (reconcile will see it as stale with no lock and report `writer_lock_absent`). The planner's analysis of this is sound.

## Amendment: CONC-REV-002

### Vote
ACCEPT

### Rationale
Independently verified that rollback cleanup failures are silently discarded at all three claimed locations:

- `cli_writer_lease.rs:150`: `let _ = store.release_writer_lock(...)` after lease record write failure
- `lease_service.rs:127`: `let _ = store.release_writer_lock(...)` after worktree creation failure
- `lease_service.rs:144`: `let _ = store.release_writer_lock(...)` after lease write failure (plus `let _ = worktree.remove_worktree(...)`)

In each case, the comment "Invariant: failed acquisition leaves neither lease record nor lock" is aspirational but unenforced. A double failure (write fails, then rollback release fails) leaves a bare `writer-*.lock` with no associated lease record. Since `reconcile()` iterates lease records/files to discover cleanup targets, the orphaned lock is invisible and blocks future writers indefinitely.

While a double filesystem failure is an edge case, the fix is minimal and the diagnostic value is high: capturing the rollback error and including it in the returned error converts a silent resource leak into an actionable operator message. The risk-reward ratio strongly favors acceptance.

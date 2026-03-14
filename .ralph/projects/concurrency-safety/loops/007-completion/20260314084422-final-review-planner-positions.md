---
artifact: final-review-planner-positions
loop: 7
project: concurrency-safety
backend: claude(opus)
role: planner
created_at: 2026-03-14T08:44:22Z
---

# Planner Positions

## Amendment: CLI-RECONCILE-TESTS-001

### Position
REJECT

### Rationale
All three claimed-missing tests already exist in the codebase:

1. **"Stale CLI writer lease with no task/worktree, asserting on ReconcileReport counters"** — exists at `tests/unit/automation_runtime_test.rs:3720` as `reconcile_stale_cli_lease_cleans_lease_and_writer_lock`. It injects a stale `CliWriterLease` record + writer lock, calls `LeaseService::reconcile` with `ttl_override_seconds: Some(0)`, and asserts `stale_lease_ids.len() == 1`, `released_lease_ids.len() == 1`, `failed_task_ids.is_empty()`, `cleanup_failures.is_empty()`. This is a direct match for the requested test.

2. **"Partial cleanup with missing writer-lock file"** — exists at `tests/unit/automation_runtime_test.rs:3793` as `reconcile_stale_cli_lease_missing_writer_lock_reports_cleanup_failure`. It creates a stale CLI lease WITHOUT a writer lock, calls reconcile, and asserts `released_lease_ids.is_empty()`, `cleanup_failures.len() == 1`, and that the failure details contain `"writer_lock_absent"`.

3. **"Conformance test injecting stale CLI lease + writer lock, running daemon reconcile, then verifying run start succeeds"** — exists at `tests/cli.rs:4570` as `cli_daemon_reconcile_cleans_stale_cli_lease`. It injects a stale CLI lease JSON + writer lock file, verifies `run start` is blocked, runs `daemon reconcile`, asserts `stale_leases=1`, `released_leases=1`, `failed_tasks=0`, then verifies `run start` succeeds afterward. There is also a unit-level equivalent at `tests/unit/automation_runtime_test.rs:4407` (`reconcile_stale_cli_cleanup_allows_subsequent_run_start`).

The amendment's factual premise — that the test at `cli_writer_lease.rs:399` is the only test — is incorrect. It apparently missed the extensive test suite added in `automation_runtime_test.rs` (lines 3720–4454) and `cli.rs` (lines 4570–4706).

## Amendment: CLI-RECONCILE-TESTS-002

### Position
REJECT

### Rationale
The test already exists at `tests/cli.rs:4490–4534` (unnamed in the grep output but visible starting at line 4490). It:
- Sets `RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE=1`
- Runs `run start` on a valid project
- Asserts non-zero exit status
- Asserts stderr contains `"writer_lock_absent"` or `"guard close failed"`
- Verifies the CLI lease record file remains durable after the close failure

This is precisely the test the amendment claims is missing. The injection seam is exercised and verified.

## Amendment: CONC-REV-001

### Position
ACCEPT

### Rationale
The ordering issue is real. In `lease_service.rs:187–202`, `LeaseService::release()` executes cleanup in the order: worktree removal → **lease file deletion** (line 188) → **writer lock release** (line 197). Both sub-step outcomes are captured individually, and `resources_released` is computed from all of them — but the lease file is already gone by the time writer-lock release is attempted.

If writer-lock release fails (I/O error or `OwnerMismatch`), the function correctly reports `resources_released: false`. The immediate caller sees the failure. However, the lease file has already been deleted, so **future reconcile passes cannot rediscover the orphaned writer lock**: `reconcile()` discovers worktree leases via `store.list_leases()` (line 276) and CLI leases via `store.list_lease_records()` (line 445) — both require a durable lease file.

The result is a stranded `writer-*.lock` file with no reconcile-visible lease record pointing at it, which blocks all future writer acquisitions for that project with no automated recovery path.

The fix is straightforward: release the writer lock before deleting the lease file. If lock release fails, the lease file stays durable for reconcile. If lock release succeeds but lease file deletion fails, the orphaned lease file is benign — reconcile will see it as stale with no lock and report `writer_lock_absent` as a cleanup failure (which is the existing, correct behavior at line 482–490).

## Amendment: CONC-REV-002

### Position
ACCEPT

### Rationale
The discarded rollback result is real and verified at two locations:

1. **CLI acquisition** (`cli_writer_lease.rs:148–151`): When the lease record write fails at line 148, the rollback at line 150 uses `let _ = store.release_writer_lock(...)`, silently discarding any release failure. If the rollback release also fails, the caller receives only the lease-write error while a bare `writer-*.lock` file persists with no CLI lease record.

2. **Worktree acquisition** (`lease_service.rs:124–145`): The same pattern appears at line 127 (worktree creation failure rollback) and line 144 (lease write failure rollback), both using `let _ = store.release_writer_lock(...)`.

The impact: a bare writer lock file with no corresponding lease record is invisible to `daemon reconcile` — Pass 1 (line 276) iterates worktree leases, Pass 2 (line 445) iterates CLI lease records. Neither discovers orphaned lock files directly. The lock blocks future writer acquisitions indefinitely with no automated recovery.

While the double-failure scenario (lock acquire succeeds → lease/worktree write fails → lock release fails) requires correlated filesystem failures, the fix is low-risk: capture the rollback release result and include it in the returned error so operators know manual lock cleanup may be needed. This converts a silent resource leak into an actionable diagnostic.

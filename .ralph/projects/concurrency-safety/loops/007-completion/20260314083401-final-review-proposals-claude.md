---
artifact: final-review-proposals
loop: 7
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T08:34:01Z
---

Now I have a complete picture. Let me compile the final review.

---

# Final Review: AMENDMENTS

## Amendment: CLI-RECONCILE-TESTS-001

### Problem
`[P1]` The master prompt explicitly requires four categories of tests for CLI writer-lease reconcile behavior. **Three of them are missing**:

1. **Missing:** "Add a reconcile test for a stale CLI writer lease with no task/worktree and assert: `stale_leases == 1`, `released_leases == 1`, `failed_tasks == 0`."
2. **Missing:** "Add a reconcile test for partial cleanup of a stale CLI writer lease, such as a missing writer-lock file, and assert it becomes a cleanup failure rather than a successful release."
3. **Missing:** "Add a CLI or conformance test that injects a stale CLI lease plus writer lock, runs `daemon reconcile`, and then verifies `run start` or `run resume` succeeds."

The existing test `lease_is_reconcile_visible_and_stale_detectable` (`cli_writer_lease.rs:399`) only proves visibility and staleness detection on the model — it never calls `LeaseService::reconcile` and never asserts on `ReconcileReport` counters. There are zero tests exercising the Pass 2 CLI-lease reconcile path in `lease_service.rs:440-546`.

### Proposed Change
Add three tests:

1. **Unit test in `lease_service.rs` or `cli_writer_lease.rs`**: Manually persist a stale `CliWriterLease` record + writer lock in a tempdir, call `LeaseService::reconcile` with `now` past the TTL, assert `stale_lease_ids.len() == 1`, `released_lease_ids.len() == 1`, `failed_task_ids.len() == 0`, `cleanup_failures.is_empty()`.

2. **Unit test for partial cleanup**: Same setup but omit or pre-delete the writer lock file. Assert reconcile reports a cleanup failure (`cleanup_failures.len() == 1`) and does **not** count the lease as released.

3. **Conformance test**: Inject a stale CLI lease (JSON with `lease_kind: "cli_writer"`) + writer lock file, run `daemon reconcile`, then run `run start` (or `run resume` against a paused project) and assert it succeeds (writer lock is acquirable).

### Affected Files
- `src/contexts/automation_runtime/cli_writer_lease.rs` or `src/contexts/automation_runtime/lease_service.rs` — add 2 unit tests
- `src/contexts/conformance_spec/scenarios.rs` — add 1 conformance scenario

---

## Amendment: CLI-RECONCILE-TESTS-002

### Problem
`[P2]` The `RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE` env-var injection seam in `src/cli/run.rs:193-199` has no corresponding test that exercises it. The seam exists in production code but is dead code from the test suite's perspective — no conformance scenario or unit test sets this variable and verifies the `close()` failure path.

### Proposed Change
Add a conformance test (or integration test) that:
- Sets `RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE=1`
- Runs `run start` on a valid project
- Asserts the command exits with a non-zero status and the error message includes `GuardCloseFailed` / `writer_lock_absent`

Without a test, this injection seam is unverified and risks silently breaking if `close()` error handling changes.

### Affected Files
- `src/contexts/conformance_spec/scenarios.rs` — add 1 conformance scenario

---

## Summary of non-amendment observations

The implementation is architecturally sound. Key strengths:

- **Writer-lock release** (`fs.rs:1239-1427`) is well-engineered with 4-phase TOCTOU protection (content check → atomic rename → inode verification → staging delete), and 6 dedicated unit tests exercise the race-condition windows via test hooks.
- **`CliWriterLeaseGuard`** (`cli_writer_lease.rs`) correctly sequences heartbeat shutdown: signal → closed flag → tick_lock wait → abort handle → cleanup. The `std::sync::Mutex` usage is safe because no `.await` is held across the lock.
- **Serde backward compatibility** for lease records (`model.rs:292-331`) uses `#[serde(untagged)]` with `TaggedLeaseRecord` (internally tagged) tried first, falling back to `LegacyWorktreeLease` with `deny_unknown_fields`.
- **`close_inner` idempotency** via `AtomicBool` ensures Drop after explicit `close()` is a no-op.
- **Reconcile Pass 2** (`lease_service.rs:440-546`) correctly validates `project_id` before side effects, releases the writer lock before deleting the lease record, and reports sub-step failures individually.
- **No `set_current_dir` in `daemon_loop.rs`** — verified both by grep and the existing `DAEMON-LIFECYCLE-008` structural + runtime conformance test.
- All 444 unit tests pass, all 218 conformance tests pass, build succeeds.

---
artifact: final-review-planner-positions
loop: 11
project: concurrency-safety
backend: claude(opus)
role: planner
created_at: 2026-03-14T12:07:14Z
---

# Planner Positions

## Amendment: CS-REV-001

### Position
ACCEPT

### Rationale
The described crash-safety gap is real and verified in the source code.

In `cli_writer_lease.rs:140`, `acquire()` calls `store.acquire_writer_lock()` first. The CLI lease record is not persisted until `cli_writer_lease.rs:152` via `store.write_lease_record()`. If the process crashes between these two lines, a `writer-<project>.lock` file exists on disk with content set to the lease_id, but no corresponding `CliWriterLease` record is written.

In `lease_service.rs:563-679` (reconcile pass 2), stale CLI leases are discovered exclusively by iterating `store.list_lease_records(base_dir)` at line 568 and filtering for `LeaseRecord::CliWriter` variants. There is no code path in reconcile that scans for orphaned writer lock files directly — only lease records are enumerated.

This means a crash in the gap between lines 140 and 152 produces an unrecoverable orphaned writer lock. No subsequent `daemon reconcile` invocation will ever discover it, and all future `run start`/`run resume` calls for that project will fail with `ProjectWriterLockHeld`. This directly undermines the stale-lock recovery design that the CLI writer lease system was built to provide.

The proposed fix (persist the lease record before acquiring the writer lock, with cleanup on failure) is sound and addresses the root cause.

## Amendment: CS-REV-002

### Position
ACCEPT

### Rationale
The u64-to-i64 cast overflow is verified in the source code, though the practical risk is very low.

At `daemon.rs:44`, `ttl_seconds` is declared as `Option<u64>` with no upper-bound validation. At `lease_service.rs:394`, the worktree stale check performs `chrono::Duration::seconds(ttl as i64)`, and at `lease_service.rs:577`, the identical cast appears for CLI lease stale checks. For any value above `i64::MAX` (~9.2e18), `ttl as i64` wraps to a negative number in Rust's `as` semantics. `chrono::Duration::seconds(negative)` produces a negative duration, so `last_heartbeat + negative_duration` yields a timestamp in the past, causing `now > past_time` to evaluate true — marking every lease as immediately stale regardless of actual age.

This would cause reconcile to incorrectly fail active daemon tasks and release healthy CLI writer locks, which is a data-loss scenario.

While the trigger requires an astronomically large TTL input (>292 billion years), this is still a correctness defect: the code accepts the value without validation and silently produces dangerous behavior. The fix is trivial — either clamp to `i64::MAX` or reject the value at CLI parse time — and the amendment correctly notes that `lease_heartbeat_deadline()` already handles the default TTL path safely.

---
artifact: final-review-votes
loop: 11
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T12:07:58Z
---

# Vote Results

## Amendment: CS-REV-001

### Vote
ACCEPT

### Rationale
I verified the crash-safety gap directly in the source. At `cli_writer_lease.rs:140`, `acquire()` calls `store.acquire_writer_lock()` first, and the CLI lease record is not persisted until `cli_writer_lease.rs:152` via `store.write_lease_record()`. If the process crashes between these two lines, a `writer-<project>.lock` file exists on disk (containing the lease_id), but no corresponding `LeaseRecord::CliWriter` entry is written.

The reconcile pass 2 at `lease_service.rs:568` discovers stale CLI leases exclusively via `store.list_lease_records(base_dir)` and filters for `LeaseRecord::CliWriter` variants. There is no code path in the reconciler that scans for orphaned writer lock files directly. This means a crash in the acquisition gap produces an permanently orphaned writer lock that no amount of `daemon reconcile` invocations can discover or release. All subsequent `run start`/`run resume` calls for that project will fail with `ProjectWriterLockHeld` indefinitely.

This is a serious correctness defect that directly undermines the system's core stale-lock recovery guarantee. The proposed fix — persisting the lease record before acquiring the writer lock, with cleanup on failure — is sound, well-scoped, and addresses the root cause. I agree with the planner's position.

## Amendment: CS-REV-002

### Vote
ACCEPT

### Rationale
I verified the u64-to-i64 cast at both sites. At `lease_service.rs:394`, the worktree stale check performs `chrono::Duration::seconds(ttl as i64)`, and at `lease_service.rs:577`, the identical unguarded cast appears for CLI lease stale checks. The `daemon.rs:44` CLI definition accepts `Option<u64>` with no upper-bound constraint. For values above `i64::MAX`, Rust's `as` cast wraps to a negative number, `chrono::Duration::seconds(negative)` produces a negative duration, and every lease becomes immediately stale — potentially failing active daemon tasks and releasing healthy CLI writer locks.

I agree with the planner that the practical trigger threshold is astronomically high (~292 billion years), making real-world exploitation essentially impossible absent a severely malformed script or automation. However, the fix is trivial — a single saturation clamp or early CLI rejection — and the silent production of dangerous behavior from a silently accepted input is a correctness defect regardless of likelihood. The `lease_heartbeat_deadline()` path already handles the default TTL safely, so aligning the override path is straightforward and consistent. Accepting as a low-cost correctness improvement.

---
artifact: final-review-votes
loop: 5
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T07:38:18Z
---

I’m checking the cited code paths in the lock recovery and CLI lease cleanup flow before voting. First step is to read the referenced lines and trace the surrounding control flow so the vote is grounded in the current source, not just the planner summary.
I’ve confirmed both cited sites exist as described. Next I’m checking the full `release_writer_lock` implementation and the lease cleanup semantics around `remove_lease` so the vote reflects the actual invariants the code is enforcing.
The lock-release function already has tests around replacement races and verification failures, so I’m reading those next. That should tell me whether the current behavior is a deliberate policy choice or an incomplete implementation.
The lock tests confirm the intended invariant is “fail closed without touching a replacement lock,” but they currently only cover the `staging missing` branch, not `staging exists and canonical was reacquired`. I’m now checking the lease-guard tests and call sites to see whether silent cleanup failure is an intentional CLI behavior or an inconsistency.
# Vote Results

## Amendment: CS-LKR-001

### Vote
ACCEPT

### Rationale
The planner’s position is correct. In the two verification-error branches, the recovery step uses `fs::rename(&staging, &path)`, which on Unix can replace an existing destination. Once the original lock has been moved to staging, another writer can legitimately reacquire the canonical lock path, and this recovery rename can then overwrite that new live lock.

That directly contradicts the owner-safe release design in the surrounding function. The inode-mismatch branch already uses a fail-closed restore pattern with `hard_link` plus `AlreadyExists` handling, so leaving these two error paths on `rename` is an unsafe inconsistency in the same race-sensitive flow.

## Amendment: CS-LKR-002

### Vote
ACCEPT

### Rationale
The planner’s position is correct here as well. `CliWriterLeaseGuard::drop()` ignores `remove_lease` errors after a successful lock release, and the normal `run start` and `run resume` paths rely entirely on that implicit drop cleanup. That means the CLI can report success while leaving behind a durable CLI lease record with no writer lock.

The downstream effect is real: reconcile treats `writer_lock_absent` as a cleanup failure and skips lease deletion, so the orphaned lease becomes a persistent manual-repair condition instead of self-healing. An explicit fallible shutdown path for the success case is the right fix, with `Drop` remaining best-effort cleanup for unwind and crash-style paths.

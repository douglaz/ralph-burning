---
artifact: final-review-proposals
loop: 11
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T12:06:27Z
---

Reviewing the diff against `9e97106ed885475187f23576f3cb3a7f9738f75b` first, then I’ll read the changed implementation files end-to-end and trace the CLI lease/reconcile paths against the concurrency requirements.
The main review surface is the new lease model, the CLI guard, and the lock-release path in the filesystem store. I’m pulling exact line maps now so I can trace the new cleanup and contention behavior without guessing.
I’ve confirmed the CLI now explicitly acquires and closes a lease-backed lock in both `run start` and `run resume`. Next I’m tracing the guard itself and the store contract, because that’s where stale recovery and failure semantics actually live.
The next risk surface is `FsDaemonStore::release_writer_lock`. That code now does the real mutual-exclusion cleanup, and it’s also where subtle races or cleanup-accounting regressions would hide.
I’ve traced the storage and reconcile paths. One remaining risk is whether the heartbeat can actually run during a long CLI execution, because if the runtime setup starves the background task the lease would be reclaimable even while the CLI is healthy.
I found one serious correctness gap: the CLI acquires the writer lock before it persists the CLI lease record. That leaves a crash window where the process can die holding an unrecoverable lock, which defeats the stale-lock recovery feature. I’m checking whether there’s a second regression in the shared release path or if that one is just a comment mismatch.
# Final Review: AMENDMENTS

## Amendment: CS-REV-001 [P1]

### Problem
`CliWriterLeaseGuard::acquire()` takes the project writer lock first at [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L140) and only persists the CLI lease record afterward at [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L152). `daemon reconcile` only discovers stale CLI holders by enumerating lease records at [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L568).

If the CLI crashes in that gap, the stale `writer-<project>.lock` has no matching lease record. Reconcile can never see or release it, so later `run start`/`run resume` calls stay blocked with `ProjectWriterLockHeld`. That breaks the core stale-lock recovery objective.

### Proposed Change
Make CLI lock acquisition crash-safe by ensuring a reconcile-visible lease exists before a writer lock can be stranded. The simplest fix is to persist the CLI lease record first, then acquire the writer lock and delete the prewritten lease on lock-acquire failure. A dedicated helper that atomically manages the pair would also satisfy the requirement.

### Affected Files
- [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L130) - reorder or combine lease persistence and writer-lock acquisition so every stranded CLI lock is recoverable.
- [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L568) - this is the reconcile path that currently exposes the gap because it only scans lease records.

## Amendment: CS-REV-002 [P2]

### Problem
`daemon reconcile --ttl-seconds` accepts an unrestricted `u64` at [src/cli/daemon.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/daemon.rs#L42), but both override-based stale checks cast that value directly to `i64` at [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L394) and [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L577).

For any value above `i64::MAX`, that cast wraps negative. The result is that fresh worktree leases and fresh CLI leases become immediately stale under reconcile, which can incorrectly fail active daemon tasks or release a healthy CLI writer lock.

### Proposed Change
Clamp `ttl_override_seconds` to `i64::MAX` before constructing `chrono::Duration`, or reject oversized values during CLI argument validation. Reusing the same saturation rule already used by `lease_heartbeat_deadline()` keeps override behavior aligned with the default TTL path.

### Affected Files
- [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L393) - saturate the override TTL in both stale checks.
- [src/cli/daemon.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/daemon.rs#L42) - optionally validate and reject unsupported TTL override values early.

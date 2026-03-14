---
artifact: final-review-proposals
loop: 7
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T08:42:21Z
---

Reviewing the changes against the concurrency-safety prompt first. I’ll inspect the diff to identify touched implementation and test files, then read the relevant files end-to-end before checking for correctness gaps and regressions.
The diff is concentrated in the filesystem lock release path, the new CLI writer lease guard, lease reconciliation, and a large set of runtime and CLI tests. I’m reading those implementation files end-to-end now, then I’ll trace the new close/reconcile paths against the prompt’s failure-mode requirements.
I’ve narrowed the review surface to the new lease model, the CLI guard lifecycle, owner-aware lock release, and reconcile accounting. Next I’m reading those files with line numbers so I can trace failure handling end-to-end and anchor any findings precisely.
The CLI path, guard shutdown, and reconcile output paths are clear. I’m now looking for edge cases the new ownership checks introduced, especially where older test helpers or manual fixtures might still serialize leases in the pre-change shape and now hit the new parser.
I’ve found one area that needs a deeper trace: the shared worktree `release()` path still deletes the lease file before attempting the owner-aware writer-lock release, which may now strand a lock without any reconcile-visible lease when that second sub-step fails. I’m verifying that against the helper stores and existing tests before deciding whether it rises to an amendment.
# Final Review: AMENDMENTS

## Amendment: CONC-REV-001 [P1]

### Problem
At [lease_service.rs:188](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L188) and [lease_service.rs:197](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L197), `LeaseService::release()` deletes the worktree lease file before it attempts the owner-aware writer-lock release. Reconcile only rediscovers stale worktree cleanup from lease files starting at [lease_service.rs:276](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L276), so any later `OwnerMismatch` or I/O failure leaves a still-held project writer lock with no reconcile-visible lease to clean. That strands the lock and can block future writers indefinitely.

### Proposed Change
Keep the worktree lease durable until the writer-lock release has positively succeeded. The simplest fix is to move `remove_lease()` after successful `release_writer_lock()`, or to recreate the lease file if lock release fails. Add a regression test that forces a writer-lock failure during `LeaseService::release()` and asserts the lease file is still present afterward.

### Affected Files
- [lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - reorder or compensate the lease-file and writer-lock cleanup sequence.
- [automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add coverage for partial release where the writer lock remains held.

## Amendment: CONC-REV-002 [P2]

### Problem
The new CLI acquisition rollback still swallows unreleased-lock failures. After the guard acquires the writer lock at [cli_writer_lease.rs:136](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L136), a lease-write failure at [cli_writer_lease.rs:148](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L148) triggers best-effort cleanup, but the release result is discarded at [cli_writer_lease.rs:150](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L150). If that rollback release fails, the command returns only the write error while leaving a bare `writer-*.lock` with no CLI lease record, which `daemon reconcile` cannot heal. The same silent pattern remains in worktree acquisition at [lease_service.rs:127](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L127) and [lease_service.rs:144](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L144).

### Proposed Change
Do not discard rollback cleanup outcomes. Capture `release_writer_lock()` failures and return a combined acquisition error that explicitly says the writer lock may still be held. Add targeted tests for lease-persist failure followed by release failure so this path cannot silently regress again.

### Affected Files
- [cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - propagate rollback cleanup failures from CLI acquisition.
- [lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - do the same for worktree acquisition rollback.
- [automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add regression coverage for rollback-release failure paths.

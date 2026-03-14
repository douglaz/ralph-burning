---
artifact: final-review-votes
loop: 7
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T08:48:17Z
---

I’m verifying the planner’s claims against the code and tests before voting. I’ll inspect the referenced implementation and existing coverage for each amendment, then return votes with rationale only.
The coverage claims for the two CLI reconcile amendments check out so far; the named tests are present and match the requested scenarios. I’m checking the release and rollback paths now to see whether the planner’s two ACCEPT positions are technically sound.
The concurrency issues are real in the current source: worktree release still deletes the lease before lock release, and both acquisition rollback paths still discard `release_writer_lock()` failures. I’m checking whether either has already been covered by regression tests before I finalize the votes.
I hit a path mismatch while checking the store implementation; the relevant code is in `src/adapters/fs.rs`, not under `contexts/automation_runtime`. I’m confirming reconcile discovery is file-backed, which matters for whether these stranded-lock cases are recoverable.
I’m checking one edge case before locking the votes: whether a retained `task.lease_id` gives any other recovery path for a deleted worktree lease file. If daemon-loop can already heal that case reliably, it would weaken the `CONC-REV-001` rationale.
# Vote Results

## Amendment: CLI-RECONCILE-TESTS-001

### Vote
REJECT

### Rationale
The claimed gap is already covered. The test suite already includes a stale CLI reconcile unit test that injects a stale `CliWriterLease`, calls `LeaseService::reconcile`, and asserts the expected stale/released/failed/cleanup counters; a second unit test that covers the missing-writer-lock partial-cleanup case and asserts cleanup failure instead of successful release; and a CLI-level test that runs `daemon reconcile` and then verifies `run start` succeeds afterward. This amendment would add duplicate coverage rather than close a real hole.

## Amendment: CLI-RECONCILE-TESTS-002

### Vote
REJECT

### Rationale
The env-var seam is already exercised. There is an existing CLI test that sets `RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE=1`, runs `run start`, asserts a non-zero exit, checks for `writer_lock_absent` or the guard-close failure wording, and verifies the CLI lease remains durable after the failed close. The proposal’s premise is therefore incorrect.

## Amendment: CONC-REV-001

### Vote
ACCEPT

### Rationale
The bug is real. In the worktree release path, the lease file is removed before `release_writer_lock()` runs. If writer-lock release then fails or returns owner mismatch, the lock can remain held while the durable lease record is already gone. Future automated cleanup does not discover orphaned writer locks directly; it discovers worktree cleanup work from lease records. Keeping the lease durable until lock release succeeds is the correct fix, and a regression test should assert the lease file still exists after a forced writer-lock release failure.

## Amendment: CONC-REV-002

### Vote
ACCEPT

### Rationale
The rollback failure is currently swallowed in all cited acquisition paths via `let _ = store.release_writer_lock(...)`. That means a lease-persist or worktree-setup failure can still leave behind a bare writer lock if the rollback release also fails, while the caller only sees the original acquisition error. Because reconcile scans lease records, not raw lock files, that orphaned lock is not automatically recoverable. Propagating a combined error that warns the lock may still be held, plus regression coverage for the double-failure case, is warranted.

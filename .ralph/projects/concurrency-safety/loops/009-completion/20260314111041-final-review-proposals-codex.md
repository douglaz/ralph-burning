---
artifact: final-review-proposals
loop: 9
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T11:10:41Z
---

Reviewing the diff against `9e97106...` first, then I’ll read the changed implementation files end-to-end and trace the new CLI lease/reconcile flow for correctness and failure handling.
I have the diff and changed file set. Next I’m tracing the lease model, CLI guard, store, and reconcile paths together so any review findings are tied to actual end-to-end behavior rather than isolated hunks.
Plan:
1. Read the changed runtime/storage/CLI files end-to-end and reconstruct the acquisition, heartbeat, close, and reconcile state machine.
2. Check the failure paths against the spec: stale detection, strict cleanup accounting, rollback behavior, and writer-lock contention.
3. Spot-check the new tests and any harness changes for gaps or regressions, then write only concrete amendments with exact file/line references if warranted.
I’ve finished the main CLI/runtime files. Now I’m drilling into the filesystem store and the test harness changes, because the remaining risk is in lock-file semantics and whether the new tests actually cover the failure modes they claim to.
I found one likely lifecycle bug in the close path and I’m checking whether there’s a second TOCTOU issue around `run start`/`run resume` precondition reads happening before lock acquisition. If the engine revalidates under lock, that one is fine; if not, it’s a correctness hole.
# Final Review: AMENDMENTS

## Amendment: RB-REVIEW-20260314-01 [P1]

### Problem
A CLI run that fails during lease-file deletion can leave behind a stale CLI lease record that `daemon reconcile` can never clean up. In [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L87) and [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L89), `close()` releases the writer lock first and then returns `GuardCloseFailed` if `remove_lease()` fails. Later, in [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L604) and [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L637), reconcile treats `writer_lock_absent` as a cleanup failure and `continue`s before attempting lease-file deletion.

That means a transient close-time delete failure produces a permanently orphaned stale lease: every future reconcile reports the same failure, never removes the stale record, and old stale CLI leases can keep poisoning reconcile output after later runs on the same project.

### Proposed Change
Keep the strict accounting, but make the stale record recoverable. The smallest fix is in reconcile: when a stale CLI lease hits `WriterLockReleaseOutcome::AlreadyAbsent`, record the cleanup failure but still attempt to delete the stale CLI lease record, and do not increment `released_leases`. Alternatively, make `close()` fail closed by restoring the writer lock if lease deletion fails. In either case, add a regression test for “close leaves stale CLI lease, later reconcile prunes it without counting it as released.”

### Affected Files
- [src/contexts/automation_runtime/lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - make stale CLI leases recoverable when the writer lock is already gone.
- [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - align close-path behavior with the recovery model.
- [tests/unit/automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add regression coverage for close-time lease-delete failure followed by reconcile.

## Amendment: RB-REVIEW-20260314-02 [P3]

### Problem
The new full-suite conformance test executes a copied binary from `tempdir()` in [tests/cli.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/cli.rs#L4766) and [tests/cli.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/cli.rs#L4772). On hardened Linux runners where the system temp mount is `noexec`, that binary cannot be executed, so `cargo test` fails even though the product is fine.

### Proposed Change
Create the stable binary in an exec-capable location instead of the OS temp dir, such as a temp directory under `std::env::current_exe()?.parent()` or another workspace/target-local directory.

### Affected Files
- [tests/cli.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/cli.rs) - move the “stable binary” fixture out of `tempdir()` so the test does not depend on `/tmp` being executable.

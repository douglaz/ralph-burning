# Final Review Amendments Applied

## Round 1

### Amendment: CSR-20260314-01

### Problem
The new CLI path writes the owning `lease_id` into the writer-lock file at [ralph-burning-rewrite/src/adapters/fs.rs:1228](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs), but release still blindly unlinks by project path at [ralph-burning-rewrite/src/adapters/fs.rs:1245](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs). Both stale-CLI reconcile at [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:454](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) and [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:466](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs), and normal guard drop at [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:165](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs), rely on that blind removal. If the lock file is replaced between acquisition and cleanup, stale cleanup can delete a live writer’s lock and reopen the project to concurrent mutation.

### Proposed Change
Make writer-lock release owner-aware: read the current lock-file contents and only remove it when it matches the expected `lease_id`. Treat mismatches as cleanup failures, and keep the CLI lease durable so reconcile does not tear down another process’s lock.

### Affected Files
- [ralph-burning-rewrite/src/adapters/fs.rs:1228](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs) - the owner token is written here and needs a matching checked-release path.
- [ralph-burning-rewrite/src/contexts/automation_runtime/mod.rs:72](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/mod.rs) - the store trait needs an owner-aware writer-lock release/read API.
- [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:454](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - stale CLI reconcile should release the lock against the expected `lease_id`, not just the project id.
- [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:165](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - RAII cleanup should use the same owner-aware release path.
- [ralph-burning-rewrite/tests/unit/automation_runtime_test.rs:3705](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add an owner-mismatch regression test.

### Reviewer
codex

### Amendment: CSR-20260314-02

### Problem
Both CLI cleanup paths delete the durable lease record before they prove the writer lock can be released: guard drop removes the lease at [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:162](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) before releasing the lock at [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:165](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs), and stale CLI reconcile removes the lease at [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:446](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) before validating `project_id` at [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:454](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) and releasing the lock at [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:466](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs). Any later failure strands `writer-<project>.lock` with no reconcile-visible CLI lease, so the new self-healing path cannot recover it on the next `daemon reconcile`.

### Proposed Change
Validate the project id first, then release the writer lock, and only delete the CLI lease record after lock removal succeeds. Add a regression test that injects a writer-lock release failure and verifies the CLI lease file remains on disk for a later reconcile pass.

### Affected Files
- [ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs:162](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - reverse the cleanup order so drop does not orphan an unrecoverable lock.
- [ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs:446](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - stale CLI cleanup should keep the lease durable until lock release succeeds.
- [ralph-burning-rewrite/tests/unit/automation_runtime_test.rs:3778](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - extend partial-cleanup coverage to assert the lease survives a lock-release failure.

Targeted checks run under `nix develop`: `cli_daemon_reconcile_cleans_stale_cli_lease` and `cli_lease_guard_creates_reconcile_visible_lease_record`. I did not run the full build/test/conformance suite.

### Reviewer
codex


## Round 2

### Amendment: CS-LKR-001

### Problem
In [src/adapters/fs.rs:1306](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L1306) and [src/adapters/fs.rs:1327](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L1327), the verification-failure recovery path restores the writer lock with `fs::rename(&staging, &path)`. On Unix, `rename` replaces an existing destination, so if another writer acquires `path` after the original lock was moved to staging, this recovery step can overwrite that new live lock. That breaks the owner-safety guarantee the new lock-release flow is trying to enforce.

### Proposed Change
Make the recovery path fail closed the same way the inode-mismatch branch already does: never overwrite `path` if it already exists. Use a safe restore strategy such as `hard_link`/`AlreadyExists` handling, or leave the staged lock durable and return an error when canonical `path` has been reacquired.

### Affected Files
- [src/adapters/fs.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs) - replace the `rename`-back recovery in the verification-error branches with a restore path that cannot clobber a newly acquired canonical lock.

### Reviewer
codex

### Amendment: CS-LKR-002

### Problem
`CliWriterLeaseGuard::drop()` silently ignores lease-file deletion failure after a successful lock release at [src/contexts/automation_runtime/cli_writer_lease.rs:173](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L173). Both normal CLI paths only rely on implicit drop cleanup after acquiring the guard at [src/cli/run.rs:156](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs#L156) and [src/cli/run.rs:240](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs#L240). If `remove_lease` hits an I/O error on a successful `run start`/`run resume`, the command still exits successfully, but the stale CLI lease is left behind with no writer lock. Later reconcile will hit the strict `writer_lock_absent` failure path at [src/contexts/automation_runtime/lease_service.rs:482](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L482), so the leak becomes a persistent manual-repair state.

### Proposed Change
Add an explicit fallible shutdown path for normal command completion, such as `CliWriterLeaseGuard::close() -> AppResult<()>`, and call it from `run start`/`run resume` before returning success. Keep `Drop` as best-effort unwind cleanup only. Normal successful CLI runs should not hide lease teardown failures.

### Affected Files
- [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - add a fallible explicit cleanup path and avoid silently swallowing lease deletion failure on the success path.
- [src/cli/run.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs) - use the explicit guard shutdown on the normal `start` and `resume` exit paths so cleanup failures surface to the caller.

### Reviewer
codex


## Round 3

### Amendment: CONC-REV-001

### Problem
At [lease_service.rs:188](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L188) and [lease_service.rs:197](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L197), `LeaseService::release()` deletes the worktree lease file before it attempts the owner-aware writer-lock release. Reconcile only rediscovers stale worktree cleanup from lease files starting at [lease_service.rs:276](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L276), so any later `OwnerMismatch` or I/O failure leaves a still-held project writer lock with no reconcile-visible lease to clean. That strands the lock and can block future writers indefinitely.

### Proposed Change
Keep the worktree lease durable until the writer-lock release has positively succeeded. The simplest fix is to move `remove_lease()` after successful `release_writer_lock()`, or to recreate the lease file if lock release fails. Add a regression test that forces a writer-lock failure during `LeaseService::release()` and asserts the lease file is still present afterward.

### Affected Files
- [lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - reorder or compensate the lease-file and writer-lock cleanup sequence.
- [automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add coverage for partial release where the writer lock remains held.

### Reviewer
codex

### Amendment: CONC-REV-002

### Problem
The new CLI acquisition rollback still swallows unreleased-lock failures. After the guard acquires the writer lock at [cli_writer_lease.rs:136](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L136), a lease-write failure at [cli_writer_lease.rs:148](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L148) triggers best-effort cleanup, but the release result is discarded at [cli_writer_lease.rs:150](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L150). If that rollback release fails, the command returns only the write error while leaving a bare `writer-*.lock` with no CLI lease record, which `daemon reconcile` cannot heal. The same silent pattern remains in worktree acquisition at [lease_service.rs:127](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L127) and [lease_service.rs:144](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L144).

### Proposed Change
Do not discard rollback cleanup outcomes. Capture `release_writer_lock()` failures and return a combined acquisition error that explicitly says the writer lock may still be held. Add targeted tests for lease-persist failure followed by release failure so this path cannot silently regress again.

### Affected Files
- [cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - propagate rollback cleanup failures from CLI acquisition.
- [lease_service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs) - do the same for worktree acquisition rollback.
- [automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add regression coverage for rollback-release failure paths.

### Reviewer
codex


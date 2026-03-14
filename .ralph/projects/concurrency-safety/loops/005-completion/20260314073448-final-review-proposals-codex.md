---
artifact: final-review-proposals
loop: 5
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T07:34:48Z
---

Reviewing the diff against `9e97106ed885475187f23576f3cb3a7f9738f75b` first, then I’ll read the changed implementation files end-to-end and trace the new CLI lease/reconcile paths for correctness and failure handling.
I have the changed file set. Next I’m reading the diffs and the full implementations around the new `cli_writer_lease` path, plus the reconcile and CLI entrypoints that now depend on it.
The main surface area is `FsDaemonStore` and the reconcile path. I’m pulling full file contents with line numbers now so I can trace interactions instead of reviewing the diff in isolation.
I’ve read the core runtime files. Next I’m checking the new tests and the conformance path to see whether they actually exercise the intended crash-recovery behavior or are only covering the happy path.
The code path looks internally consistent so far, but there are a couple of places where failure handling may be weaker than the spec. I’m running the requested verification next to see whether behavior matches the tests and whether any latent regressions show up under the full suite.
`cargo test` is still running through the long integration/conformance portion. I’m letting it complete before I lock findings, because some of the edge cases here only show up once the nested CLI/conformance flow runs.
The targeted build and test suite are green. I’m finishing the standalone `conformance run` now, then I’ll decide whether the remaining concerns are real defects or just defensive nits.
# Final Review: AMENDMENTS

## Amendment: CS-LKR-001 [P1]

### Problem
In [src/adapters/fs.rs:1306](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L1306) and [src/adapters/fs.rs:1327](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L1327), the verification-failure recovery path restores the writer lock with `fs::rename(&staging, &path)`. On Unix, `rename` replaces an existing destination, so if another writer acquires `path` after the original lock was moved to staging, this recovery step can overwrite that new live lock. That breaks the owner-safety guarantee the new lock-release flow is trying to enforce.

### Proposed Change
Make the recovery path fail closed the same way the inode-mismatch branch already does: never overwrite `path` if it already exists. Use a safe restore strategy such as `hard_link`/`AlreadyExists` handling, or leave the staged lock durable and return an error when canonical `path` has been reacquired.

### Affected Files
- [src/adapters/fs.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs) - replace the `rename`-back recovery in the verification-error branches with a restore path that cannot clobber a newly acquired canonical lock.

## Amendment: CS-LKR-002 [P2]

### Problem
`CliWriterLeaseGuard::drop()` silently ignores lease-file deletion failure after a successful lock release at [src/contexts/automation_runtime/cli_writer_lease.rs:173](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L173). Both normal CLI paths only rely on implicit drop cleanup after acquiring the guard at [src/cli/run.rs:156](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs#L156) and [src/cli/run.rs:240](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs#L240). If `remove_lease` hits an I/O error on a successful `run start`/`run resume`, the command still exits successfully, but the stale CLI lease is left behind with no writer lock. Later reconcile will hit the strict `writer_lock_absent` failure path at [src/contexts/automation_runtime/lease_service.rs:482](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/lease_service.rs#L482), so the leak becomes a persistent manual-repair state.

### Proposed Change
Add an explicit fallible shutdown path for normal command completion, such as `CliWriterLeaseGuard::close() -> AppResult<()>`, and call it from `run start`/`run resume` before returning success. Keep `Drop` as best-effort unwind cleanup only. Normal successful CLI runs should not hide lease teardown failures.

### Affected Files
- [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - add a fallible explicit cleanup path and avoid silently swallowing lease deletion failure on the success path.
- [src/cli/run.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/run.rs) - use the explicit guard shutdown on the normal `start` and `resume` exit paths so cleanup failures surface to the caller.

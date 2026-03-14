---
artifact: final-review-proposals
loop: 13
project: concurrency-safety
backend: codex
role: final_reviewer
created_at: 2026-03-14T13:40:30Z
---

Reviewing the diff against `9e97106ed885475187f23576f3cb3a7f9738f75b` first, then I’ll read the changed implementation files end-to-end and trace the lock/lease paths through reconcile.
I have the changed file set. Next I’m reading the runtime and CLI implementation files with line numbers, then I’ll cross-check the filesystem lock release path and the new tests against the acceptance criteria.
I’m in the service layer now. After this pass I’ll trace the close/reconcile cleanup semantics against the new writer-lock release implementation, because that’s where most correctness risk sits.
I’ve finished the first code pass. I’m checking a few edge cases now against the actual runtime behavior, especially where the guard and reconcile paths interpret `AlreadyAbsent`/`OwnerMismatch` differently.
I found one concrete lifecycle bug in the explicit close path. I’m tightening the trace now so I can cite the exact lines where failed close attempts are still finalized and drop stops retrying.
# Final Review: AMENDMENTS

## Amendment: CS-REV-001 [P2]

### Problem
[`close()` in `cli_writer_lease.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L57) says `Drop` should become a no-op only after a successful close, but [`close_inner()` marks the guard closed before any cleanup runs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L73). It then treats lease deletion as failed only on `Err(_)` at [`cli_writer_lease.rs:91`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L91), even though [`ResourceCleanupOutcome`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/mod.rs#L44) explicitly distinguishes `Removed` from `AlreadyAbsent`. Because [`Drop` just calls `close_inner()` again](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs#L256), a failed explicit `close()` is still finalized: there is no best-effort retry on drop, and `close()` can incorrectly report success when `remove_lease()` returned `AlreadyAbsent`.

### Proposed Change
Separate “heartbeat stopped” from “fully closed”, and only mark the guard closed after `release_writer_lock == Released` and `remove_lease == Removed`. Match `ResourceCleanupOutcome` explicitly so `AlreadyAbsent` becomes a `GuardCloseFailed` variant such as `lease_file_absent`. Keep `Drop` able to make one last best-effort cleanup attempt after a failed explicit `close()`.

### Affected Files
- [src/contexts/automation_runtime/cli_writer_lease.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/cli_writer_lease.rs) - fix the close state machine and handle `AlreadyAbsent` explicitly
- [tests/unit/automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add regression coverage for `remove_lease => AlreadyAbsent` and failed-close/drop-retry behavior

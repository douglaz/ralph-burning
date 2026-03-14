---
artifact: final-review-votes
loop: 13
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T13:42:49Z
---

# Vote Results

## Amendment: CS-REV-001

### Vote
ACCEPT

### Rationale
I verified all three claims directly against the source code and they hold:

1. **`AlreadyAbsent` silently treated as success** (line 91): `if let Err(e) = self.store.remove_lease(...)` only catches `Err`. An `Ok(ResourceCleanupOutcome::AlreadyAbsent)` return falls through to the `Ok(())` at line 99, reporting a clean close. This is demonstrably inconsistent with `acquire()` (lines 169-179), which treats the identical `AlreadyAbsent` outcome from the same `remove_lease` method as a `AcquisitionRollbackFailed` error. The `ResourceCleanupOutcome` enum exists precisely to make this distinction (mod.rs lines 40-49), so ignoring it on the close path defeats the design intent.

2. **`closed` flag set before cleanup** (line 73): `self.closed.store(true, Ordering::Release)` runs at step 2, before the writer-lock release (line 82) and lease removal (line 91). The flag legitimately serves to quiesce the heartbeat task (documented at lines 35-36, checked at lines 211 and 217), but it is also the idempotency guard at lines 66-68. This dual-purpose conflation is the root cause of claim 3.

3. **`Drop` cannot retry after failed `close()`**: `close(mut self)` consumes `self`, so `Drop::drop()` runs after `close()` returns. But `closed` was set to `true` at line 73 before any resource cleanup. `Drop::drop()` → `close_inner()` hits the early-return at line 66-68 and does nothing. This means a transient I/O failure during explicit `close()` permanently orphans the writer lock and/or lease file until the daemon reconcile loop detects staleness — a window that could be up to `CLI_LEASE_TTL_SECONDS` (300s, line 21).

The proposed fix is well-scoped: introduce a separate "heartbeat quiesced" flag distinct from "fully closed", match `ResourceCleanupOutcome` explicitly on the close path (consistent with `acquire()`), and allow `Drop` to make one best-effort retry. This does not over-engineer — it aligns the close path with the cleanup accounting model already established by the `ResourceCleanupOutcome` and `WriterLockReleaseOutcome` enums. The test additions for `AlreadyAbsent` on close and failed-close-then-drop are necessary given these are currently untested paths with real correctness implications.

---
artifact: final-review-planner-positions
loop: 13
project: concurrency-safety
backend: claude(opus)
role: planner
created_at: 2026-03-14T13:41:58Z
---

Good, the test file exists. Now I have all the information I need. Let me verify the key claims against the source:

**Claim 1: `close_inner()` marks `closed = true` before cleanup runs.**
Line 73: `self.closed.store(true, Ordering::Release);` — this happens at step 2, before the writer-lock release at line 82 and lease removal at line 91. **Confirmed.**

**Claim 2: `remove_lease` returning `AlreadyAbsent` is silently treated as success.**
Line 91: `if let Err(e) = self.store.remove_lease(...)` — `remove_lease` returns `AppResult<ResourceCleanupOutcome>` (line 76 of mod.rs). If it returns `Ok(AlreadyAbsent)`, the `if let Err` doesn't match, and line 99 returns `Ok(())`. Compare with `acquire()` at lines 169-179, which explicitly treats `AlreadyAbsent` as a failure via `AcquisitionRollbackFailed`. **Confirmed — inconsistent handling.**

**Claim 3: After a failed `close()`, `Drop` cannot retry.**
`close(mut self)` at line 58 consumes `self`. Inside `close_inner()`, `closed` is set to `true` at line 73 before any cleanup. If cleanup fails (lines 82-122), `close_inner` returns `Err(...)`. Then `close()` returns `Err(...)`, and `self` is dropped. `Drop::drop()` calls `close_inner()` again (line 260), but line 66-68 sees `closed == true` and returns `Ok(())` immediately — no retry. **Confirmed.**

# Planner Positions

## Amendment: CS-REV-001

### Position
ACCEPT

### Rationale
All three claims in this amendment are verified against the source code:

1. **`AlreadyAbsent` silently treated as success during close** (`cli_writer_lease.rs:91`): The `remove_lease` call at line 91 uses `if let Err(e) = ...`, meaning `Ok(ResourceCleanupOutcome::AlreadyAbsent)` falls through to the `Ok(())` at line 99. The guard reports a clean close even though the lease file was already gone — an anomalous state that the codebase treats as a failure elsewhere. The `acquire()` method (lines 169-179 in the same file) explicitly handles `AlreadyAbsent` from `remove_lease` as a `AcquisitionRollbackFailed` error, confirming that the codebase considers this condition noteworthy, not silently acceptable.

2. **`closed` flag conflates heartbeat shutdown with full closure** (`cli_writer_lease.rs:73`): The `closed` flag is set to `true` at line 73, **before** the writer-lock release (line 82) and lease deletion (line 91). This flag serves dual purpose — heartbeat quiescence (legitimate, documented at lines 35-36) and idempotency guard (line 66-68). The conflation means that once the heartbeat is stopped, no further cleanup attempt is possible through `close_inner`, even if the resource cleanup steps fail.

3. **`Drop` cannot retry after failed `close()`** (`cli_writer_lease.rs:256-261`): Because `close(mut self)` consumes `self`, Rust's drop semantics guarantee `Drop::drop()` runs after `close()` returns. But since `closed` was already set to `true` at line 73 (before cleanup), `Drop::drop()` → `close_inner()` hits the early-return at line 66-68 and does nothing. A failed explicit close permanently loses the opportunity for best-effort cleanup — the writer lock and/or lease file may be left dangling until the daemon reconcile loop discovers them as stale.

The proposed fix — separating "heartbeat stopped" from "fully closed" and matching `ResourceCleanupOutcome` explicitly — directly addresses all three issues without over-engineering. Adding test coverage for the `AlreadyAbsent` path and the failed-close-then-drop sequence is appropriate given these are currently untested edge cases with real correctness implications.

---
artifact: final-review-proposals
loop: 11
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T11:58:59Z
---

The implementation is correct. The `std::sync::Mutex` is appropriate here because:
1. It's used in both sync (`Drop`) and async (heartbeat) contexts
2. The critical section (heartbeat tick) is brief I/O only
3. Poison recovery via `unwrap_or_else(|e| e.into_inner())` ensures cleanup proceeds even after panic

---

# Final Review: NO AMENDMENTS

## Summary

The implementation correctly satisfies all requirements from the master prompt:

**CLI Writer-Lock Recovery (#3):**
- `CliWriterLeaseGuard` wraps the project writer lock with a durable `CliWriterLease` record and a 300s TTL / 30s heartbeat. The guard is used by both `run start` and `run resume` in `src/cli/run.rs`.
- The guard's `close()` and `Drop` implementations follow proper RAII cleanup order: stop heartbeat (via `closed` flag + `tick_lock` + `Notify`), owner-aware writer-lock release, then lease-record deletion. Lease records are preserved when lock release fails, keeping them visible for reconcile.
- `LeaseService::reconcile()` has a dedicated Pass 2 that scans CLI writer leases, releases locks, deletes lease records, and enforces strict cleanup accounting (stale CLI leases do not increment `failed_tasks`).

**Daemon CWD Safety (#6):**
- `daemon_loop.rs` contains no `set_current_dir` call (verified by grep and DAEMON-LIFECYCLE-008 conformance scenario).

**Design Integrity:**
- CLI leases use `CliWriterLease` (distinct from `WorktreeLease`) with a `LeaseRecord::CliWriter` enum variant — no fake task IDs or worktree paths.
- `LeaseService::acquire()` is not called from CLI paths; `CliWriterLeaseGuard::acquire()` is a separate path.
- Backward compatibility for existing worktree lease files is handled via `LeaseRecordWire` (`#[serde(untagged)]`) with a `LegacyWorktreeLease` fallback using `deny_unknown_fields`.
- `list_leases()` correctly filters out `CliWriter` records so existing worktree-lease consumers are unaffected.

**Test Coverage:**
- 6 unit tests for CLI lease reconcile (happy path, missing lock, missing lease file, non-stale, post-close prune, owner mismatch, re-acquisition).
- 5 unit tests for the guard itself (acquire, heartbeat, drop cleanup, contention, stale detection).
- 2 CLI integration tests (reconcile clean + reconcile failure with missing lock).
- Conformance scenarios DAEMON-LIFECYCLE-007 and DAEMON-LIFECYCLE-008 pass.

**Verification:** `cargo build` succeeds, 452 unit tests pass, 218 conformance scenarios pass.

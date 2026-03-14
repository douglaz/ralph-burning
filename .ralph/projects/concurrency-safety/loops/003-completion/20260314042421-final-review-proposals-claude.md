---
artifact: final-review-proposals
loop: 3
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T04:24:21Z
---

I've completed a thorough review. The implementation is correct, safe, and robust. Here is my assessment:

---

# Final Review: NO AMENDMENTS

## Summary

The implementation correctly delivers CLI writer-lock lease recovery with a clean, well-synchronized design across all changed files:

**Model layer** (`model.rs`): `CliWriterLease` and `LeaseRecord` enum with `#[serde(tag = "lease_kind")]` tagged serialization. Backward compatibility for pre-existing worktree leases is handled via `LeaseRecordWire` with `#[serde(untagged)]` + `LegacyWorktreeLease` with `#[serde(deny_unknown_fields)]`, which correctly prevents CLI writer records from being misinterpreted as legacy worktree leases.

**Guard** (`cli_writer_lease.rs`): The `CliWriterLeaseGuard` RAII guard acquires the writer lock, persists a lease record, and spawns a heartbeat task (300s TTL, 30s cadence). The drop synchronization protocol — `Notify` + `AtomicBool` (with correct Release/Acquire ordering) + `std::sync::Mutex` double-check pattern + abort — prevents any race between heartbeat file I/O and cleanup. The std::sync::Mutex (not tokio) is intentionally correct since `Drop::drop` is synchronous and needs to acquire it. Cleanup releases both the lease record and writer lock independently, using `let _ =` to ensure one failure doesn't skip the other.

**Reconcile** (`lease_service.rs`): Pass 2 scans all lease records for stale CLI writer leases. Cleanup is lease-record removal + writer-lock release, with strict accounting: `AlreadyAbsent` on either sub-step is a cleanup failure. No task marking, no worktree removal. `stale_leases` and `released_leases` are correctly incremented; `failed_tasks` is untouched.

**CLI integration** (`run.rs`): Both `handle_start` and `handle_resume` use `CliWriterLeaseGuard::acquire` (not `LeaseService::acquire`) before any run-state mutation. Lock contention still surfaces `ProjectWriterLockHeld`.

**Storage layer** (`fs.rs`): `list_leases` filters to worktree-only for existing callers; `list_lease_records` returns all records for reconcile. `read_lease`/`write_lease` correctly reject/wrap CLI writer records.

**Daemon CWD safety**: `daemon_loop.rs` contains zero `set_current_dir` call sites, verified by grep and enforced by existing conformance test.

**Test coverage**: All acceptance criteria are covered — guard lifecycle (4 async unit tests + 4 integration tests), reconcile for stale CLI leases (3 unit tests), partial cleanup failure with missing writer lock (unit + CLI test), end-to-end CLI conformance test (inject stale → reconcile → run start succeeds), serde round-trips, legacy backward compat, and the `set_current_dir` structural guard. All 434 unit tests, 218 conformance tests pass, and `cargo build` is clean.

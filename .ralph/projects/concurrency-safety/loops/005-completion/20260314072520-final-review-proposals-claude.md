---
artifact: final-review-proposals
loop: 5
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T07:25:20Z
---

I've completed a thorough review of the entire changeset. Here's my assessment:

# Final Review: NO AMENDMENTS

## Summary

The implementation is correct, safe, and robust across all changed files. Key findings:

**Model layer** (`model.rs:223-360`): The `CliWriterLease` struct, `LeaseRecord` enum, and backward-compatible serde wire format (`LeaseRecordWire` with `#[serde(untagged)]` + `LegacyWorktreeLease` with `#[serde(deny_unknown_fields)]`) are all correct. Legacy worktree leases without `lease_kind` fall through `Tagged` and match `LegacyWorktreeLease`; new records match `Tagged` first. No deserialization ambiguity.

**CLI writer lease guard** (`cli_writer_lease.rs`): The RAII guard correctly coordinates heartbeat shutdown via a three-layer synchronization pattern (`Notify` + `AtomicBool` + `std::sync::Mutex`). The double-check on `closed` at lines 99 and 105 prevents any heartbeat tick from racing with drop cleanup. The `std::sync::Mutex` is only held across synchronous code (no await points), so no executor starvation risk. Drop correctly preserves the lease record when lock release fails (lines 161-175), ensuring reconcile visibility.

**Reconcile** (`lease_service.rs:440-546`): Pass 2 for CLI leases correctly skips `Worktree` records, validates `project_id` before side effects, releases the writer lock before deleting the lease record, and reports cleanup failures for `AlreadyAbsent`, `OwnerMismatch`, and I/O errors. CLI leases never increment `failed_tasks`.

**CLI integration** (`run.rs:153-162, 237-246`): Both `handle_start` and `handle_resume` acquire `CliWriterLeaseGuard` before any run-state mutation and hold it as `_lock_guard` for the function scope. Lock contention correctly surfaces `ProjectWriterLockHeld`.

**fs.rs adapter** (`fs.rs:1109-1392`): `list_leases` correctly filters to worktree-only; `list_lease_records` returns all types. The `release_writer_lock` implementation is TOCTOU-safe with inode verification and proper staging-file cleanup on mismatch. Post-rename verification and restore failures now surface explicit I/O errors instead of being silently discarded.

**CWD safety**: Confirmed no `set_current_dir` in `daemon_loop.rs`.

**Verification**: `cargo build` succeeds, all 440 unit/integration tests pass, all 218 conformance scenarios pass.

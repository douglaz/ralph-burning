---
artifact: final-review-planner-positions
loop: 5
project: concurrency-safety
backend: claude(opus)
role: planner
created_at: 2026-03-14T07:35:56Z
---

# Planner Positions

## Amendment: CS-LKR-001

### Position
ACCEPT

### Rationale
The problem is real and verified in the source code. At `src/adapters/fs.rs:1306` and `src/adapters/fs.rs:1327`, the verification-failure recovery paths use `fs::rename(&staging, &path)`, which on Unix atomically replaces the destination file. Between Phase 2 (line 1282, rename of `path -> staging`) and these error branches, another writer can acquire a new lock at `path`. The `rename(&staging, &path)` recovery would silently overwrite that new live lock, violating the owner-safety invariant.

The code already demonstrates awareness of this exact race in the inode-mismatch branch at lines 1339-1353, which correctly uses `fs::hard_link(&staging, &path)` with `AlreadyExists` handling — a strategy that fails safely if a new lock already exists at the canonical path. The two error-recovery branches at lines 1306 and 1327 simply omit this safe pattern, creating an inconsistency where the same function handles one race correctly but leaves two others vulnerable.

This is not a theoretical concern — the entire purpose of the staged-rename verification flow is to handle concurrent writers, and the recovery paths are precisely the branches that execute when something unexpected happens during that contention window. Using `rename` here directly undermines the safety guarantee the surrounding code was designed to provide.

## Amendment: CS-LKR-002

### Position
ACCEPT

### Rationale
The problem is real and the failure cascade is accurately described. At `src/contexts/automation_runtime/cli_writer_lease.rs:173-174`:

```rust
if lock_released {
    let _ = self.store.remove_lease(&self.base_dir, &self.lease_id);
}
```

The `let _ =` silently discards I/O errors from `remove_lease`. Both call sites in `src/cli/run.rs` (lines 156 and 240) bind the guard as `_lock_guard` and rely solely on implicit `Drop` for cleanup. If `remove_lease` fails after a successful lock release, the CLI command exits with `Ok(())`, leaving an orphaned lease record (lock file gone, lease file present).

The cascade into a persistent failure state is confirmed in `src/contexts/automation_runtime/lease_service.rs:478-517`. When reconcile encounters this orphaned lease, it calls `release_writer_lock` (line 480) which returns `AlreadyAbsent` (line 482) since the lock was already successfully released during drop. This sets `has_sub_step_failure = true` (line 490), which triggers the `continue` at line 515-516, skipping the lease record deletion at line 521. The orphaned lease is never cleaned up and will trigger the same failure on every subsequent reconcile cycle — a persistent manual-repair state, exactly as described.

The proposed fix — adding an explicit fallible `close()` method while keeping `Drop` as best-effort — is a well-established Rust pattern (analogous to `File::sync_all()` before close, or `JoinHandle::join()`). It surfaces cleanup failures on the happy path where the caller can handle them, while preserving the safety-net `Drop` for panic and error unwind paths.

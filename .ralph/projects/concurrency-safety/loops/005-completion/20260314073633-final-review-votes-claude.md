---
artifact: final-review-votes
loop: 5
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T07:36:33Z
---

# Vote Results

## Amendment: CS-LKR-001

### Vote
ACCEPT

### Rationale
The code confirms the inconsistency is real and dangerous. At lines 1306 and 1327, `fs::rename(&staging, &path)` is used for recovery, which on Unix atomically replaces the destination. Between the Phase 2 rename at line 1282 and these error branches, another writer can acquire a new lock at `path`. A successful `rename` back silently destroys that new live lock — and notably, the "success" case of the rename is the dangerous one, not the failure case.

The fix is already demonstrated in the same function: the inode-mismatch branch at lines 1339-1353 uses `fs::hard_link(&staging, &path)` with explicit `AlreadyExists` handling. This pattern fails safely when a new lock exists at the canonical path. The two error-recovery branches simply omitted this same safe pattern, which is clearly an oversight rather than a deliberate design choice — the surrounding code proves the authors understood this exact race.

The scope of the fix is minimal (two call sites in one function, with a known-correct pattern already adjacent), and the risk of *not* fixing it is a violation of the core owner-safety invariant that the entire staged-rename flow exists to enforce.

## Amendment: CS-LKR-002

### Vote
ACCEPT

### Rationale
The failure cascade is verified end-to-end in the source:

1. `cli_writer_lease.rs:174` — `let _ =` discards `remove_lease` errors after successful lock release
2. `run.rs:156` and `run.rs:240` — guard bound as `_lock_guard`, relying solely on implicit `Drop`
3. `lease_service.rs:482` — reconcile encounters orphaned lease, `release_writer_lock` returns `AlreadyAbsent` (lock was already released during drop)
4. `lease_service.rs:490` — sets `has_sub_step_failure = true`
5. `lease_service.rs:515-516` — `continue` skips the lease record deletion at line 521

This creates a permanent loop: each reconcile cycle hits the same orphaned lease, fails the same way, and never cleans it up. The only resolution is manual intervention.

The proposed fix — an explicit fallible `close()` method with `Drop` as best-effort fallback — is idiomatic Rust (cf. `File::sync_all()`, `JoinHandle::join()`). It correctly distinguishes between the happy path (where cleanup failures should surface) and the unwind path (where best-effort is appropriate). The change is well-scoped to two files and doesn't alter the existing `Drop` safety net.

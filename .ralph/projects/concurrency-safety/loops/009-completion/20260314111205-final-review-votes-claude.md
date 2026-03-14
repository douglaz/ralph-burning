---
artifact: final-review-votes
loop: 9
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T11:12:05Z
---

# Vote Results

## Amendment: CONC-001

### Vote
ACCEPT

### Rationale
Verified in the source. `release()` at line 255 can only fail via `ProjectId::new()` — worktree errors are now wrapped in `Ok(ReleaseResult { worktree_error: Some(...) })` at lines 267-280. The `Err(e)` arm at lines 550-557 still labels the error `worktree_remove:` with a comment referencing "Physical release failed (e.g. worktree removal)". This is factually wrong: the only `Err` reaching that arm is a `ProjectId` validation failure. The fix is trivially correct and zero-risk — relabeling a string and updating a stale comment.

## Amendment: RB-REVIEW-20260314-01

### Vote
ACCEPT

### Rationale
Verified the full chain in both files. The scenario is real and produces a permanently orphaned lease:

1. `cli_writer_lease.rs:87-96` — `close_inner()` successfully releases the writer lock (`Released`), then `remove_lease()` fails, returning `GuardCloseFailed { step: "lease_file_delete" }`. Writer lock is gone; CLI lease record persists on disk.
2. `lease_service.rs:602-612` — next reconcile calls `release_writer_lock()` on this stale lease, gets `AlreadyAbsent`, sets `has_sub_step_failure = true`.
3. `lease_service.rs:637-638` — `if has_sub_step_failure { continue; }` unconditionally skips lease-file deletion at line 643.

Every subsequent reconcile repeats this exact loop: find stale lease, attempt lock release, get `AlreadyAbsent`, record failure, skip deletion. The stale record is never pruned. The key insight is that `AlreadyAbsent` is qualitatively different from `OwnerMismatch` or `Err` — the lock is already gone, so proceeding to delete the orphaned lease record is safe and correct. The proposed fix (record the anomaly but still attempt lease-file deletion for the `AlreadyAbsent` case) is the right minimal change.

## Amendment: RB-REVIEW-20260314-02

### Vote
ACCEPT

### Rationale
Verified at `tests/cli.rs:4766-4772`. The test creates a temp directory via `tempdir()` (defaults to OS `/tmp`), copies/hardlinks the binary there, and executes it via `Command::new(&stable_binary)`. On hardened Linux environments where `/tmp` is mounted `noexec`, execution will fail with a permission error that has nothing to do with product correctness. The comment at lines 4761-4765 explains the intent (avoiding ETXTBSY/ENOENT from cargo relink races), so the binary does need to live outside the build `target/` tree — but it should be in an exec-capable location. Using a subdirectory under `current_exe().parent()` or a workspace-local temp path preserves the race-avoidance intent while being compatible with `noexec` tmp mounts. Straightforward portability fix.

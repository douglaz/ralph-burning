---
artifact: final-review-votes
loop: 3
project: concurrency-safety
backend: claude
role: final_reviewer
created_at: 2026-03-14T04:32:53Z
---

# Vote Results

## Amendment: CSR-20260314-01

### Vote
ACCEPT

### Rationale
Confirmed in source. `acquire_writer_lock` (fs.rs:1228) writes the `lease_id` into the lock file via `FileSystem::write_create_new(&path, lease_id)`, but `release_writer_lock` (fs.rs:1245) performs a blind `fs::remove_file` without ever reading the contents back. The trait signature at mod.rs:78-82 takes only `(base_dir, project_id)` — no owner token parameter exists.

Both cleanup sites use this blind removal: the guard drop at cli_writer_lease.rs:165 and stale reconcile at lease_service.rs:466. The race is straightforward: Process A loses its lock (e.g., via stale reconcile), Process B acquires and writes its own `lease_id`, then Process A's deferred guard drop fires and blindly deletes Process B's live lock file. The `lease_id` was stored at acquisition time precisely to enable ownership verification, but no verification ever happens. A compare-and-delete release is the correct fix and makes the stored `lease_id` actually load-bearing.

## Amendment: CSR-20260314-02

### Vote
ACCEPT

### Rationale
Confirmed in source. Both cleanup paths delete the lease record before confirming the writer lock is released:

1. **Guard drop** (cli_writer_lease.rs:162-167): `remove_lease` at line 162 executes before `release_writer_lock` at line 165, and both results are silently discarded with `let _ =`. If the lock release fails for any reason (permissions, filesystem error), the lease record is already gone and the orphaned lock file becomes permanently unrecoverable — reconcile iterates lease records to discover locks, so no record means no discovery.

2. **Stale reconcile** (lease_service.rs:446-466): `store.remove_lease` at line 446 runs before `ProjectId::new` validation at line 454. If validation fails, the code pushes a `cleanup_failure` and `continue`s — but the lease record was already deleted at line 446. The writer lock file is stranded with no corresponding lease record for any future reconcile pass to find.

The lease record is the breadcrumb that makes orphaned writer locks discoverable by the self-healing reconcile path. Deleting it before confirming lock release breaks that invariant. Reversing the order (release lock first, delete lease only on success) is a straightforward correctness fix that preserves recoverability.

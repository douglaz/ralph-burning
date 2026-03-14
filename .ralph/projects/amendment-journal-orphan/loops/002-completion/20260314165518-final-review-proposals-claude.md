---
artifact: final-review-proposals
loop: 2
project: amendment-journal-orphan
backend: claude
role: final_reviewer
created_at: 2026-03-14T16:55:18Z
---

Build succeeds and all 459 tests pass. Now let me complete my detailed review by tracing through the critical logic paths.

# Final Review: NO AMENDMENTS

## Summary

The implementation correctly fixes the amendment journal orphan bug by applying **both Strategy A and Strategy B** from the master prompt, creating defense-in-depth against duplicate amendments.

### Strategy A: Cleanup unjournaled amendment files on append failure (`engine.rs:998-1013`)

On journal append failure at batch index `N`:
- Files for `amendments[N..]` (the failed amendment and all later ones) are removed via `remove_amendment`
- Only successfully journaled amendments (`amendments[..=last_index]`) are added to the snapshot's pending queue
- Special case: when the **first** append fails (`last_journaled_amendment_index == None`), all disk files are cleaned up but the full batch is preserved in the snapshot for resume. This correctly prevents the completion round from being silently dropped.

### Strategy B: Idempotent reconciliation (`engine.rs:2259-2297`)

`reconcile_amendments_from_disk` now receives the journal events and builds a `journaled_ids` set from all `AmendmentQueued` entries. Any disk amendment already represented in the journal is skipped. This provides a safety net if Strategy A cleanup fails (e.g., filesystem error during `remove_amendment`).

### Key correctness properties verified by code tracing:

1. **No duplicates after mid-batch failure + resume**: After failure at index 1 of 3, disk has only amendment 0 (file), journal has amendment_queued for amendment 0, snapshot has amendment 0 in pending. On resume, reconciliation skips the disk file (already journaled). Single copy throughout.

2. **No duplicates after first-append failure + resume**: All disk files cleaned up, all amendments preserved in snapshot only. On resume, reconciliation finds no disk files and returns early. Amendments processed from snapshot.

3. **Cleanup failure resilience**: If `remove_amendment` fails for some files, Strategy B catches them during reconciliation — the orphaned file is in `journaled_ids` if its event was written, so it's skipped; or it's added to the snapshot exactly once via the `existing_ids` set.

4. **Journal remains append-only**: No journal mutations, compaction, or rewrites. Only new events are appended.

5. **No public CLI behavior changes**: All changes are internal to the persistence and reconciliation paths.

### Failpoint infrastructure

The refactored `maybe_inject_project_failpoint` mechanism (`fs.rs:57-93`) is well-designed:
- Project-scoped format (`<project_id>:<threshold>`) prevents cross-test interference
- Counter resets when config changes, enabling proper test isolation
- `ScopedJournalAppendFailpoint` RAII guard serializes failpoint-using tests via `FAILPOINT_ENV_MUTEX`

### Test coverage

All four required test scenarios are present and pass:
1. `mid_batch_journal_append_failure_cleans_up_orphaned_files` — Strategy A cleanup verification
2. `resume_after_partial_journal_failure_no_duplicate_amendments` — end-to-end resume with no duplicates
3. `resume_after_first_journal_append_failure_preserves_pending_amendments` — zero-prefix edge case
4. `full_batch_success_persists_all_amendments` — regression test for the happy path

Build succeeds, all 459 tests pass.

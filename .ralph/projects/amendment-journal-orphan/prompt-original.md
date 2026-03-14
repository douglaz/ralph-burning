# Fix amendment journal orphan on mid-batch append failure

## Objective

Fix the amendment persistence race in `ralph-burning-rewrite/` where a journal append failure mid-batch leaves orphaned amendment files on disk.

## Problem (GitHub #4)

In `src/contexts/workflow_composition/engine.rs:935-1006`, amendment files are written to disk, then journal `amendment_queued` events are emitted one-by-one. If a journal append fails mid-batch:
- Already-written amendment files remain on disk
- Already-appended journal events for earlier amendments persist
- On resume, `reconcile_amendments_from_disk` re-reads all amendment files and re-adds them to the snapshot, creating duplicates for amendments that already have journal events

## Required Changes

1. After writing all amendment files and before emitting journal events, record the amendment IDs written
2. In the journal-append failure path, clean up amendment files that don't yet have journal events (all amendments from the failed one onward)
3. Alternatively, make `reconcile_amendments_from_disk` idempotent by checking journal events before re-adding — skip amendments that already have a `amendment_queued` event in the journal
4. Add tests that verify:
   - Mid-batch journal failure cleans up orphaned amendment files (or reconciliation deduplicates correctly)
   - Resume after partial failure doesn't create duplicate amendments
   - Full batch success still works correctly

## Constraints
- Do not change any public CLI behavior
- All existing tests (`cargo test`) and conformance scenarios (`ralph-burning conformance run`) must continue to pass
- Use `nix develop -c cargo test` and `nix develop -c cargo build` to build and test
- Preserve the append-only invariant of the journal

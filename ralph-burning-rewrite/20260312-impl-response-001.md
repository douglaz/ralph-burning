# Implementation Response (Iteration 1)

## Changes Made
1. **Completion guard resumability** — When `completion_guard()` returns `CompletionBlocked` inside `complete_run()`, the snapshot is now persisted with `status = Failed`, `active_run = None`, and a descriptive `status_summary` before the error is returned. This ensures `run resume` can pick the run back up (it requires `Failed` or `Paused` with no active run). Non-`CompletionBlocked` errors from the guard (e.g. I/O errors from `has_pending_amendments`) propagate without the snapshot transition. File: `engine.rs:1488-1497`.

2. **Amendment batch write rollback on partial failure** — The amendment file write loop now tracks successfully written amendment IDs. If any write fails mid-batch, all already-written files from the same batch are rolled back via `remove_amendment()` before calling `fail_run_result()`. This prevents orphaned partial batches that could confuse resume reconciliation. File: `engine.rs:636-665`.

3. **Deterministic ordering via `batch_sequence`** — Added a `batch_sequence: u32` field to `QueuedAmendment` (with `#[serde(default)]` for backward-compatible deserialization). `build_queued_amendments()` sets it from the enumeration index (1-based). All sort sites — `FsAmendmentQueueStore::list_pending_amendments()` and `reconcile_amendments_from_disk()` — now sort by `(created_at, batch_sequence)` instead of `created_at` alone, guaranteeing deterministic within-batch ordering even when timestamps are identical.

4. **Regression test: completion guard resumability** — New test `completion_guard_produces_resumable_snapshot_when_disk_amendments_remain` places an orphaned amendment file on disk, runs a full standard flow, verifies the guard fires with a `CompletionBlocked` error, asserts the snapshot is `Failed` with `active_run == None`, then resumes successfully (the engine reconciles the orphaned amendment, restarts from planning, drains it, and completes).

5. **Regression test: batch_sequence deterministic ordering** — New test `amendment_queue_batch_sequence_provides_deterministic_ordering` writes three amendments with the same timestamp but different `batch_sequence` values in reverse order, then verifies `list_pending_amendments` returns them sorted correctly.

6. **Conformance scenarios** — Added SC-CR-013 (completion guard leaves snapshot in resumable state) and SC-CR-014 (same-batch amendments ordered deterministically by batch_sequence). Updated SC-CR-012 description to reflect batch rollback semantics.

## Could Not Address
- None

## Pending Changes (Pre-Commit)
- `src/contexts/workflow_composition/engine.rs` — completion guard snapshot transition, batch write rollback, batch_sequence in builder, sort key update in reconciliation
- `src/contexts/project_run_record/model.rs` — `batch_sequence` field on `QueuedAmendment`
- `src/adapters/fs.rs` — sort key update in `list_pending_amendments`
- `tests/unit/workflow_engine_test.rs` — new completion guard regression test
- `tests/unit/adapter_contract_test.rs` — updated `make_amendment` helper, new `make_amendment_with_seq` helper, new batch ordering test
- `tests/conformance/features/run_completion_rounds.feature` — SC-CR-012 updated, SC-CR-013 and SC-CR-014 added

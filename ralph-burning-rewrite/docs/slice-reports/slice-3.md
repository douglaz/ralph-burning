# Slice 3 Report — Manual Amendment Parity

## Scope

Slice 3 adds manual amendment management as a first-class CLI surface and
unifies the amendment intake path across manual, PR-review, issue-command,
and workflow-stage sources:

- `project amend add --text ...` / `--file ...`
- `project amend list`
- `project amend remove <id>`
- `project amend clear`
- Shared amendment service with dedup, reopen-on-completed, and journal events
- PR-review ingestion migrated to shared reopen/dedup infrastructure

## Contracts Changed

- `QueuedAmendment` extended with `source: AmendmentSource` and `dedup_key: String`
  fields. Backwards-compatible via `#[serde(default)]` — legacy records default
  to `workflow_stage` source and empty dedup key.
- `AmendmentSource` enum: `Manual`, `PrReview`, `IssueCommand`, `WorkflowStage`
- `AmendmentAddResult` enum: `Created { amendment_id }` / `Duplicate { amendment_id }`
- `amendment_queued` journal event now includes `source` and `dedup_key` fields
- New `amendment_queued_manual_event` builder for manual amendments (no `run_id`)
- New error variants: `DuplicateAmendment`, `AmendmentNotFound`,
  `AmendmentLeaseConflict`, `AmendmentClearPartial`
- `reopen_completed_project()` / `reopen_completed_project_with_snapshot()`
  extracted to shared service function used by both manual and PR-review paths
- `stage_amendment_batch()` shared service for batch amendment intake
- `remove_amendment()` and `clear_amendments()` now accept run snapshot ports
  and sync `run.json` atomically with disk
- `planning_stage_for_flow()` moved to service.rs (was duplicated in pr_review.rs)
- `FileSystem::project_root` visibility changed to `pub(crate)`
- PR-review ingestion service now takes a `JournalStorePort` and routes through
  shared staging service for consistent dedup, journal, and snapshot handling
- CLI `project amend add` uses RAII writer lease instead of probe-and-release
- CLI `project amend list` surfaces dedup_key metadata per amendment
- CLI `project amend clear` reports exact removed/remaining IDs on partial failure
- CLI body truncation uses char-aware logic (UTF-8 safe)

## Files Modified

- `src/contexts/project_run_record/model.rs` — AmendmentSource, QueuedAmendment fields
- `src/contexts/project_run_record/service.rs` — shared amendment service
- `src/contexts/project_run_record/journal.rs` — enriched journal events
- `src/shared/error.rs` — new error variants
- `src/cli/project.rs` — CLI subcommands
- `src/contexts/automation_runtime/pr_review.rs` — migrated to shared service
- `src/contexts/workflow_composition/engine.rs` — source/dedup_key fields
- `src/contexts/conformance_spec/scenarios.rs` — 12 conformance scenarios
- `src/adapters/fs.rs` — `project_root` visibility
- `tests/unit/project_run_record_test.rs` — 20 new unit tests
- `tests/cli.rs` — 12 new CLI integration tests
- `tests/unit/adapter_contract_test.rs` — updated QueuedAmendment constructors
- `tests/unit/prompt_builder_test.rs` — updated amendment helper
- `tests/unit/journal_test.rs` — updated event builder call
- `tests/conformance/features/manual_amendments.feature` — feature file
- `docs/amendments.md` — user-facing documentation
- `docs/slice-reports/slice-3.md` — this report

## Tests Run

- `cargo check` — clean
- `cargo check --features test-stub` — clean
- `cargo test --features test-stub --test unit dedup_key` — unit tests
- `cargo test --features test-stub --test unit amendment_source` — unit tests
- `cargo test --features test-stub --test unit add_manual_amendment` — unit tests
- `cargo test --features test-stub --test unit clear_amendments` — unit tests
- `cargo test --features test-stub --test cli project_amend` — CLI tests
- 12 conformance scenarios (`parity_slice3_*`)

## Results

- `cargo check` passed in both default and `test-stub` builds
- 20 new unit tests for dedup key computation, AmendmentSource serialization,
  backwards-compatible deserialization, and all service operations passed
- 12 CLI integration tests covering add/list/remove/clear, duplicate detection,
  completed-project reopen, journal recording, and lease conflict rejection passed
- 12 conformance scenarios (8 original + 4 new: restart persistence,
  completion blocking, lease-conflict rejection, run.json sync)

## Review Response Changes (Iteration 1)

1. **Canonical amendment state**: `add_manual_amendment`, `remove_amendment`,
   `clear_amendments`, and `reopen_completed_project` now update
   `snapshot.amendment_queue.pending` in `run.json` atomically.
2. **PR-review staging parity**: PR-review ingestion routed through shared
   `stage_amendment_batch` service for consistent dedup, journal, snapshot,
   and reopen behavior.
3. **Operator-facing CLI contract**: `project amend list` surfaces `dedup_key`
   metadata; partial `clear` reports exact removed/remaining IDs; body
   truncation is UTF-8 safe.
4. **Conformance deliverables**: 4 new scenarios added (restart persistence,
   completion blocking, lease-conflict rejection, run.json sync); executor
   assertions aligned with `Amendment: <id>` CLI output format.
5. **RAII writer lease**: `project amend add` acquires a real RAII writer lease
   instead of probe-and-release.
6. **UTF-8 truncation**: body preview in `project amend list` uses
   char-boundary-aware truncation.

## Review Response Changes (Iteration 2)

1. **Amendment list source of truth**: `project amend list` now reads from the
   canonical `RunSnapshot.amendment_queue.pending` in `run.json` instead of the
   file-backed queue. Full dedup key is exposed (no truncation).
2. **CLI integration tests aligned**: 5 failing tests updated to match the
   `Amendment: <id>` CLI output format — `project_amend_add_text_succeeds_and_prints_id`,
   `project_amend_add_file_succeeds`, `project_amend_add_then_list_shows_amendment`,
   `project_amend_remove_existing`, `project_amend_duplicate_manual_add_is_noop`.
3. **Conformance coverage expanded**:
   - `parity_slice3_completion_blocking` now asserts interrupted_run stage rewind
     to planning and verifies run restart behavior with pending amendments.
   - `parity_slice3_lease_conflict_rejection` fixed to write lock fixture at the
     real writer-lock path (`.ralph-burning/daemon/leases/writer-{id}.lock`).
   - New `parity_slice3_clear_partial_failure` scenario added to feature file
     and executor registrations.
4. **PR-review AmendmentsStaged metadata**: `stage_amendment_batch` now returns
   `Vec<String>` (staged IDs) instead of `usize`. PR-review journal metadata
   reports the deduplicated staged count and IDs rather than the full input batch.

## Review Response Changes (Iteration 3)

1. **Amendment mutation failure safety**: All mutation paths (`add`, `remove`,
   `clear`, `stage_amendment_batch`) now drive dedup and existence checks from
   canonical snapshot state instead of disk. `remove` and `clear` commit the
   snapshot update before performing best-effort file deletion. `add` rolls back
   the amendment file if the snapshot write fails. This eliminates
   canonical-state drift when file operations succeed but snapshot writes fail.
2. **Conformance: completion blocking**: `parity_slice3_completion_blocking`
   executor changed from `run start` to `run resume` to match the CLI contract
   for paused projects.
3. **Conformance: clear partial failure**: `parity_slice3_clear_partial_failure`
   now uses the deterministic `RALPH_BURNING_TEST_AMENDMENT_REMOVE_FAIL_AFTER`
   failpoint instead of filesystem permission tricks, and asserts exact
   removed/remaining amendment IDs match between stderr output and `run.json`.
4. **Unit tests for snapshot-write failures**: Added 3 new tests:
   `add_manual_amendment_rolls_back_file_on_snapshot_write_failure`,
   `remove_amendment_preserves_amendment_on_snapshot_write_failure`,
   `clear_amendments_preserves_all_on_snapshot_write_failure`.
5. **SharedRunSnapshotStore test fixture**: Added a read+write snapshot store
   for tests that call service functions multiple times and need writes visible
   on subsequent reads. Existing dedup and multi-call tests migrated to it.

## Review Response Changes (Iteration 4)

1. **Amendment commit transactionality**: Reordered `add_manual_amendment` and
   `stage_amendment_batch` so journal events are written AFTER the canonical
   snapshot is committed. A snapshot write failure can no longer leave orphaned
   journal entries. Journal writes are best-effort after canonical commit.
2. **Remove failure on disk deletion**: `remove_amendment` now deletes the file
   first. If file deletion fails, no mutation is visible (snapshot untouched).
   If the snapshot write fails after file deletion, the file is restored.
3. **Clear partial-failure invariants**: `clear_amendments` now deletes files
   first, then updates the snapshot. If all files are deleted but snapshot write
   fails, all files are restored. On partial deletion, `AmendmentClearPartial`
   is returned even if the repair snapshot write fails, ensuring the caller
   always gets the exact removed/remaining IDs.
4. **Partial-clear conformance proof**: `parity_slice3_clear_partial_failure`
   now requires BOTH the removed ID AND the remaining ID to be present in
   stderr (not just one of the pair). Uses `&&` within each ordering check
   instead of `||` across them.
5. **New unit test**: `remove_amendment_fails_when_file_deletion_fails` verifies
   that a remove with a failing disk delete leaves the snapshot untouched.
6. **Docs updated**: `amendments.md` failure safety section rewritten to match
   the actual write ordering (file → snapshot → journal for add; file → snapshot
   for remove/clear).

## Review Response Changes (Iteration 5)

1. **Durable history persistence**: Journal preparation (`read_journal`,
   `serialize_event`) now happens BEFORE any mutations in both
   `add_manual_amendment` and `stage_amendment_batch`. This eliminates the
   split outcome where a command could fail after the amendment was already
   committed to `run.json`, or succeed without the required history event.
   The journal append itself remains best-effort after canonical commit.
2. **Shared staging atomicity**: `stage_amendment_batch` now rolls back all
   earlier amendment files if a mid-batch `write_amendment` call fails. Previously,
   earlier files would leak if a later write in the same batch failed.
3. **Clear partial-failure invariant**: When `clear` reports partial success,
   the repair snapshot write must succeed before `AmendmentClearPartial` is
   returned. If the repair write fails, deleted amendment files are restored and
   the underlying I/O error is returned instead, ensuring `run.json` always
   reflects the actual pending set.
4. **Failure-injection test coverage**: Added 4 new unit tests with dedicated
   test fixtures:
   - `add_manual_amendment_fails_cleanly_on_journal_read_failure` — verifies no
     mutation occurs when journal preparation fails.
   - `stage_amendment_batch_rolls_back_earlier_files_on_mid_batch_write_failure`
     — verifies earlier files are cleaned up when a later write fails.
   - `clear_partial_failure_restores_files_when_repair_write_fails` — verifies
     deleted files are restored when the repair snapshot write fails.
   New fixtures: `FailingJournalStore`, `FailAfterNWritesAmendmentQueue`,
   `FailAfterNRemovesAmendmentQueue`, `FailingRepairWriteStore`.
5. **Docs updated**: `amendments.md` failure safety section rewritten to describe
   the journal-preparation-first ordering and the repair-write-failure behavior
   for `clear`.

## Remaining Known Gaps

- None within the Slice 3 acceptance scope

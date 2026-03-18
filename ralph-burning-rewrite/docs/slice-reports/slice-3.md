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

## Remaining Known Gaps

- None within the Slice 3 acceptance scope

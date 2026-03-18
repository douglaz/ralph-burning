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
- `reopen_completed_project()` extracted to shared service function used by both
  manual and PR-review paths
- `planning_stage_for_flow()` moved to service.rs (was duplicated in pr_review.rs)
- `FileSystem::project_root` visibility changed to `pub(crate)`

## Files Modified

- `src/contexts/project_run_record/model.rs` — AmendmentSource, QueuedAmendment fields
- `src/contexts/project_run_record/service.rs` — shared amendment service
- `src/contexts/project_run_record/journal.rs` — enriched journal events
- `src/shared/error.rs` — new error variants
- `src/cli/project.rs` — CLI subcommands
- `src/contexts/automation_runtime/pr_review.rs` — migrated to shared service
- `src/contexts/workflow_composition/engine.rs` — source/dedup_key fields
- `src/contexts/conformance_spec/scenarios.rs` — 8 conformance scenarios
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
- 8 conformance scenarios (`parity_slice3_*`)

## Results

- `cargo check` passed in both default and `test-stub` builds
- 20 new unit tests for dedup key computation, AmendmentSource serialization,
  backwards-compatible deserialization, and all service operations passed
- 12 CLI integration tests covering add/list/remove/clear, duplicate detection,
  completed-project reopen, journal recording, and lease conflict rejection passed
- 8 conformance scenarios passed

## Remaining Known Gaps

- None within the Slice 3 acceptance scope

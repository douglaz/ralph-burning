# Slice 1 Report — Full Requirements and PRD Parity

## Scope

Slice 1 implements the full requirements-drafting pipeline with staged execution, cache-keyed reuse, conditional question gating, quick-mode revision loop, versioned project seed, and stage-aware CLI output.

## Legacy References Consulted

The following old-`ralph` code, docs, and tests under `multibackend-orchestration/` were used as the parity reference for this slice (paths from `rb.md` architecture overview):

- `src/prd/pipeline.rs` — staged PRD pipeline: ideation, research, synthesis, implementation spec, gap analysis, validation, and seed generation stages; cache-keyed resume; conditional question round gating
- `src/prd/quick.rs` — quick PRD writer/reviewer revision loop, structured revision feedback, approval-based termination, bounded revision limit
- `src/daemon/interactive_prd.rs` — interactive PRD issue workflow: question round pause/resume, answer ingestion, stage rerun after answers
- `src/validate/` — conformance suite exercising staged happy path, cache reuse, question round end-to-end, quick revision loop, versioned seed output
- `rb.md` (lines 101–103, 153–155) — architecture overview documenting PRD pipeline structure, quick-PRD contracts, and caching behavior

## Contracts Changed

- `RequirementsRun` extended with: `current_stage`, `committed_stages` (BTreeMap), `quick_revision_count`, `last_transition_cached` — all `#[serde(default)]` for backward compatibility
- `RequirementsStageId` extended with 6 new variants: `Ideation`, `Research`, `Synthesis`, `ImplementationSpec`, `GapAnalysis`, `Validation`
- `RequirementsPayload` extended with constructors and validation for all new stage types
- `ProjectSeedPayload` versioned: `version` field (default 1 via `default_seed_version()`), `source: Option<SeedSourceMetadata>`
- `RequirementsJournalEventType` extended with: `StageCompleted`, `StageReused`, `QuestionRoundOpened`, `RevisionRequested`, `RevisionCompleted`
- `FullModeStage` enum with `pipeline_order()`, `downstream_stages()`, `question_round_invalidated()`

## New Types

- `IdeationPayload`, `ResearchPayload`, `SynthesisPayload`, `ImplementationSpecPayload`, `GapAnalysisPayload`, `ValidationPayload`
- `ValidationOutcome` (Pass/NeedsQuestions/Fail), `GapSeverity`, `SeedSourceMetadata`, `CommittedStageEntry`, `RevisionFeedback`
- `compute_stage_cache_key()` — deterministic hash from stage name + input

## Files Modified

- `src/contexts/requirements_drafting/model.rs` — domain types
- `src/contexts/requirements_drafting/contracts.rs` — validation contracts
- `src/contexts/requirements_drafting/renderers.rs` — Markdown renderers
- `src/contexts/requirements_drafting/service.rs` — orchestration (full pipeline, quick pipeline, cache, question rounds)
- `src/contexts/agent_execution/model.rs` — stage ID mapping
- `src/adapters/stub_backend.rs` — canned outputs for new stages
- `src/cli/requirements.rs` — stage-aware progress display
- `tests/unit/requirements_drafting_test.rs` — 22 new `parity_slice1_*` unit tests
- `tests/conformance/features/requirements_drafting.feature` — 7 new parity scenarios
- `src/contexts/conformance_spec/scenarios.rs` — 5 new conformance executors
- `docs/requirements.md` — workflow documentation
- `docs/slice-reports/slice-1.md` — this report

## Tests Run

- `cargo check` — passed (0 errors, 0 warnings)
- `cargo test --features test-stub --test unit` — 605 passed, 0 failed
- Unit tests cover: stage contracts, renderers, cache key determinism, pipeline ordering, question round invalidation via validation stage, backward compatibility, versioned seed, full-mode answer with cache reuse
- 8 conformance scenario executors: full-mode happy path, cache reuse, question round invalidation contract, quick-mode revision loop, quick-mode max revisions, versioned seed, show stage progress, backward-compat run.json

## Results

- All 605 unit tests pass
- Registry drift test passes (all feature scenarios have executors, no orphans)
- `cargo check` passes for both test-stub and production builds

## Review Response (Iteration 1)

### Required Change 1: Question-round invalidation
Fixed: `open_question_round` now clears downstream `committed_stages` entries (synthesis, implementation_spec, gap_analysis, validation, project_seed) before writing the paused `awaiting_answers` state. Ideation and research entries are preserved.

### Required Change 2: Seed rollback canonical state
Fixed: the journal-append failure path in `generate_and_commit_seed` now removes `committed_stages["project_seed"]` and resets `current_stage` to `Validation` before calling `fail_run`, ensuring canonical state is pinned to the last successful pre-seed boundary.

### Required Change 3: Conformance deliverables
Fixed: added missing `parity_slice1_cache_reuse_on_resume`, `parity_slice1_question_round_invalidates_downstream`, `parity_slice1_quick_mode_max_revisions`, and `parity_slice1_backward_compat_run_json` conformance executors. Updated feature file to match registry. Fixed 11 failing unit tests by updating stub configuration to use validation-driven question rounds (matching the new full-mode pipeline). Updated `answer_uses_round_two_ids` test to verify full-mode answer behavior with cache reuse.

### Recommended 1: Cache key hash comment
Fixed: updated docstring from "SHA-256" to accurately describe `DefaultHasher` (SipHash).

### Recommended 2: Seed source metadata docs
Fixed: updated `docs/requirements.md` to list actual `SeedSourceMetadata` fields (`mode`, `run_id`, `question_rounds`, `quick_revisions`) instead of the incorrect "committed stages and timing". Updated conformance feature file and executor to expect `source.mode = "draft"` instead of `"full"`.

## Review Response (Iteration 2)

### Required Change 1: Question-round accounting
Fixed: `question_round` now tracks completed rounds only. `open_question_round` no longer sets `run.question_round` — it uses the round number only as a local for naming suffixes. `answer()` increments `question_round` once per completed round. One question round now persists as `1` in both `run.json` and `seed/project.json`.

### Required Change 2: Journal durability for Slice 1 transitions
Fixed: `QuestionRoundOpened`, `RevisionRequested`, and `RevisionCompleted` journal appends are now durable — if they fail, canonical state is pinned to the last fully committed boundary and `fail_run` is called. `RevisionCompleted` failure also rolls back the revised draft payload/artifact pair.

### Required Change 3: Conformance coverage
Fixed: all four gap scenarios now exercise actual behaviors via in-process `RequirementsService` with custom stub configurations:
- `parity_slice1_cache_reuse_on_resume` — triggers a question round, answers, and verifies `StageReused` journal events for cached ideation/research
- `parity_slice1_question_round_invalidates_downstream` — triggers validation `needs_questions`, verifies synthesis+downstream cleared and ideation/research preserved
- `parity_slice1_quick_mode_revision_loop` — reviewer returns `request_changes` once then `approved`, verifies `quick_revision_count = 1` and `RevisionRequested` in journal
- `parity_slice1_quick_mode_max_revisions` — reviewer always returns `request_changes`, verifies run fails with revision limit message and `quick_revision_count >= 5`

## Review Response (Iteration 3)

### Required Change 1: Stage-reuse journal durability
Fixed: all 6 `StageReused` journal appends in `run_full_mode_pipeline` (ideation, research, synthesis, implementation_spec, gap_analysis, validation) now fail the run on append error instead of silently continuing via `let _ =`. Each uses `fail_run` and returns the error, keeping canonical state pinned to the last fully committed stage.

### Required Change 2: Quick-mode rollback state restoration
Fixed: review and revision rollback paths now save prior committed IDs before overwriting and restore them on journal failure:
- `ReviewCompleted` rollback restores `latest_review_id` to the prior committed review (instead of clearing to `None`)
- `RevisionCompleted` rollback restores `latest_draft_id` and `recommended_flow` to prior committed values (instead of clearing to `None`)
Added 2 new tests: `later_loop_review_journal_failure_restores_prior_review_id` and `revision_completed_journal_failure_restores_prior_draft_id`.

### Recommended 1: Max-revision conformance text
Fixed: updated `requirements_drafting.feature` max-revision scenario text from "five times" / count `5` to "always returns request_changes" / count `6`, matching the implemented `revision > MAX_QUICK_REVISIONS` behavior documented in `scenarios.rs`.

### Test Results (Iteration 3)
- `cargo check --features test-stub` — clean
- `cargo test --features test-stub --test unit` — 607 passed, 0 failed (1 ignored)
- Registry drift check — passed

## Review Response (Iteration 4)

### Required Change 1: Answer-boundary rollback
Fixed: on `AnswersSubmitted` journal append failure, `answer()` now restores the pre-answer `question_round`, `status`, and `status_summary`, and clears `answers.json` so `answers_already_durably_stored()` does not treat the failed run as having crossed the answer boundary. The run remains resumable via `requirements answer`.

### Required Change 2: Question-round open rollback
Fixed: on `QuestionRoundOpened` journal append failure, `open_question_round()` now restores `committed_stages` and `latest_question_set_id` from a pre-question snapshot, and removes the non-durable question-set payload/artifact pair before failing.

### Required Change 3: Full-mode stage rollback
Fixed: `commit_full_mode_stage()` now derives the prior `current_stage` from the last committed stage in pipeline order (rather than snapshotting `run.current_stage` which was already advanced by the pipeline code) and accepts `prior_recommended_flow` as a parameter. On `StageCompleted` journal failure, both fields are restored so `run.json` reflects only the last durable stage. The synthesis stage block captures `run.recommended_flow` before mutating it and passes the pre-mutation value.

### Required Change 4: Slice report legacy references
Added: documented the exact legacy `prd`/`quick-prd` references consulted from old-`ralph` under `multibackend-orchestration/`: `src/prd/pipeline.rs`, `src/prd/quick.rs`, `src/daemon/interactive_prd.rs`, `src/validate/`, and `rb.md` architecture overview.

### Recommended 1: MAX_QUICK_REVISIONS docs alignment
Fixed: updated `docs/requirements.md` from "is reached" to "the revision count exceeds `MAX_QUICK_REVISIONS` (5)", matching the `revision > MAX_QUICK_REVISIONS` code behavior.

### Test Results (Iteration 4)
- `cargo check` — clean (both production and test-stub builds)
- `cargo test --features test-stub --test unit` — 610 passed, 0 failed (1 ignored)
- Registry drift check — passed
- 3 new tests added: `answers_submitted_journal_failure_restores_question_boundary`, `question_round_opened_journal_failure_restores_pre_question_state`, `stage_completed_journal_failure_restores_current_stage_and_recommended_flow`

## Remaining Known Gaps

- None within the Slice 1 acceptance scope

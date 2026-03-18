# Slice 1 Report — Full Requirements and PRD Parity

## Scope

Slice 1 implements the full requirements-drafting pipeline with staged execution, cache-keyed reuse, conditional question gating, quick-mode revision loop, versioned project seed, and stage-aware CLI output.

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

## Remaining Known Gaps

- None within the Slice 1 acceptance scope
- Cache reuse and question-round conformance scenarios verify structural contracts via CLI state inspection; deep multi-step behavioral tests are covered by unit tests with custom stub configuration

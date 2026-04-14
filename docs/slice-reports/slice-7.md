# Slice 7: Prompt and Template Override Parity — Report

## Legacy References Consulted

- Slice 7 feature spec defining template catalog, override precedence, placeholder validation, and failure invariants
- Existing prompt construction in `engine.rs`, `prompt_review.rs`, `completion.rs`, `final_review.rs`, and `service.rs` (requirements)

## Contracts Changed

- All prompt surfaces (workflow stages, panels, requirements) now route through `template_catalog::resolve_and_render()` instead of inline `format!()` calls
- Three-tier precedence resolution: project override > workspace override > built-in default (layers never merged)
- Override files validated at resolution time for unknown/missing placeholders and UTF-8 correctness
- Malformed overrides produce `AppError::MalformedTemplate` and never silently fall back to a lower-precedence source
- Panel functions (`invoke_panel_member`, `invoke_completer`, `build_reviewer_prompt`, `build_voter_prompt`, `build_arbiter_prompt`) now accept `base_dir` and `project_id` parameters for template resolution
- `build_draft_prompt` now returns `AppResult<String>` instead of `String`

## Files Modified

- `src/contexts/workspace_governance/template_catalog.rs` — new module: template catalog with 32 template IDs, manifests, precedence resolution, validation, and rendering
- `src/contexts/workspace_governance/mod.rs` — added `pub mod template_catalog` export
- `src/shared/error.rs` — added `MalformedTemplate` variant to `AppError`
- `src/contexts/workflow_composition/engine.rs` — routed `build_stage_prompt()` through catalog
- `src/contexts/workflow_composition/prompt_review.rs` — routed panel prompts through catalog
- `src/contexts/workflow_composition/completion.rs` — routed completer prompt through catalog
- `src/contexts/workflow_composition/final_review.rs` — routed reviewer/voter/arbiter prompts through catalog
- `src/contexts/requirements_drafting/service.rs` — routed all 10 requirements prompt surfaces through catalog
- `src/contexts/conformance_spec/scenarios.rs` — 10 conformance scenario executors for Slice 7
- `tests/unit/template_catalog_test.rs` — 22 unit tests for catalog resolution, validation, rendering
- `tests/unit/prompt_builder_test.rs` — 2 regression tests for workflow override behavior
- `tests/unit/requirements_drafting_test.rs` — 5 tests for requirements override parity
- `tests/unit.rs` — registered `template_catalog_test` module
- `tests/conformance/features/template_overrides.feature` — 10 conformance scenarios
- `docs/templates.md` — user/operator documentation for template override system
- `docs/slice-reports/slice-7.md` — this report

## Tests Run

- `cargo check` (library)
- `cargo check --tests --features test-stub` (all tests)
- Unit tests: `template_catalog_test` (22 tests)
- Unit tests: `prompt_builder_test` override regression (2 tests)
- Unit tests: `requirements_drafting_test::template_override_parity` (5 tests)
- Conformance: 10 scenarios with `@parity_slice7_*` tags

## Review Response (Iteration 1)

### Required Change 1 — Failure invariants
Moved template resolution/rendering before all durable state writes:
- `engine.rs`: `build_stage_prompt()` now runs before `stage_entered` journal append and snapshot write
- `service.rs`: `resolve_and_render()` now runs before `write_run()` in all 6 full-mode stages (ideation, research, synthesis, implementation spec, gap analysis, validation)
- `service.rs`: Quick-mode revision template rendering now runs before `RevisionRequested` journal append

A malformed override now fails with no new journal entries, snapshots, or run-state transitions for the affected contract.

### Required Change 2 — Template documentation contract
Synced `docs/templates.md` with the actual manifests in `template_catalog.rs`:
- `final_review_reviewer`: `prompt_text` → `project_prompt`
- `final_review_voter`: `prompt_text, prior_reviews` → `title, amendments, json_schema`
- `final_review_arbiter`: `prompt_text, prior_reviews` → `amendments, reviewer_votes, json_schema`
- `requirements_review`: removed spurious `idea` from required placeholders
- `requirements_question_set`: `idea, draft_artifact, review_artifact` → `idea, missing_info`
- `requirements_project_seed`: `synthesis_artifact, impl_spec_artifact` → `requirements_artifact, follow_ups`
- `requirements_synthesis`: added missing `research_artifact` required placeholder, removed incorrect `answers` optional

### Recommended Improvement — CLI integration tests
Added two CLI integration tests in `tests/cli.rs`:
- `run_start_malformed_template_override_exits_nonzero_with_no_durable_state_change`: verifies malformed override causes non-zero exit, mentions the error in stderr, writes no stage_entered events, and creates no payloads
- `run_start_malformed_project_override_does_not_fall_back_to_workspace`: verifies a malformed project override is not silently replaced by a valid workspace override

## Review Response (Iteration 2)

### Required Change 1 — CLI regression test fix
Fixed the `run_start_malformed_template_override_exits_nonzero_with_no_durable_state_change` test in `tests/cli.rs`:
- Changed broad `!post_journal.contains("stage_entered")` assertion to planning-specific checks: no `stage_entered` or `stage_completed` for the `planning` stage specifically
- Removed pre-journal line-count comparison that was too strict — earlier stages like `prompt_review` legitimately enter and complete before the malformed `planning` template is reached
- Removed unused `pre_journal` variable

### Required Change 2 — Requirements project-override wiring (narrowed docs)
Requirements drafting operates at the workspace level before any project exists, so project-level template overrides are architecturally inapplicable. Updated `docs/templates.md`:
- Split resolution order documentation into workflow/panel (three-tier) and requirements (two-tier: workspace + built-in only)
- Clarified that project override paths apply to workflow and panel templates only
- The `None` project_id in all `resolve_and_render()` calls in `service.rs` is correct behavior, not a bug

### Recommended Improvement — Verbatim block preservation test
Added `render_preserves_verbatim_pre_rendered_blocks` in `tests/unit/template_catalog_test.rs`:
- Verifies multi-line JSON schema blocks and multi-paragraph prompt text survive substitution intact
- Pins the blank-line normalization behavior (3+ consecutive newlines collapsed to 2)

## Review Response (Iteration 3)

### Required Change 1 — Requirements project-override parity
Wired `project_id: Option<&ProjectId>` through the entire requirements service execution path:
- Updated `draft()`, `quick()`, and `answer()` public APIs to accept an optional project ID
- Updated all internal methods: `run_full_mode_pipeline()`, `run_quick_mode_pipeline()`, `open_question_round()`, `generate_and_commit_seed()`, `build_draft_prompt()`
- All 11 `resolve_and_render()` calls in `service.rs` now pass `project_id` instead of `None`
- CLI callers (`requirements.rs`, `project.rs`, `daemon_loop.rs`) pass `None` when no project context exists
- Updated `docs/templates.md` to document unified three-tier precedence for all prompt surfaces

### Required Change 2 — Malformed panel override failure invariants
Pre-validated panel template overrides before durable state writes in all three panel dispatch functions:
- `dispatch_prompt_review_panel()`: validates `prompt_review_refiner` and `prompt_review_validator` before `stage_entered`
- `dispatch_completion_panel()`: validates `completion_panel_completer` before `stage_entered`
- `dispatch_final_review_panel()`: validates `final_review_reviewer`, `final_review_voter`, and `final_review_arbiter` before `stage_entered`
A malformed panel override now fails before any journal event or snapshot mutation.

### Required Change 3 — Unknown placeholder rejection is incomplete
Extended `validate_template()` to reject any `{{...}}` marker token that is not a valid manifest placeholder:
- Added `extract_all_marker_tokens()` that captures every `{{...}}` token regardless of character validity
- `validate_template()` now compares all marker tokens against valid placeholders; tokens with invalid names (hyphens, spaces, etc.) are rejected as malformed
- Updated the internal test that previously codified the ignore behavior
- Added 2 unit tests: `placeholder_with_hyphens_rejected_as_malformed`, `placeholder_with_spaces_rejected_as_malformed`
- Added conformance scenario `@parity_slice7_invalid_marker_rejection`

## Remaining Known Gaps

- Template path helpers were placed in `template_catalog.rs` rather than `adapters/fs.rs` as specified. The deviation keeps template logic self-contained in one module rather than splitting between the catalog and the filesystem adapter.

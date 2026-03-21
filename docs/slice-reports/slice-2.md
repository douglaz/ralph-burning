# Slice 2 Report — Bootstrap and Auto Parity

## Scope

Slice 2 adds the requirements-seed ingestion glue that was still missing after
Slice 1:

- `project create --from-requirements <run-id>`
- `project bootstrap --idea ...`
- `project bootstrap --from-file ...`
- optional `project bootstrap --start`

The implementation preserves project-creation atomicity and the required split
between pre-creation failures (no project state) and post-creation start
failures (valid project retained).

## Legacy References Consulted

The following old-`ralph` references under `../ralph-burning/` were consulted
for the legacy `auto` and `quick-dev-auto` behavior:

- `src/cli/auto.rs` — CLI contract for idea input, project-id derivation, and
  workspace bootstrap behavior
- `src/cli/quick_dev_auto.rs` — CLI contract for quick-dev auto bootstrap
- `src/daemon/tasks.rs` — authoritative orchestration order for
  `auto`/`quick-dev-auto`: quick PRD → create project → start orchestration

## Contracts Changed

- `SeedHandoff` now carries `requirements_run_id` so the project-creation path
  can durably tag seed-originated projects
- `RequirementsStorePort` / `FsRequirementsStore` now expose
  `list_requirements_run_ids()` for CLI-side existence validation
- `project create` now accepts `--from-requirements <run-id>` as an alternate
  creation source
- `project bootstrap` is a new CLI surface that accepts `--idea` or
  `--from-file`, optional `--flow`, and optional `--start`
- `ProjectCreated` journal details for seed-originated projects now include:
  `source = "requirements"`, `requirements_run_id`, `seed_flow`, and optional
  `recommended_flow`

## Files Modified

- `src/contexts/requirements_drafting/service.rs`
- `src/adapters/fs.rs`
- `src/contexts/project_run_record/service.rs`
- `src/cli/project.rs`
- `src/cli/requirements.rs`
- `tests/unit/project_run_record_test.rs`
- `tests/cli.rs`
- `tests/conformance/features/bootstrap.feature`
- `src/contexts/conformance_spec/scenarios.rs`
- `docs/bootstrap.md`
- `docs/slice-reports/slice-2.md`

## Tests Run

- `cargo fmt`
- `cargo check`
- `cargo check --features test-stub`
- `cargo test --features test-stub --test unit create_project_from_seed`
- `cargo test --features test-stub --test cli project_create_from_requirements`
- `cargo test --features test-stub --test cli project_bootstrap`
- `cargo test --features test-stub --test cli requirements_show_displays_completed_run`
- `cargo test --features test-stub --test cli requirements_quick_creates_completed_run`
- `target/debug/ralph-burning conformance run --filter parity_slice2_create_from_requirements`
- `target/debug/ralph-burning conformance run --filter parity_slice2_bootstrap_standard`
- `target/debug/ralph-burning conformance run --filter parity_slice2_bootstrap_quick_dev`
- `target/debug/ralph-burning conformance run --filter parity_slice2_bootstrap_with_start`
- `target/debug/ralph-burning conformance run --filter parity_slice2_bootstrap_from_file`
- `target/debug/ralph-burning conformance run --filter parity_slice2_failure_before_creation`
- `target/debug/ralph-burning conformance run --filter parity_slice2_failure_after_creation_before_start`
- `target/debug/ralph-burning conformance run --filter parity_slice2_duplicate_seed_project_id`

## Results

- `cargo check` passed in both default and `test-stub` builds
- 5 new unit tests for `create_project_from_seed()` passed
- 6 targeted CLI tests for `project create --from-requirements`,
  `project bootstrap`, and impacted requirements output passed
- All 8 Slice 2 conformance scenarios passed
- No compilation or registry-drift issues were encountered during verification

## Remaining Known Gaps

- None within the Slice 2 acceptance scope

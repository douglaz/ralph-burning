# Add adapter tests and error handling for missing tools, malformed output, and sync failures

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add new test files and test functions only. Do NOT modify existing source code (adapters,
models, error types). Minor additions to source are only acceptable if strictly required to
expose existing behavior for testing (e.g., a `#[cfg(test)]`-gated constructor) — prefer
avoiding any source changes when possible.

## Scope note (from the bead's planner comment)
This bead MUST cover BOTH error handling AND happy-path tests. The title emphasizes error
handling, but the adapter tests must also verify successful create/update/close/sync flows,
correct JSON parsing of well-formed output, and nominal dependency operations. Error-only
testing is insufficient.

## Background — what already exists

### br adapter (`src/adapters/br_process.rs`):
- `BrError` enum: `BrNotFound`, `BrTimeout`, `BrExitError`, `BrParseError`, `Io`
- `BrOutput { stdout, stderr, exit_code }`
- `BrCommand` — type-safe builder (show, ready, update_status, close, list_by_status,
  sync_flush, sync_import, dep_tree, graph, create/custom)
- `ProcessRunner` trait (already async, already `Send + Sync`) — this is the seam for mocks.
  `OsProcessRunner` is the production impl. Custom test runners can implement this trait.
- `BrAdapter<R: ProcessRunner>` — read-only operations
- `BrMutationAdapter<R: ProcessRunner>` — create/update/close with sync tracking
- Pending-mutation journal: `PENDING_MUTATIONS_DIR = ".beads/.br-unsynced-mutations.d"` and
  legacy marker `.beads/.br-unsynced-mutations` (dirty-flag discipline)
- Repo-wide lock file: `.beads/.br-sync.lock`
- Existing `#[cfg(test)] mod tests` in br_process.rs has command-builder unit tests only —
  no process-runner mocking. That's the gap.

### bv adapter (`src/adapters/bv_process.rs`):
- `BvError` enum (mirrors BrError shape)
- `BvOutput`, `BvCommand` (flag-style: `--robot-triage`, `--robot-next`, etc.)
- Also has a `ProcessRunner` trait / mockable seam
- Existing test module at line 434

### Test conventions (`tests/unit/`):
- Each concern gets its own file, e.g. `adapter_contract_test.rs`, `tmux_adapter_test.rs`,
  `stub_backend_test.rs`. Follow that pattern.
- Tests use `ralph_burning::adapters::...` imports (crate-name path)
- Tests use `tempfile::tempdir()` for workspace isolation
- Each test returns `Result<(), Box<dyn std::error::Error>>` or `Result<()>` using `?`
- Assertions include meaningful messages on failure

## What to implement

### Create `tests/unit/br_adapter_test.rs` (new)

A mock `ProcessRunner` implementation driven by a scripted queue of `(expected_args_pattern,
response_or_error)`. The mock records the actual calls it saw so tests can assert on the
command line produced.

Cover these categories (aim for ~18-24 tests total across br/bv):

#### br happy-path tests:
- `br_show_parses_well_formed_json` — mock returns valid bead JSON, verify struct fields
- `br_ready_returns_empty_list_when_no_ready_beads`
- `br_list_by_status_parses_array_output`
- `br_dep_tree_parses_nested_dependency_output`
- `br_graph_parses_workspace_graph_output`
- `br_create_mutation_issues_correct_command_line`
- `br_update_status_mutation_issues_correct_command_line`
- `br_close_mutation_issues_correct_command_line`
- `br_sync_flush_clears_dirty_marker_on_success`
- `br_sync_import_runs_after_flush_when_pending_mutations_present`

#### br error-handling tests:
- `br_missing_binary_produces_br_not_found_error` — runner returns `BrError::BrNotFound`,
  verify adapter surfaces it without swallowing details
- `br_timeout_produces_br_timeout_error_with_command_context`
- `br_non_zero_exit_produces_br_exit_error_preserving_stdout_stderr`
- `br_malformed_json_produces_br_parse_error_with_raw_output`
- `br_partial_json_output_surfaces_parse_error_not_panic`
- `br_sync_flush_failure_keeps_dirty_marker_so_retry_is_safe`

### Create `tests/unit/bv_adapter_test.rs` (new)

#### bv happy-path tests:
- `bv_robot_next_parses_recommended_bead`
- `bv_robot_triage_parses_triage_output`
- `bv_related_work_parses_related_work_output` (if implemented)
- `bv_impact_analysis_parses_output` (if implemented)

#### bv error-handling tests:
- `bv_missing_binary_produces_bv_not_found_error`
- `bv_empty_graph_reports_no_next_bead_gracefully` — not a panic, a clean "nothing ready" signal
- `bv_blocked_graph_reports_all_beads_blocked`
- `bv_non_zero_exit_surfaces_stderr_in_error`
- `bv_malformed_output_produces_bv_parse_error`
- `bv_timeout_produces_bv_timeout_error`

### Implementation guidance

- Put the mock runner in each test file (or a shared `br_adapter_test_support.rs` helper if
  both adapter tests need the same mock — either is fine). It should:
  - Implement the `ProcessRunner` trait for the adapter under test
  - Take a queue of scripted responses (each either a `BrOutput`/`BvOutput` OR a `BrError`/`BvError`)
  - Record the actual args passed, so tests can assert the command line produced
- Use `#[tokio::test]` for async tests, matching the async `ProcessRunner::run` signature.
- Mutation tests should verify the pending-mutation journal/marker behavior by
  constructing the adapter with a `tempdir()` workspace and inspecting files under
  `.beads/.br-unsynced-mutations.d/` and `.beads/.br-unsynced-mutations` after each call.
- Assertion messages should explain the expected invariant on failure (e.g.,
  `"sync_flush failure must leave dirty marker in place for safe retry"`).
- Never spawn a real `br` or `bv` process. All tests use the mock seam.
- Reuse existing fixtures in `src/test_support/` if helpful (e.g. `BeadGraphFixtureBuilder`)
  but do not require them — a simple JSON string literal in the test is fine.

### Test harness hookup

- If `tests/unit.rs` (or `tests/unit/mod.rs`) declares each unit test file explicitly, add
  `mod br_adapter_test;` and `mod bv_adapter_test;` so cargo picks them up.
- If the unit harness auto-discovers, no hookup is needed — just confirm with `cargo test
  --test unit br_adapter` after adding the files.

### Do NOT
- Do NOT modify existing source in `src/adapters/br_process.rs` or `src/adapters/bv_process.rs`
  beyond the minimum needed to expose mockable surface, if anything. The `ProcessRunner`
  trait is already public — no changes should be required.
- Do NOT touch `.ralph-burning/` orchestration state.
- Do NOT modify existing tests in `tests/unit/` other than the harness hookup line.
- Do NOT introduce new dependencies.

## Acceptance Criteria
- ~18-24 new adapter tests covering happy-path AND error paths for both br and bv
- All tests use mock process runners (no real br/bv execution)
- Tests verify command-line construction, JSON parsing, and error-type surfacing
- Mutation tests verify dirty-flag / pending-mutation journal discipline
- Clear assertion messages on failure
- All existing tests still pass
- `cargo test && cargo clippy -- -D warnings && cargo fmt --check` pass
- `nix build` passes on the final tree

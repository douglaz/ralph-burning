# Add CLI integration tests for milestone commands, task aliases, and compatibility

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add new test functions only. Do NOT modify existing source code or existing tests.

## Background — what already exists

### Existing CLI tests (`tests/cli.rs`):
- ~251 integration tests already exist
- Tests use `Command::new(binary())` to run the actual CLI binary
- Fixture helpers: `initialize_workspace_fixture()`, `write_milestone_fixture()`, `write_br_milestone_selection_script()`
- `prepend_path()` for mock script injection

### Milestone CLI (`src/cli/milestone.rs`):
- Subcommands: Create, Plan, ExportBeads, Next, Run, Show, BeadHistory, Status, Tasks

### Task CLI (`src/cli/task.rs`):
- Subcommands: Create, Show, Select, List — aliases for project commands

### Project CLI (`src/cli/project.rs`):
- Deprecation notices on all commands (stderr, not stdout)
- All existing commands still work

## What to implement

### Create new tests in `tests/cli.rs`

#### 1. Milestone command tests:
- `milestone_create_produces_valid_milestone` — create with valid args, verify exit 0 and milestone directory created
- `milestone_create_rejects_invalid_id` — invalid characters in ID, verify helpful error
- `milestone_show_displays_milestone_detail` — show existing milestone, verify output contains expected fields
- `milestone_show_json_output` — show with --json, verify valid JSON with expected keys
- `milestone_status_shows_current_state` — status for existing milestone, verify state displayed

#### 2. Task alias tests:
- `task_show_dispatches_to_project_show` — verify task show produces same output as project show
- `task_list_dispatches_to_project_list` — verify task list works
- `task_select_dispatches_to_project_select` — verify task select works

#### 3. Compatibility tests:
- `project_commands_print_deprecation_to_stderr` — verify deprecation notice on stderr
- `project_commands_stdout_unaffected_by_deprecation` — verify piped stdout has no deprecation text
- `project_show_still_works_with_deprecation` — full backward compat

#### 4. Error handling tests:
- `milestone_show_nonexistent_produces_error` — verify clear error for missing milestone
- `milestone_run_without_workspace_fails` — running outside workspace
- `task_show_nonexistent_produces_error` — clear error for missing task

#### 5. Output format tests:
- `milestone_show_human_readable_format` — verify text output has expected sections
- `milestone_status_json_format` — verify --json output is valid JSON with correct fields

### Implementation guidance
- Follow existing test patterns in tests/cli.rs
- Use `Command::new(binary())` for all tests
- Set up fixtures with existing helpers
- Each test should be independent (own temp dir)
- Assert on exit codes, stdout content, stderr content as appropriate

## Files to modify
- `tests/cli.rs` — add new test functions

## Acceptance Criteria
- All milestone commands tested for basic functionality
- Task aliases tested for correct dispatch
- Deprecation notices verified (stderr only)
- Error messages verified for helpfulness
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

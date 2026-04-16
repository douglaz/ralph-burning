# Add CLI commands for bead execution history and milestone task listing

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE-ONLY change
Do NOT delete, rewrite, or restructure any existing code. Only add new code.
The view models, service functions, and tests for the underlying queries already exist.
Your job is to wire two new CLI subcommands to the existing service layer.

## Background — what already exists (DO NOT MODIFY)

The following are already implemented and working:

- `RunHistoryView` in `src/contexts/project_run_record/queries.rs` — already has `milestone_id` and `bead_id` fields
- `history_lineage()` in `src/contexts/project_run_record/queries.rs` — extracts lineage from journal events
- `run_history()` in `src/contexts/project_run_record/service.rs` — already populates lineage
- `BeadLineageView` in `src/contexts/milestone_record/queries.rs` — milestone/bead metadata view
- `TaskRunAttemptView` in `src/contexts/milestone_record/queries.rs` — single execution attempt
- `BeadExecutionHistoryView` in `src/contexts/milestone_record/queries.rs` — lineage + runs combined
- `MilestoneTaskView` and `MilestoneTaskListView` in `src/contexts/milestone_record/queries.rs` — task listing view
- `read_bead_lineage()` in `src/contexts/milestone_record/service.rs:2254` — fetches lineage
- `find_runs_for_bead()` in `src/contexts/milestone_record/service.rs:2287` — queries task-run entries
- `bead_execution_history()` in `src/contexts/milestone_record/service.rs:2297` — combines lineage + runs
- `ProjectDetail.task_lineage` in `src/contexts/project_run_record/model.rs` — already populated by `show_project()`
- Comprehensive unit tests for all of the above already pass

## What to implement

### 1. `milestone bead-history <milestone_id> <bead_id>` subcommand

Add a new subcommand to `src/cli/milestone.rs` that calls the existing `bead_execution_history()` service function and prints the result.

- Add `BeadHistory { milestone_id: String, bead_id: String, json: bool }` variant to `MilestoneSubcommand` enum
- Add `handle_bead_history(milestone_id, bead_id, json)` handler function
- The handler should:
  1. Load the workspace base dir
  2. Call `milestone_service::bead_execution_history()` with appropriate store instances
  3. If `--json`: serialize `BeadExecutionHistoryView` as JSON and print
  4. If not `--json`: print human-readable format showing lineage header then a table of runs
- Follow the same patterns as `handle_show()` and `handle_status()` in the same file

### 2. `milestone tasks <milestone_id>` subcommand

Add a new subcommand to `src/cli/milestone.rs` that lists all Ralph projects/tasks linked to a milestone.

- Add `Tasks { milestone_id: String, json: bool }` variant to `MilestoneSubcommand` enum
- Add `handle_tasks(milestone_id, json)` handler function
- The handler should:
  1. Load the workspace base dir
  2. Scan all projects in `.ralph-burning/projects/` for ones whose `task_source.milestone_id` matches
  3. Build a `MilestoneTaskListView` from the matching projects
  4. If `--json`: serialize as JSON and print
  5. If not `--json`: print human-readable table (project_id, bead_id, status, created_at)

### 3. Unit tests

Add tests in `src/cli/milestone.rs` (or a test module) that verify:
- `bead-history` subcommand parses correctly
- `tasks` subcommand parses correctly
- The view construction logic works (can reuse existing test helpers from `src/contexts/milestone_record/queries.rs`)

## Files to modify (additive only)

- `src/cli/milestone.rs` — add two new subcommand variants and handlers
- NO other files should need changes

## Acceptance Criteria
- `milestone bead-history <milestone_id> <bead_id>` works with `--json`
- `milestone tasks <milestone_id>` works with `--json`
- All existing tests still pass unchanged
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

# Show milestone and bead lineage in task/project/run output

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add lineage display to existing commands. Do NOT change existing output fields or break existing behavior.

## Background — what already exists

### Task show (`src/cli/task.rs` lines 149-165):
- Already has `BeadLineageView` with milestone_id, milestone_name, bead_id, bead_title, acceptance_criteria
- Already displays lineage in text output
- No `--json` flag exists

### Project show (`src/cli/project.rs`):
- `ProjectRecord` has `task_source: Option<TaskSource>` with milestone_id, bead_id, parent_epic_id, origin, plan_hash, plan_version
- `ProjectDetail` carries `task_lineage: Option<BeadLineageView>`
- Text output shows project info but no `--json` flag

### Run commands (`src/cli/run.rs`):
- `run status` uses `RunStatusView` / `RunStatusJsonView` — NO lineage data
- `run history` exists but doesn't expose TaskSource/lineage
- No milestone_id or bead_id in run output structs

### What's missing:
1. `--json` flag for `task show` and `project show`
2. Lineage fields in `run status` and `run history` output
3. Bead scope summary in run detail

## What to implement

### 1. Add `--json` flag to `task show`

In `src/cli/task.rs`:
- Add `--json` flag to TaskShowArgs (or the Show subcommand)
- When `--json`, serialize the full `ProjectDetail` (which already includes `task_lineage`) as JSON to stdout
- This gives structured access to milestone_id, bead_id, bead_title

### 2. Add `--json` flag to `project show`

In `src/cli/project.rs`:
- Add `--json` flag to the Show subcommand
- When `--json`, serialize `ProjectDetail` as JSON

### 3. Add lineage to `run status` output

In `src/cli/run.rs`:
- Load the project's `TaskSource` when displaying run status
- Add `milestone_id` and `bead_id` fields to `RunStatusJsonView`
- In text output, show "Milestone: {name} | Bead: {title}" line when lineage exists
- Non-milestone tasks display normally (no lineage line)

### 4. Add lineage to `run history` output

In `src/cli/run.rs`:
- When displaying run history for a milestone-linked project, include the bead_id in each history entry
- In JSON output, add `bead_id` field to history entries

### 5. Add tests

- Test `task show --json` includes milestone_id and bead_id for milestone-linked tasks
- Test `task show --json` for non-milestone tasks (no lineage fields)
- Test `run status` displays lineage line for milestone-linked runs
- Test `run status --json` includes lineage fields

## Files to modify

- `src/cli/task.rs` — add --json flag to show
- `src/cli/project.rs` — add --json flag to show
- `src/cli/run.rs` — add lineage to status and history output, update view structs
- Tests in relevant test modules

## Acceptance Criteria
- task show --json includes milestone_id, bead_id, bead_title
- project show --json includes full ProjectDetail with task_lineage
- run status shows milestone/bead context for linked runs
- run status --json includes lineage fields
- Non-milestone tasks display normally (no lineage)
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

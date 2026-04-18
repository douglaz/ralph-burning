# Add integration tests for milestone store CRUD, journal, lifecycle, and linkage

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add new test files and test functions only. Do NOT modify existing source code.

## Background — what already exists

### Milestone store (`src/adapters/fs.rs`):
- `FsMilestoneStore` — create, load, update, list milestones
- `FsMilestoneJournalStore` — append-only journal of milestone events
- `FsMilestoneSnapshotStore` — status snapshot persistence
- `FsMilestonePlanStore` — plan.md + plan.json storage
- `FsTaskRunLineageStore` — bead→task-run linkage

### MilestoneRecord model (`src/contexts/milestone_record/model.rs`):
- `MilestoneRecord` with id, name, state, created_at, updated_at
- `MilestoneState`: Planning, Ready, Running, Paused, Completed, Failed
- Journal events: `MilestoneEvent` with type, actor, timestamp, payload

### Existing test infrastructure (`src/test_support/`):
- `TempWorkspaceBuilder` — creates temp workspaces
- `MilestoneFixtureBuilder` — seeds milestones with state
- `BeadGraphFixtureBuilder` — writes .beads/issues.jsonl

## What to implement

### Create `tests/unit/milestone_store_integration_test.rs` (or extend existing test files)

#### 1. Milestone CRUD tests (~8 tests):
- `create_milestone_writes_all_expected_artifacts` — verify milestone.toml, status.json, journal.ndjson on disk
- `load_existing_milestone_round_trips_all_fields` — write then read, compare
- `update_milestone_fields_persists_to_disk` — update name/description, reload, verify
- `list_milestones_returns_all_workspace_milestones` — create 3, list returns all 3
- `list_milestones_empty_workspace_returns_empty` — no milestones, empty list
- `load_missing_milestone_returns_clear_error` — missing directory, helpful error
- `load_corrupted_milestone_toml_fails_gracefully` — bad TOML, clear error
- `milestone_id_validation_rejects_invalid_chars` — bad ID chars rejected

#### 2. Journal event tests (~5 tests):
- `append_journal_event_writes_ndjson_line` — verify format
- `read_journal_returns_events_in_chronological_order`
- `journal_event_schema_has_required_fields` — timestamp, actor, type, payload
- `truncated_journal_file_loads_partial_events` — corruption recovery
- `corrupted_journal_line_is_skipped_or_errors_clearly`

#### 3. Lifecycle transition tests (~6 tests):
- `transition_planning_to_ready_succeeds` — valid transition
- `transition_ready_to_running_succeeds`
- `transition_completed_to_running_rejected` — invalid
- `transition_planning_to_completed_rejected` — skip-state rejected
- `lifecycle_transition_emits_journal_event` — verify event written
- `invalid_transition_returns_clear_error_message`

#### 4. Bead-to-task linkage tests (~4 tests):
- `record_task_run_for_bead_persists_linkage`
- `query_task_runs_for_bead_returns_all_attempts`
- `multiple_attempts_preserve_retry_history`
- `linkage_survives_milestone_reload`

#### 5. Recovery / atomicity tests (~2 tests):
- `atomic_write_prevents_partial_state` — simulate interrupted write, verify no partial state
- `temp_directory_isolation_between_tests` — no cross-contamination

### Test requirements
- All tests use temp dirs (tempfile::tempdir)
- Clear assertion messages on failure
- Use existing helpers from `src/test_support/`
- Total test time <5 seconds
- Don't modify source code — only add tests

## Files to create

- `tests/unit/milestone_store_integration_test.rs` (new) — or extend an existing test file if preferred
- Ensure the test module is discoverable (check `tests/unit/mod.rs` if exists, or top-level tests/)

## Acceptance Criteria
- ~25 integration tests covering CRUD, journal, lifecycle, linkage, recovery
- All tests pass in <5 seconds total
- Use temp dirs for isolation
- Clear assertion messages
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

# Implementation Response — Loop 16: Daemon Issue Watchers and Requirements Handoff

## Changes Made

### 1. Domain Model Extensions (`src/contexts/automation_runtime/model.rs`)
- Added `DispatchMode` enum (`Workflow`, `RequirementsDraft`, `RequirementsQuick`) with serde, Display, and `as_str()`.
- Added `WatchedIssueMeta` struct carrying `issue_ref`, `source_revision`, `title`, `body`, `labels`, and optional `routing_command`.
- Extended `DaemonTask` with three new fields: `dispatch_mode` (defaults to `Workflow` for backward compat), `source_revision: Option<String>`, `requirements_run_id: Option<String>`.
- Added `WaitingForRequirements` variant to `TaskStatus` with transitions: `Active→WaitingForRequirements`, `WaitingForRequirements→Pending|Failed|Aborted`.
- Added journal event types: `WatcherIngestion`, `RequirementsHandoff`, `RequirementsWaiting`, `RequirementsResumed`.

### 2. Watcher Ingestion Port (`src/contexts/automation_runtime/watcher.rs` — NEW)
- Defined `IssueWatcherPort` trait with `fn poll(&self, base_dir) -> AppResult<Vec<WatchedIssueMeta>>`.
- Implemented `parse_requirements_command(text) -> AppResult<Option<DispatchMode>>` — multiline-aware, extracts `/rb requirements draft|quick` from issue body.
- Implemented `resolve_dispatch_mode(meta) -> AppResult<DispatchMode>` — delegates to `parse_requirements_command` when `routing_command` is present.
- Implemented `resolve_watched_issue_flow(meta, routing_engine, default_flow)` — resolves flow through the standard routing engine.

### 3. File-Based Issue Watcher Adapter (`src/adapters/issue_watcher.rs` — NEW)
- `FileIssueWatcher` reads `.ralph-burning/daemon/watched/*.json` files, deserializing each as `WatchedIssueMeta`.
- `InMemoryIssueWatcher` for test scenarios, constructed with a fixed `Vec<WatchedIssueMeta>`.

### 4. Error Variants (`src/shared/error.rs`)
- `WatcherIngestionFailed { issue_ref, details }` — watcher poll or ingestion failure.
- `RequirementsHandoffFailed { task_id, details }` — requirements draft/quick execution failure.
- `DuplicateWatchedIssue { issue_ref, source_revision }` — rejected re-poll with different revision while non-terminal task exists.

### 5. Task Service Extensions (`src/contexts/automation_runtime/task_service.rs`)
- Extended `CreateTaskInput` with `dispatch_mode` and `source_revision` fields.
- `create_task_from_watched_issue` — idempotent ingestion keyed on `(issue_ref, source_revision)`. Generates deterministic `task_id` from `sha256(issue_ref + source_revision)[..12]`. Returns `Ok(None)` on idempotent re-poll, `Err(DuplicateWatchedIssue)` when a non-terminal task exists with a different revision.
- `mark_waiting_for_requirements(store, base_dir, task_id, requirements_run_id)` — transitions `Active→WaitingForRequirements`, sets `requirements_run_id`, clears `lease_id`.
- `resume_from_waiting(store, base_dir, task_id)` — transitions `WaitingForRequirements→Pending`, switches `dispatch_mode` to `Workflow`.

### 6. Requirements Drafting Daemon Helpers (`src/contexts/requirements_drafting/service.rs`)
- `read_requirements_run_status(store, base_dir, run_id)` — reads the run state.
- `is_requirements_run_complete(store, base_dir, run_id)` — checks if run status is `Completed`.
- `extract_seed_handoff(store, base_dir, run_id)` — extracts `SeedHandoff` struct with `project_id`, `project_name`, `flow`, `prompt_body`, `prompt_path`, and `recommended_flow`.

### 7. Daemon Loop Integration (`src/contexts/automation_runtime/daemon_loop.rs`)
- Added optional `watcher: Option<&'a dyn IssueWatcherPort>` and `requirements_store: Option<&'a dyn RequirementsStorePort>` with builder methods `with_watcher()` and `with_requirements_store()`.
- `process_cycle` now calls `poll_watchers()` then `check_waiting_tasks()` before processing pending tasks.
- `process_task` dispatches on `dispatch_mode`: `RequirementsQuick→handle_requirements_quick`, `RequirementsDraft→handle_requirements_draft`, `Workflow→` standard execution path.
- `handle_requirements_quick`: builds standalone `RequirementsService`, runs quick drafting, links `requirements_run_id`, extracts seed handoff, enforces routed flow over seed-recommended flow, creates project, executes workflow.
- `handle_requirements_draft`: builds `RequirementsService`, runs draft, transitions task to `WaitingForRequirements`.
- `check_waiting_tasks`: iterates waiting tasks, checks if linked requirements run is complete, resumes via `resume_from_waiting`.
- `build_requirements_service` helper creates a standalone `RequirementsService` with `StubBackendAdapter`.

### 8. CLI Wiring (`src/cli/daemon.rs`)
- Wired `FileIssueWatcher` and `FsRequirementsStore` into `handle_start`.
- Updated status output format to include `dispatch={}` and `requirements_run={}` columns.

### 9. Conformance Coverage
- Fixed stale `SC-PROJ-009` fixture: added missing `stage_cursor` to the `run.json` fixture inside `active_run`.
- Added `register_daemon_issue_intake` with 8 scenarios (`DAEMON-INTAKE-001` through `DAEMON-INTAKE-008`) in `scenarios.rs`.
- Created `tests/conformance/features/daemon_issue_intake.feature` with matching Gherkin (8 scenarios with `# DAEMON-INTAKE-NNN` ID comments).

### 10. Unit Tests (`tests/unit/automation_runtime_test.rs`)
- `task_state_machine_active_to_waiting`, `waiting_to_pending`, `waiting_to_failed`, `waiting_to_aborted`
- `watched_issue_ingestion_creates_task_idempotently`
- `watched_issue_newer_revision_after_terminal_creates_fresh_task`
- `watched_issue_different_revision_while_non_terminal_fails`
- `waiting_for_requirements_resume_transitions`, `resume_from_non_waiting_state_fails`
- `parse_requirements_command` tests (5 variants: draft, quick, unknown fails, no match, multiline)
- `dispatch_mode_display`, JSON roundtrip, backward-compat deserialization

### 11. Unit Tests (`tests/unit/requirements_drafting_test.rs`)
- `daemon_handoff` module: `is_requirements_run_complete_returns_false_for_drafting`, `returns_true_for_completed`, `read_requirements_run_status_returns_run`

### 12. E2E / CLI Tests (`tests/cli.rs`)
- Updated all 8 existing `DaemonTask` literals with new required fields (`dispatch_mode`, `source_revision`, `requirements_run_id`).
- Added: `daemon_status_shows_waiting_for_requirements_task`, `daemon_status_shows_dispatch_mode`, `daemon_abort_waiting_task_succeeds`.

## Could Not Address
- None. All spec requirements implemented.

## Test Results
- **372 tests pass, 0 failures** (97 CLI/E2E + 275 unit tests).
- `cargo check --lib --tests` passes with no warnings relevant to new code.

## Pending Changes (Pre-Commit)

### New files
- `src/adapters/issue_watcher.rs`
- `src/contexts/automation_runtime/watcher.rs`
- `tests/conformance/features/daemon_issue_intake.feature`

### Modified files
- `src/adapters/mod.rs`
- `src/cli/daemon.rs`
- `src/contexts/automation_runtime/daemon_loop.rs`
- `src/contexts/automation_runtime/mod.rs`
- `src/contexts/automation_runtime/model.rs`
- `src/contexts/automation_runtime/task_service.rs`
- `src/contexts/conformance_spec/scenarios.rs`
- `src/contexts/requirements_drafting/service.rs`
- `src/shared/error.rs`
- `tests/cli.rs`
- `tests/unit/automation_runtime_test.rs`
- `tests/unit/requirements_drafting_test.rs`

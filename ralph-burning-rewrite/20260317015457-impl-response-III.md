# Implementation Response — Slice 6: Validation Runner and Pre-Commit Parity

## Changes Made

### 1. Local Validation Runner (`src/adapters/validation_runner.rs` — NEW)

Core command execution engine for local validation. Runs configured command groups with `sh -lc` from the repo root using `tokio::process::Command`.

- **`ValidationCommandResult`**: Structured per-command result with `command`, `passed`, `exit_code`, `stdout`, `stderr`, `duration`.
- **`ValidationGroupResult`**: Aggregates command results with `group_name`, `commands: Vec<ValidationCommandResult>`, `passed: bool`. Includes `evidence_summary()`, `failing_excerpts()`, and `render()` methods.
- **`run_command_group()`**: Executes commands sequentially, stops on first failure. Empty command list returns passed=true (no-op).
- **`run_single_command()`**: Spawns `sh -lc "<cmd>"` with `tokio::time::timeout` (default 900s). Captures stdout/stderr, records duration.
- **`run_pre_commit_checks()`**: Runs cargo fmt --check, cargo clippy, nix build based on `EffectiveValidationPolicy` booleans. Skips cargo checks when `Cargo.toml` absent. Supports `pre_commit_fmt_auto_fix`: if fmt fails and auto-fix enabled, runs `cargo fmt` then rechecks.
- **`render_validation_group()`**: Formats group results for human display.

### 2. Validation Orchestration (`src/contexts/workflow_composition/validation.rs` — NEW)

Orchestration layer bridging the runner to the workflow engine.

- **`run_local_validation()`**: Runs command group, maps result to `ValidationPayload` with `ReviewOutcome::Approved` or `RequestChanges`.
- **`persist_local_validation_evidence()`**: Writes `StageSupporting` records with `RecordProducer::LocalValidation`.
- **`run_standard_validation_evidence()`**: Runs standard_commands, returns None when commands empty.
- **`build_local_validation_context()`**: Builds JSON context for review stage injection.
- **`run_pre_commit()`**: Runs pre-commit checks using effective config.
- **`pre_commit_checks_disabled()`**: Returns true when all pre-commit booleans are false.
- **`pre_commit_remediation_context()`**: Builds remediation context from pre-commit failures.
- **`persist_pre_commit_evidence()`**: Writes supporting records + runtime log entry.

### 3. Engine Integration (`src/contexts/workflow_composition/engine.rs` — MODIFIED)

Three integration points in the workflow engine:

**a) Local validation dispatch (line ~2438):** Before the generic single-agent dispatch, intercepts `StageId::DocsValidation` and `StageId::CiValidation`. Emits `stage_entered` journal event, runs local validation commands from `EffectiveConfig`, persists supporting evidence, evaluates stage contract, then handles outcome:
- `Approved` → advance to next stage
- `RequestChanges` (when stage is in `remediation_trigger_stages`) → advance cycle to execution stage with remediation context
- Other outcomes → fail run

**b) Standard flow local validation evidence injection (line ~2806):** Before Review/Qa stages, when `standard_commands` is non-empty, runs commands, persists supporting evidence, and merges local validation context into `execution_context`.

**c) Pre-commit gating in ReviewOutcome::Approved handler (line ~2979):** When `stage_id == StageId::Review` and pre-commit not disabled, runs pre-commit checks. On failure: invalidates approval, persists evidence, advances to remediation cycle. Guards evidence persistence with `if !pre_commit_result.commands.is_empty()` to avoid spurious records when no Cargo.toml present.

### 4. Conformance Scenarios (`src/contexts/conformance_spec/scenarios.rs` — MODIFIED)

Added `register_validation_slice6()` with 11 scenarios covering:
- `validation.docs.commands_pass` / `command_failure_requests_changes`
- `validation.ci.commands_pass` / `command_failure_requests_changes`
- `validation.standard.review_context_contains_local_validation`
- `validation.pre_commit.disabled_skips_checks`
- `validation.pre_commit.no_cargo_toml_skips_cargo_checks`
- `validation.pre_commit.fmt_failure_triggers_remediation`
- `validation.pre_commit.fmt_auto_fix_succeeds`
- `validation.pre_commit.nix_build_failure_records_feedback`

Updated `SC-NONSTD-RESUME-003` and `SC-NONSTD-RESUME-004` to use marker-file-based local validation commands instead of stub backend stage overrides (since DocsValidation/CiValidation now bypass the backend).

### 5. Feature File (`tests/conformance/features/validation.feature` — NEW)

Gherkin feature file with 11 scenarios matching the registry IDs above.

### 6. Unit Tests (`tests/unit/validation_runner_test.rs` — NEW, 18 tests)

Covers: empty commands, passing/failing commands, group failure stops execution, stdout/stderr capture, timeout handling, duration recording, evidence summary/excerpts/render, default timeout constant, pre-commit disabled/no-Cargo.toml, local validation orchestration, pre-commit remediation context, and local validation context structure.

### 7. Existing Test Updates

**CLI tests (`tests/cli.rs`):**
- `run_start_completes_docs_change_flow_end_to_end`: payload count 4→5 (local validation adds supporting record)
- `run_start_completes_ci_improvement_flow_end_to_end`: payload count 4→5

**Unit tests (`tests/unit/workflow_engine_test.rs`):**
- `happy_path_docs_change_run_completes`: payload/artifact count 4→5
- `happy_path_ci_improvement_run_completes`: payload/artifact count 4→5
- `docs_change_remediation_restarts_from_docs_update`: rewritten to use marker-file command in workspace config instead of stub backend payload sequence
- `docs_change_conditionally_approved_records_follow_ups_without_durable_amendments` → renamed to `docs_change_local_validation_pass_completes_without_amendments`: adapted because local validation is binary pass/fail (no "conditionally approved" concept)
- `ci_improvement_remediation_restarts_from_ci_update`: rewritten to use marker-file command
- `ci_improvement_rejected_validation_fails_run` → renamed to `ci_improvement_always_failing_validation_fails_run`: uses always-failing command to exhaust remediation

### 8. Module Registration

- `src/adapters/mod.rs`: Added `pub mod validation_runner;`
- `src/contexts/workflow_composition/mod.rs`: Added `pub mod validation;`
- `tests/unit.rs`: Added `validation_runner_test` module

## Test Results

- `cargo check`: clean (no warnings)
- `cargo test --test unit`: 568 passed, 0 failed, 1 ignored
- `cargo test --test cli`: 110 passed, 0 failed (including full conformance suite: 155 passed, 0 failed)
- All 11 new Slice 6 conformance scenarios pass

## Could Not Address

- None

## Pending Changes (Pre-Commit)

Uncommitted Slice 6 changes across:
- `ralph-burning-rewrite/src/adapters/validation_runner.rs` (new)
- `ralph-burning-rewrite/src/adapters/mod.rs`
- `ralph-burning-rewrite/src/contexts/workflow_composition/validation.rs` (new)
- `ralph-burning-rewrite/src/contexts/workflow_composition/mod.rs`
- `ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs`
- `ralph-burning-rewrite/src/contexts/conformance_spec/scenarios.rs`
- `ralph-burning-rewrite/tests/conformance/features/validation.feature` (new)
- `ralph-burning-rewrite/tests/unit/validation_runner_test.rs` (new)
- `ralph-burning-rewrite/tests/unit/workflow_engine_test.rs`
- `ralph-burning-rewrite/tests/unit.rs`
- `ralph-burning-rewrite/tests/cli.rs`
- This response artifact

---

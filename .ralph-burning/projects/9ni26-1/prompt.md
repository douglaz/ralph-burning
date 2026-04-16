# Add structured tracing spans and log fields for milestone, controller, and bead operations

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add tracing instrumentation to existing functions. Do NOT restructure code, change signatures, or refactor logic. Only add `#[instrument]` attributes, `info_span!` blocks, and structured `tracing::info!/warn!/error!/debug!` calls.

## Background — what already exists

The codebase already uses `tracing` (v0.1) and `tracing-subscriber` (v0.3). Some files already have ad-hoc `tracing::warn!` and `tracing::debug!` calls but lack structured spans.

### Existing tracing patterns in the codebase (follow these exactly):

```rust
// Pattern 1: Operation tracking with structured fields
tracing::info!(
    operation = "sync_flush",
    outcome = "success",
    duration_ms = duration_ms,
    "br sync flush completed"
);

// Pattern 2: Error/warning with context fields
tracing::warn!(
    bead_id = bead_id,
    task_id = task_id,
    error = %e,
    "failed to post planned-elsewhere comment (non-blocking)"
);

// Pattern 3: Debug for non-blocking failures
tracing::debug!(
    error = %e,
    "bv --robot-next hint capture failed (non-blocking)"
);
```

### Standard structured fields (use these names consistently):
- `milestone_id` — milestone identifier
- `bead_id` — bead identifier
- `task_id` — project/run identifier
- `operation` — human-readable operation name
- `stage` — workflow stage name
- `outcome` — "success" or "failure"
- `duration_ms` — execution time in milliseconds
- `error` — error details (use `%e` format)

## What to implement

### 1. Add `#[instrument]` to key functions in `src/contexts/milestone_record/controller.rs`

This file has 0 tracing calls. Add `#[instrument]` with `skip` for non-Display types and structured fields for IDs. Target these functions:

- `initialize_controller()` (~line 264) — fields: milestone_id, bead_id
- `initialize_controller_with_state()` (~line 282) — fields: milestone_id, bead_id
- `initialize_controller_with_request()` (~line 301) — fields: milestone_id
- `sync_controller_task_claimed()` (~line 318) — fields: milestone_id, bead_id, task_id
- `sync_controller_task_running()` (~line 395) — fields: milestone_id, bead_id, task_id
- `sync_controller_task_reconciling()` (~line 476) — fields: milestone_id, bead_id
- `load_controller()` (~line 715) — fields: milestone_id
- `transition_controller()` (~line 729) — fields: milestone_id
- `checkpoint_controller_stop()` (~line 769) — fields: milestone_id
- `resume_controller()` (~line 783) — fields: milestone_id

For each, add an `tracing::info!` on success with `operation = "function_name"` and `outcome = "success"`. On error paths, the existing `?` propagation is fine — the caller's span will capture the error context.

### 2. Add tracing spans to key functions in `src/contexts/milestone_record/service.rs`

This file is large (14k+ lines). It already has 5 `tracing::warn!` calls in the propose-new-bead area. Add structured tracing to these key operations:

**Milestone CRUD** — add `#[instrument]` or manual spans:
- `create_milestone()` (~line 1316) — fields: milestone_id; log INFO on creation
- `load_milestone()` (~line 1469) — fields: milestone_id; DEBUG level
- `load_snapshot()` (~line 1478) — fields: milestone_id; DEBUG level
- `list_milestones()` (~line 1487) — DEBUG level
- `update_status()` (~line 1495) — fields: milestone_id; INFO on transition

**Bead tracking** — add spans with bead_id and task_id:
- `record_bead_start()` (~line 1898) — fields: milestone_id, bead_id, task_id; INFO level
- `record_bead_completion()` (~line 1995) — fields: milestone_id, bead_id, task_id; INFO level
- `record_bead_completion_with_disposition()` (~line 2029) — fields: milestone_id, bead_id, task_id; INFO level

**Journal operations** — add DEBUG spans:
- `read_journal()` (~line 2064) — fields: milestone_id
- `read_bead_lineage()` (~line 2260) — fields: milestone_id, bead_id

**Task operations**:
- `list_tasks_for_milestone()` (~line 2373) — fields: milestone_id; DEBUG
- `update_task_run()` (~line 2440) — fields: milestone_id, task_id; INFO
- `repair_task_run()` (~line 2568) — fields: milestone_id, task_id; INFO

**Propose-new-bead** (already has some tracing, enhance with spans):
- `handle_propose_new_bead()` (~line 3070) — add an `info_span!("handle_propose_new_bead", bead_id, milestone_id)` at entry

### 3. Add tracing to `src/adapters/br_health.rs`

This file has 0 tracing calls. Add:
- `check_beads_health()` (~line 19) — INFO on healthy, WARN on conflict markers or missing file
- Include the `base_dir` path in the log fields

### 4. Enhance tracing in `src/adapters/br_process.rs`

This file already has 19 tracing calls. Enhance with spans:

**Core execution methods** — add `#[instrument]` or entry spans:
- `exec_read()` (~line 654) — add span with `operation` field showing the command
- `exec_mutation()` (~line 674) — add span with `operation` field; INFO on success with duration
- `exec_json()` (~line 694) — DEBUG span

**Mutation methods** — add INFO-level success logging:
- `create_bead()` (~line 1238) — log bead creation with title
- `update_bead_status()` (~line 1258) — log status change with bead_id, new status
- `close_bead()` (~line 1265) — log bead closure with bead_id
- `add_dependency()` (~line 1272) — log with bead_id, dependency_id
- `sync_flush()` (~line 1308) — already has some, ensure span wraps it
- `sync_import()` (~line 1509) — add INFO on success

### 5. Add entry span to `src/contexts/automation_runtime/success_reconciliation.rs`

This file already has 20 tracing calls. Add:
- `reconcile_success()` (~line 180) — add an `info_span!("reconcile_success", milestone_id, bead_id, task_id)` wrapping the function body
- Log summary at end: number of amendments processed, beads created, planned-elsewhere mappings

## Implementation guidance

- Use `#[tracing::instrument(skip_all, fields(milestone_id = %milestone_id, ...))]` for functions with complex parameters
- For functions taking port/adapter trait objects, always use `skip_all` and manually specify the fields you want
- Don't instrument simple getters, private helpers, or test code
- Don't add spans to functions that are called in tight loops (e.g., per-journal-event iteration)
- Keep existing tracing calls — enhance, don't replace
- The `tracing` crate is already in Cargo.toml, no dependency changes needed

## Files to modify

- `src/contexts/milestone_record/controller.rs` — add spans and #[instrument] (currently 0 tracing)
- `src/contexts/milestone_record/service.rs` — add spans to CRUD/bead/journal ops (currently 5 tracing calls)
- `src/adapters/br_process.rs` — enhance with spans on core methods (currently 19 tracing calls)
- `src/adapters/br_health.rs` — add health check tracing (currently 0 tracing)
- `src/contexts/automation_runtime/success_reconciliation.rs` — add entry span (currently 20 tracing calls)

## Acceptance Criteria
- All functions listed above have structured tracing spans or log calls
- Structured fields (milestone_id, bead_id, task_id, operation) used consistently
- Log levels are meaningful: ERROR for failures, WARN for degraded, INFO for operations, DEBUG for internals
- No existing tracing calls removed
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

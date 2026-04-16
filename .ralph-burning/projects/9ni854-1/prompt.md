# Handle failed task outcomes and retry-or-needs-operator reconciliation

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add the failure reconciliation handler. Do NOT delete or restructure existing code.
Follow the patterns established by the existing success reconciliation in `success_reconciliation.rs`.

## Background — what already exists

The codebase has substantial infrastructure for this already:

### Controller state machine (`src/contexts/milestone_record/controller.rs`):
- `MilestoneControllerState` enum with `Idle, Selecting, Claimed, Running, Reconciling, Blocked, NeedsOperator, Completed`
- `ControllerTaskStatus` enum: `Pending, Running, Succeeded, Failed, Missing`
- Failed tasks currently route to generic `Reconciling` state (same path as success) — no dedicated failure handler

### Task run model (`src/contexts/milestone_record/model.rs`):
- `TaskRunOutcome`: `Pending, Running, Succeeded, Failed, Skipped` (line 725)
- `TaskRunEntry` has `outcome_detail: Option<String>` for failure reasons
- `is_terminal()` returns true for `Succeeded | Failed | Skipped`
- No explicit retry counter — retries tracked via multiple `TaskRunEntry` rows per bead
- `collapse_task_run_attempts()` groups entries by `(milestone_id, bead_id, project_id, run_id)`

### Journal events (`src/contexts/milestone_record/model.rs` line 291):
- `MilestoneEventType::BeadFailed` exists but is not used in reconciliation flows
- Event structure has `details` field for failure context

### Progress tracking:
- `MilestoneProgress.failed_beads` counter exists

### Success reconciliation (`src/contexts/automation_runtime/success_reconciliation.rs`):
- `reconcile_success()` is the main entry point (~line 180)
- Uses port traits: `MilestoneJournalPort`, `MilestoneQueryPort`, `BrMutationPort`, `BrQueryPort`
- Closes beads via `BrCommand::close()`, syncs with `sync_flush()`, records `BeadCompleted` journal event
- This is the template to follow for failure reconciliation

## What to implement

### 1. Create `src/contexts/automation_runtime/failure_reconciliation.rs`

Create a `reconcile_failure()` function that:

1. **Records the failure in milestone state**:
   - Call `record_bead_completion_with_disposition()` or equivalent with `TaskRunOutcome::Failed`
   - Include `outcome_detail` (error message/summary) from the failed task run
   - Append a `BeadFailed` journal event with bead_id, task_id, attempt number, and error summary

2. **Leaves the bead open** — do NOT close the bead or call `br close`. Failed execution should not mutate bead state. The bead stays in_progress for retry.

3. **Counts prior attempts** for this bead:
   - Query task run entries for this bead using existing `collapse_task_run_attempts()` or `find_runs_for_bead()`
   - Count how many terminal `Failed` outcomes exist for this bead

4. **Decides retry vs needs_operator**:
   - If attempt count < MAX_RETRIES (use a constant, default 3): transition controller to a retryable state (could reuse `Blocked` with a reason, or stay in `Reconciling` with retry metadata)
   - If attempt count >= MAX_RETRIES: transition controller to `NeedsOperator` with a clear reason like "bead {bead_id} failed {n} times: {last_error}"

5. **Logs the failure** with structured tracing:
   - `tracing::warn!` on each failure with bead_id, task_id, attempt_number, error_summary
   - `tracing::error!` when escalating to NeedsOperator

6. **Is idempotent**: calling `reconcile_failure()` twice for the same failed run should not double-count attempts or create duplicate journal events. Check if the failure was already recorded before recording.

### 2. Wire into the dispatch in `src/contexts/automation_runtime/mod.rs`

Register the new module and make `reconcile_failure` accessible. Follow the pattern used for `success_reconciliation`.

### 3. Add unit tests

Test these scenarios:
- First failure: records journal event, counts 1 attempt, controller stays retryable
- Second failure: records, counts 2, still retryable  
- Third failure (at MAX_RETRIES): escalates to NeedsOperator
- Idempotency: calling twice with same run doesn't double-count
- Bead is NOT closed on failure
- Journal event contains bead_id, error detail

### Function signature guidance

Follow the dependency injection pattern from `reconcile_success()`:
```rust
pub async fn reconcile_failure(
    milestone_id: &str,
    bead_id: &str,
    task_id: &str,
    run_id: &str,
    error_summary: &str,
    journal_port: &dyn MilestoneJournalPort,
    query_port: &dyn MilestoneQueryPort,
    // ... other ports as needed
) -> Result<FailureReconciliationOutcome>
```

Return an enum:
```rust
pub enum FailureReconciliationOutcome {
    Retryable { attempt_number: u32, max_retries: u32 },
    EscalatedToOperator { attempt_number: u32, reason: String },
}
```

## Files to modify

- `src/contexts/automation_runtime/failure_reconciliation.rs` (new) — main handler
- `src/contexts/automation_runtime/mod.rs` — register module
- Tests in the new file or test module

## Acceptance Criteria
- Failed tasks are recorded in journal with BeadFailed event
- Bead is NOT closed on failure (stays open for retry)
- Attempt counting works across multiple failures
- Escalation to NeedsOperator after MAX_RETRIES
- Idempotent: duplicate calls don't double-count
- Structured tracing on failures
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

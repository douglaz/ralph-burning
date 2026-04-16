# Handle propose-new-bead outcomes with conservative thresholds and dependency injection

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add the missing `ProposeNewBead` handler. Do NOT delete or restructure existing code.
Follow the patterns established by the existing `PlannedElsewhere` handler.

## Background — what already exists

The reconciliation infrastructure is already in place:

- `FindingClassification::ProposeNewBead { finding_summary, proposed_title, proposed_scope, severity, rationale }` in `src/contexts/workflow_composition/review_classification.rs`
- `AmendmentClassification::ProposeNewBead` (mirrors FindingClassification) in `src/contexts/workflow_composition/panel_contracts.rs`
- `PlannedElsewhere` handler: `record_planned_elsewhere_mapping()` in `src/contexts/milestone_record/service.rs` — use as a template
- `BrCommand::create(title, bead_type, priority)` in `src/adapters/br_process.rs:232` — programmatic bead creation
- `BrAdapter` / `BrMutationAdapter` in `src/adapters/br_process.rs` — CLI wrapper with write-lock serialization
- `MilestoneJournalPort::append_event()` for recording journal events
- Amendment dispatch loop in `src/contexts/automation_runtime/success_reconciliation.rs` around line 1005 — iterates `final_accepted_amendments` and dispatches by classification. `ProposeNewBead` case is missing.

## What to implement

### 1. Add `ProposeNewBead` handler function in `src/contexts/milestone_record/service.rs`

Create a function like `handle_propose_new_bead()` that:
1. Defensive re-check: search existing beads for a match (use `BrAdapter` to query). If found, reclassify as planned-elsewhere and delegate to `record_planned_elsewhere_mapping()`
2. Create the new bead via `BrCommand::create()` with:
   - Title from `proposed_title`
   - Description from `finding_summary` + `rationale`
   - Priority derived from `severity`
   - Labels consistent with milestone context
3. Run `br sync --flush-only` after creation
4. Record creation in milestone journal with:
   - What finding triggered it
   - Why no existing bead matched
   - The new bead ID

### 2. Add conservative thresholds

- Log a `tracing::warn!` whenever a new bead is created
- Track count of beads created per reconciliation pass. If count exceeds 2, log an error-level warning that scope may be wrong — but still proceed (don't block)

### 3. Wire into the dispatch loop in `success_reconciliation.rs`

Add the `ProposeNewBead` match arm in the amendment processing loop (around line 1005 where `PlannedElsewhere` is handled). Call the new handler function.

### 4. Add a journal event type

Add `ProposedBeadCreated` (or similar) to the milestone journal event types if not already present. Follow the pattern of `PlannedElsewhereMapped`.

### 5. Unit tests

Add tests that verify:
- New bead creation with correct fields
- Defensive re-check finding existing bead → reclassifies to planned-elsewhere
- Conservative threshold warning when too many beads created
- Journal event recording

## Files to modify

- `src/contexts/milestone_record/service.rs` — add handler function
- `src/contexts/automation_runtime/success_reconciliation.rs` — wire dispatch
- `src/contexts/milestone_record/model.rs` — add journal event type if needed
- Tests in relevant test modules

## Acceptance Criteria
- ProposeNewBead findings create beads via `br` CLI adapter
- Defensive re-check prevents duplicates
- Conservative threshold: warn at >2 new beads per pass
- Journal records each creation with evidence
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

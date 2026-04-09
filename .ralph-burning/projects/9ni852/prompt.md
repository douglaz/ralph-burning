## Bead ID: ralph-burning-9ni.8.5.2

## Goal

Handle planned-elsewhere findings and existing-bead mappings during reconciliation. When the review stage classifies a finding as planned-elsewhere (valid concern but already covered by another bead), record the mapping and allow the active bead to proceed toward completion.

## Context

The success reconciliation handler (`src/contexts/automation_runtime/success_reconciliation.rs`) already handles bead close, sync, and milestone state updates. This bead adds handling for planned-elsewhere findings that come from the review stage.

When reconciliation encounters a planned-elsewhere finding:
1. Look up the existing bead that owns the concern (by bead_id from the classification)
2. Record a mapping: (active_bead_id, finding_summary, mapped_to_bead_id) in milestone state
3. Optionally add a comment on the mapped-to bead via `br comment` so the future implementer sees the context
4. Allow the active bead to proceed toward completion without reopen/fix loops
5. Record the mapping in the milestone journal for audit

The milestone journal and state model already exist in `src/contexts/milestone_record/`. The `br` adapter in `src/adapters/br_process.rs` provides the interface for `br comment` operations.

## Acceptance Criteria

- A `PlannedElsewhereMapping` data structure records (active_bead_id, finding_summary, mapped_to_bead_id)
- Mappings are persisted in milestone state (e.g., a `planned_elsewhere.ndjson` file alongside the milestone journal)
- Mappings are also recorded as milestone journal events for audit
- Active bead can still complete without reopen/fix loops when findings are planned-elsewhere
- Stale bead references (mapped-to bead doesn't exist) fall back gracefully with a warning
- Mappings survive controller restart (durable persistence)
- Optional `br comment` on the mapped-to bead includes the finding summary and source bead context
- Deterministic tests cover: successful mapping, stale bead fallback, journal recording, and persistence across reload
- Existing tests pass

## Non-Goals

- Deciding whether a finding is planned-elsewhere vs fix-now (that is the review stage's job)
- Creating new beads for genuinely missing work (covered by 9ni.8.5.3)
- Changes to the review classification logic itself

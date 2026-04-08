## Bead ID: ralph-burning-9ni.8.5.1

## Goal

Implement the happy-path reconciliation handler that runs after a bead task completes successfully.

When a bead-linked task finishes without errors:
1. Close the bead in br (br close <bead_id>) with a success reason
2. Run br sync --flush-only to persist the mutation
3. Update milestone state: advance progress, record the completion event in the journal
4. Capture next-step hints from bv (if available) so the controller knows what to select next
5. Record the task-to-bead linkage outcome (task_id, bead_id, outcome=success, timestamp)

## Key Constraints

- The close must be idempotent: re-running after a crash should not produce duplicate mutations
- If br close fails (network, tool missing), leave the bead open and transition the controller to a needs_operator state rather than silently swallowing the error
- Sync must happen after close, not before
- Next-step hints are informational, not binding; the selection step uses them as input

## Non-Goals

- Failure or retry handling (covered by 9ni.8.5.4)
- New bead creation policy (covered by 9ni.8.6)

## Acceptance Criteria

- Completed bead is closed in br with a success reason
- Sync occurs after close
- Milestone progress updates and journal records the completion event with bead_id and task_id
- Next-step hints are captured from bv output when available
- Close operation is idempotent on re-run
- br close failure transitions controller to needs_operator state
- Existing tests pass; new tests cover the reconciliation handler

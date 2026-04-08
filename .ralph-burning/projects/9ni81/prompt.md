## Bead ID: ralph-burning-9ni.8.1

## Goal

Make the milestone controller an explicit state machine with durable persistence, rather than a loose script that loses track of progress on restart.

## Controller States

- idle: no active milestone execution
- selecting: querying bv for next bead, validating against br readiness
- claimed: bead has been claimed in br, Ralph task is being created
- running: bead-linked task is executing through quick_dev
- reconciling: task completed, processing outcome back into .beads and milestone state
- blocked: bv returned no ready bead, or validation failed; awaiting graph changes
- needs_operator: unrecoverable error or ambiguous state requiring human intervention
- completed: all beads closed, milestone is done

## Journal Events

- Each state transition emits a journal event with (timestamp, from_state, to_state, bead_id, task_id, reason)
- Events are append-only NDJSON for crash recovery and audit

## Stop/Resume Semantics

- On stop (SIGTERM, user interrupt): persist current state and active bead/task identifiers
- On resume: read persisted state, validate it against current .beads and milestone artifacts, re-enter the correct state without re-doing completed work
- If persisted state is stale (bead was closed externally), detect the divergence and transition to needs_operator instead of blindly resuming

## Key Constraints

- State transitions must be validated: only legal transitions are allowed
- All state is persisted to .ralph-burning/milestones/<id>/controller.json
- The state machine is testable in isolation with mock adapters

## Non-Goals

- Parallel execution (deferred to phase 2)
- Bead selection logic (covered by 9ni.8.2)
- Task execution orchestration (covered by 9ni.8.4)

## Acceptance Criteria

- Controller persistence is explicit via controller.json
- All state transitions are validated and emit journal events
- Operator can stop and resume without losing track of active bead/task
- Resume detects stale state and transitions to needs_operator
- State machine is testable in isolation with mock adapters
- Existing tests pass; new tests cover state transitions, persistence, and resume

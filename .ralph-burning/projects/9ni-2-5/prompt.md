Implement bead ralph-burning-9ni.2.5: Add milestone lifecycle transitions and journal events.

Goal:
Implement milestone lifecycle state transitions with journal event emission so the system can track a milestone from planning through completion.

Lifecycle states:
- planning: requirements pipeline is active, plan not yet finalized
- ready: plan is complete, beads are exported, execution can begin
- running: controller is actively executing beads
- paused: execution was stopped by user or error, can be resumed
- completed: all beads are closed, milestone is done
- failed: unrecoverable error, requires operator intervention

Transition rules:
- planning -> ready
- ready -> running
- running -> paused
- running -> completed
- running -> failed
- paused -> running
- Reject invalid transitions with descriptive errors.

Journal events:
- Each transition emits {timestamp, from_state, to_state, actor, reason, metadata}
- Append to the milestone journal in NDJSON format.
- Metadata varies by transition, e.g. completed includes bead count and duration.

Key constraints:
- Transitions must be atomic: state file and journal event written together.
- Invalid transitions must produce descriptive errors.
- Transition logic must be testable in isolation without filesystem dependency.

Non-goals:
- Controller state machine internals
- Bead-to-task linkage
- Status aggregation queries

Acceptance criteria:
- All valid lifecycle transitions are implemented and produce journal events.
- Invalid transitions are rejected with descriptive error messages.
- Transitions are atomic.
- Transition logic is testable in isolation without filesystem.
- Journal events include timestamp, from/to state, actor, reason, and metadata.

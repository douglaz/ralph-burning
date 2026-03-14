---
artifact: final-review-votes
loop: 8
project: process-backend-adapter
backend: claude
role: final_reviewer
created_at: 2026-03-14T22:30:17Z
---

I now have sufficient context. The `history_record_base_id` at line 208 already includes `completion_round`, but the `invocation_id` at line 1957 does not — confirming the inconsistency.

# Vote Results

## Amendment: PB-CANCEL-TIMEOUT-HANG

### Vote
ACCEPT

### Rationale
The code path is clear and the bug is real. `ManagedChild::wait()` (process_backend.rs:58) is an unbounded `try_wait()` poll loop with only a 10ms sleep between iterations (line 80) and no deadline. `cancel()` (line 614) calls `send_sigterm()` then immediately `active_child.wait()` (line 629), blocking until the child actually exits. The service layer (service.rs:196-208) awaits `cancel()` in both the cancellation-token and timeout branches before returning — so a TERM-ignoring child makes the entire timeout/cancel path hang indefinitely, defeating the "authoritative timeout" guarantee. The planner's analysis is thorough and correct. The proposed fix (best-effort non-blocking cancel with a bounded grace period and forced cleanup) is the right shape. Adding a TERM-ignoring test child is straightforward and would prevent regression.

## Amendment: PB-CODEX-RESUME-SCHEMA

### Vote
REJECT

### Rationale
I agree with the planner's rejection. The code intentionally omits `--output-schema` on the resume path (process_backend.rs:413-425), and the unit test explicitly asserts this behavior. The planner's key finding — that `codex exec resume` does not accept `--output-schema` — means the proposed change would produce an invalid CLI invocation that breaks resume entirely. Furthermore, the agent-execution service still validates stage outputs against the contract after invocation (service.rs:236-241), so resumed sessions are not actually "unconstrained." The amendment misidentifies correct behavior as a bug.

## Amendment: WF-INVOCATION-ID-COLLISION

### Vote
ACCEPT

### Rationale
The collision is demonstrable from the code. The invocation ID format at engine.rs:1957 is `{run_id}-{stage_id}-c{cycle}-a{attempt}`, which omits `completion_round`. Meanwhile, `advance_completion_round()` (domain.rs:549) increments only `completion_round` and resets `attempt` to `1` while keeping `cycle` unchanged (confirmed by the comment at engine.rs:1537). So round-2 of the same stage with the same cycle produces an identical `invocation_id` as round 1.

This causes real data loss: `FsRawOutputStore::persist_raw_output` (fs.rs:724-734) writes to `{invocation_id}.raw` via `write_atomic` (fs.rs:136), which silently replaces any prior file at that path. Codex temp files (schema and last-message at process_backend.rs:436-437) also derive from `invocation_id`, creating aliasing hazards.

The fix is consistent with existing practice — `history_record_base_id` at engine.rs:208 already uses a 5-part format including `cr{completion_round}`. The invocation ID should simply follow the same pattern. This is a straightforward correctness fix with low risk.

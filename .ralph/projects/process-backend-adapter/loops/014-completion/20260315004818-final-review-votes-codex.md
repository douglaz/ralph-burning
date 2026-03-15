---
artifact: final-review-votes
loop: 14
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-15T00:48:18Z
---

I’m checking the cited adapter, service, engine, and retry-policy paths to validate each amendment against the current source before voting.
# Vote Results

## Amendment: RB-REVIEW-001

### Vote
ACCEPT

### Rationale
The planner’s position is correct. `check_availability()` currently accepts any regular file named `claude` or `codex` on `PATH` because it only checks `metadata.is_file()` in [process_backend.rs:588](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L588), [process_backend.rs:596](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L596), and [process_backend.rs:603](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L603). Real executability is only enforced later at `spawn()` in [process_backend.rs:201](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L201).

The consequence is material, not theoretical. Availability is checked again inside [service.rs:166](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L166), after the engine has already written `stage_entered` and updated the run snapshot in [engine.rs:1706](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1706), [engine.rs:1740](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1740), and [engine.rs:1818](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1818). Preflight should only pass for binaries that are actually runnable.

## Amendment: RB-REVIEW-002

### Vote
ACCEPT

### Rationale
The planner is right that cancellation currently depends on an external `kill` resolved from `PATH`. `send_signal()` shells out via `Command::new("kill")` in [process_backend.rs:677](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L677), and “process already gone” is detected by matching stderr text rather than OS error semantics in [process_backend.rs:687](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L687).

That would already be brittle on its own, but it matters more because cancel failures are ignored in the timeout and cancellation branches at [service.rs:196](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196) and [service.rs:204](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L204). Replacing the shell-out with an in-process signal API and treating `ESRCH` as success makes cleanup deterministic and removes `PATH`-dependent failure modes from a critical control path.

## Amendment: RB-REVIEW-003

### Vote
ACCEPT

### Rationale
The current failure classification is incorrect. A missing or unreadable Codex `--output-last-message` file is an IO/transport problem, but the adapter maps that read failure to `SchemaValidationFailure` in [process_backend.rs:517](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L517) and [process_backend.rs:523](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L523). Actual schema/JSON validation only begins afterward in the `serde_json::from_str(...)` path at [process_backend.rs:529](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L529).

This is behaviorally significant because retry handling distinguishes those classes in [retry_policy.rs:34](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L34), [retry_policy.rs:36](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L36), and [retry_policy.rs:38](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L38). Reclassifying file-read failures as `TransportFailure` preserves a meaningful distinction between “Codex never produced the file” and “Codex produced invalid JSON.”

---
artifact: final-review-votes
loop: 6
project: process-backend-adapter
backend: claude
role: final_reviewer
created_at: 2026-03-14T21:24:43Z
---

I've reviewed all the relevant code. Here are my votes:

# Vote Results

## Amendment: PBA-REV-001

### Vote
ACCEPT

### Rationale
The code at `process_backend.rs:369-378` unconditionally appends `--output-schema` and `--output-last-message` to the argv regardless of whether the command is `exec` or `exec resume`. The planner independently confirmed that the installed `codex` CLI rejects `--output-schema` on `exec resume`. The resume path is reachable in production (lines 359-360 gate on `SessionPolicy::ReuseIfAllowed` plus a prior session, which the service wires up). The existing tests mask this because the stub script does not validate the full argv shape for resume invocations. This is a straightforward, real bug — any workflow that attempts session reuse will crash before the subprocess starts. The proposed fix (split argv construction for new-session vs resume) is the obvious correct approach with minimal blast radius.

## Amendment: PBA-REV-002

### Vote
ACCEPT

### Rationale
The defect is real and the planner's nuanced analysis is correct. The adapter stores only bare `u32` PIDs (`process_backend.rs:23`), while the actual `tokio::process::Child` handle lives on the stack inside `spawn_and_wait` (`process_backend.rs:162-163`). When the service-layer `tokio::select!` at `service.rs:196` takes the cancellation or timeout branch, it calls `cancel()` (which sends fire-and-forget `SIGTERM` via `kill` at line 556-559), then returns immediately. This drops the pinned invoke future, which drops the `Child`. The planner correctly notes that Tokio 1.50 does have best-effort orphan reaping, so the "no reap path at all" claim in the amendment is slightly overstated — but the fundamental problem stands: cancellation has no deterministic confirmation that the child has actually exited, and the bare-PID approach carries a PID-reuse hazard. Storing the child handle and performing signal-then-wait is a genuine robustness improvement, especially in long-running daemon mode where accumulated zombie or orphan processes compound over time. The proposed change is well-scoped and addresses a real operational concern.

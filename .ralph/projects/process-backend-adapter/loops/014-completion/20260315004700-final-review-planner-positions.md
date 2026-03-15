---
artifact: final-review-planner-positions
loop: 14
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-15T00:47:00Z
---

I’m verifying each amendment against the current source, starting with the adapter and workflow files it cites so I can judge whether the reported behavior is real.
I’ve confirmed the cited lines exist. Next I’m checking the spawn path and where execution state changes happen, because the first amendment depends on preflight succeeding before a later runtime failure.
The first issue hinges on whether preflight runs before the workflow records a started run. I’m reading that section now, plus the adapter’s spawn path, to separate a real correctness gap from an overstated consequence.
I’ve verified the first two code paths. I’m checking how failure classes propagate into retry handling before I decide whether the third amendment is just taxonomy or a real control-flow defect.
I’ve got enough to judge the first two. For the third, I’m reading the Codex output-file setup to confirm that a missing last-message file really is a transport/IO failure rather than a schema parse path.
# Planner Positions

## Amendment: RB-REVIEW-001

### Position
ACCEPT

### Rationale
The defect is real. `check_availability()` currently treats any `PATH` entry named `claude` or `codex` as available if `std::fs::metadata(...).is_file()` succeeds, with no executable check or actual executable resolution: [process_backend.rs:588](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L588), [process_backend.rs:596](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L596), [process_backend.rs:603](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L603). `Command::new(binary).spawn()` is the first place that will reject a non-executable placeholder: [process_backend.rs:201](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L201), [process_backend.rs:210](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L210).

The amendment’s consequence is also materially correct. Although startup preflight runs before the initial run snapshot is written, the same flawed availability check is repeated inside `AgentExecutionService::invoke()`: [service.rs:166](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L166). By the time stage execution reaches that call, the engine has already appended `stage_entered` and updated the run snapshot: [engine.rs:1706](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1706), [engine.rs:1740](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1740), [engine.rs:1818](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1818). So a non-executable file can pass availability and still fail later during spawn, after durable state mutation has begun.

## Amendment: RB-REVIEW-002

### Position
ACCEPT

### Rationale
This is a genuine robustness and safety issue. Cancellation ultimately sends signals by spawning `kill` from `PATH`: [process_backend.rs:53](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L53), [process_backend.rs:61](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L61), [process_backend.rs:677](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L677). That makes cleanup depend on environment resolution rather than an in-process OS signal API, and the current success handling for “process already gone” is a stderr string match on `"No such process"`: [process_backend.rs:687](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L687).

The error suppression cited in the amendment also exists. Both the explicit cancellation branch and the timeout branch ignore `adapter.cancel()` failures: [service.rs:196](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196), [service.rs:204](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L204). Since `cancel()` depends on that external `kill` path: [process_backend.rs:633](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L633), [process_backend.rs:648](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L648), a missing or shadowed `kill` can indeed prevent deterministic subprocess cleanup without surfacing the failure.

## Amendment: RB-REVIEW-003

### Position
ACCEPT

### Rationale
The current classification is wrong. In the Codex path, write/setup failures are already treated as `TransportFailure`: [process_backend.rs:465](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L465). After a successful Codex exit, the adapter reads the `--output-last-message` file, and any read failure is currently mapped to `SchemaValidationFailure`: [process_backend.rs:496](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L496), [process_backend.rs:517](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L517), [process_backend.rs:523](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L523). But no schema validation has happened yet; actual JSON validation begins only in the subsequent `serde_json::from_str(...)` branch: [process_backend.rs:529](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L529), [process_backend.rs:535](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L535).

This misclassification has behavioral impact, not just diagnostic impact. The retry policy gives transport failures 3 attempts and schema failures 2 attempts: [retry_policy.rs:34](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L34), [retry_policy.rs:36](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L36), [retry_policy.rs:38](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/retry_policy.rs#L38). The engine uses that failure class to decide whether and how long to retry: [engine.rs:1852](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1852). An unreadable or missing output file is therefore being treated as the wrong failure mode and gets the wrong retry behavior.

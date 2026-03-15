---
artifact: final-review-planner-positions
loop: 8
project: process-backend-adapter
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-14T22:29:29Z
---

I’m verifying each amendment against the implementation by reading the cited workflow, backend, and test code first, then I’ll judge each on whether it identifies a real correctness or robustness defect.
I’ve confirmed the cancel path does `SIGTERM` followed by an unbounded poll loop, and the service awaits that path before returning timeout/cancel. Next I’m checking whether the resume-schema and invocation-id claims match the actual argv construction, temp-file naming, and raw-output persistence.
I’ve verified the resume path omits `--output-schema` and only writes the schema file on fresh Codex sessions, with a unit test explicitly asserting that behavior. I’m now tightening the last point by tracing how `completion_round`, `cycle`, and `attempt` evolve so I can confirm whether invocation IDs really collide across workflow restarts.
The last open question is whether omitting `--output-schema` on Codex resume is actually a defect or just a different valid CLI mode. I’m checking the locally installed Codex CLI help now so I can judge that amendment on actual command semantics instead of assumption.
The workflow-ID issue is real in the code path itself: completion-round restarts keep `cycle` unchanged and reset `attempt` to `1`, but the backend `invocation_id` still omits `completion_round`. I’m checking the raw-output write path now to confirm that this is an actual overwrite, not just a naming inconsistency.
# Planner Positions

## Amendment: PB-CANCEL-TIMEOUT-HANG

### Position
ACCEPT

### Rationale
This is a real robustness bug. `ManagedChild::wait()` is an unbounded `try_wait()` polling loop with no deadline ([process_backend.rs:58](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L58)). `ProcessBackendAdapter::cancel()` sends `SIGTERM` and then awaits that loop before returning ([process_backend.rs:614](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L614), [process_backend.rs:629](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L629)). `AgentExecutionService::invoke()` awaits `cancel()` in both the cancellation and timeout branches before it returns `InvocationCancelled` or `InvocationTimeout` to the caller ([service.rs:196](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L196), [service.rs:198](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L198), [service.rs:208](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L208)).

That means a TERM-ignoring or TERM-trapping child can make the timeout/cancel path hang indefinitely, so the configured timeout is not authoritative in practice. The current tests only cover children that do exit on `SIGTERM`, and even assert that `cancel()` must reap the child before returning ([process_backend_test.rs:634](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L634), [process_backend_test.rs:687](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L687)), so this failure mode is currently untested.

## Amendment: PB-CODEX-RESUME-SCHEMA

### Position
REJECT

### Rationale
The code does omit schema creation and `--output-schema` on the resume path ([process_backend.rs:413](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L413), [process_backend.rs:439](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L439)), and the unit test intentionally asserts that behavior ([process_backend_test.rs:508](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L508), [process_backend_test.rs:556](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/process_backend_test.rs#L556)). But the amendment’s core claim is wrong: for the installed Codex CLI, `codex exec resume` does not accept `--output-schema` at all. Adding it would break resume invocations, not fix them.

It is also overstated to say resumed sessions are unconstrained by schema. The agent-execution layer still validates stage outputs against the stage contract after invocation regardless of backend argv shape ([service.rs:236](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L236)). Since the proposed change depends on an invalid command shape and does not identify an actual correctness gap in the current implementation, it should be rejected.

## Amendment: WF-INVOCATION-ID-COLLISION

### Position
ACCEPT

### Rationale
This collision exists. Workflow `invocation_id` is built from only `run_id`, `stage`, `cycle`, and `attempt` ([engine.rs:1957](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1957)). But completion-round restarts intentionally keep `cycle` unchanged ([engine.rs:1537](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1537)), and `StageCursor::advance_completion_round()` increments only `completion_round` while resetting `attempt` to `1` ([domain.rs:549](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L549)). So a round-2 restart of the same stage can reuse the same backend `invocation_id` as round 1.

That matters because raw backend output is persisted by `invocation_id` ([service.rs:227](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L227), [fs.rs:724](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L724)), and the filesystem store writes to the same target path atomically, replacing any prior file at that name ([fs.rs:136](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L136)). So later completion rounds overwrite earlier backend evidence. The process backend also derives Codex temp-file names directly from `invocation_id` ([process_backend.rs:436](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L436)), which makes reused IDs unsafe there as well. This is consistent with the rest of the workflow code already treating completion rounds as part of durable artifact identity via round-aware payload/artifact IDs ([engine.rs:208](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L208), [workflow_engine_test.rs:3637](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/workflow_engine_test.rs#L3637)).

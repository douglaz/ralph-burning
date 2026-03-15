---
artifact: final-review-votes
loop: 8
project: process-backend-adapter
backend: codex
role: final_reviewer
created_at: 2026-03-14T22:31:13Z
---

I’m checking the cited backend, service, and workflow code directly so each vote is based on the implementation rather than the planner’s summary.
The core control-flow matches the planner so far. I’m checking the raw-output/temp-file collision paths and the actual `codex exec resume` CLI shape before I lock the votes.
`codex exec resume --help` confirms the planner’s key objection: resume mode exposes `--output-last-message` but not `--output-schema`. I’m doing one last pass on test coverage and artifact identity, then I’ll issue the votes.
# Vote Results

## Amendment: PB-CANCEL-TIMEOUT-HANG

### Vote
ACCEPT

### Rationale
The planner is right. [`ManagedChild::wait()`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L52) is an unbounded `try_wait()` polling loop, and [`ProcessBackendAdapter::cancel()`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L610) awaits it after sending `SIGTERM`. [`AgentExecutionService::invoke()`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L194) blocks on that cancel path in both the explicit-cancel and timeout branches, so a TERM-ignoring child can prevent `InvocationCancelled` or `InvocationTimeout` from ever returning. That is a real correctness defect in timeout semantics, and the proposed bounded or best-effort cancel behavior is the right direction. The added regression test is also warranted because the current tests only cover children that exit after `SIGTERM`.

## Amendment: PB-CODEX-RESUME-SCHEMA

### Vote
REJECT

### Rationale
I agree with the planner’s rejection. The code does omit schema generation and `--output-schema` on resume, but the proposed fix is based on the wrong command contract. Local CLI help shows `codex exec` supports `--output-schema`, while `codex exec resume` does not; adding that flag to resume would break the invocation rather than correct it. The current unit test is asserting the actual supported resume argv shape, not merely entrenching a bug. There is also still post-invocation stage validation in [`AgentExecutionService::invoke()`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L227), so the amendment overstates the impact. If tighter schema enforcement for resumed Codex sessions is desired, it needs a different design than this amendment proposes.

## Amendment: WF-INVOCATION-ID-COLLISION

### Vote
ACCEPT

### Rationale
The planner’s position is correct. Workflow stage `invocation_id` currently omits `completion_round` in [`invoke_stage_on_backend()`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1947), while [`StageCursor::advance_completion_round()`](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs#L542) increments `completion_round` and resets `attempt` to `1` without changing `cycle`. That makes later completion rounds able to reuse the same backend `invocation_id` as earlier rounds. Since raw backend output is persisted by `invocation_id` in [`AgentExecutionService::invoke()`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L227) and [`FsRawOutputStore::persist_raw_output()`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L724), later rounds overwrite earlier evidence. The process backend also derives Codex temp filenames from the same identifier in [`invoke_codex()`](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/process_backend.rs#L429), so round-aware IDs are the correct fix, and tests should cover both backend artifacts and temp-file separation.

---
artifact: final-review-votes
loop: 4
project: cursor-history-integrity
backend: codex
role: final_reviewer
created_at: 2026-03-13T23:46:09Z
---

I’m verifying both amendment claims against the cited Rust paths before voting. First step is reading the late-stage completion-round resume logic and the daemon requirements service construction side by side.
The completion-round defect is narrowing to one question: whether resume on the old round actually reuses the same history IDs and whether writes can replace existing files. I’m reading the cursor-derived ID generation and the filesystem write path now, plus the requirements target resolver to confirm the daemon inconsistency is behavioral.
The overwrite path is present: history filenames are keyed by `completion_round`, and the filesystem writer stages then renames into the canonical path without a duplicate check. I’m pulling the rename block itself and scanning for any existing regression coverage before I finalize the votes.
I’m checking one remaining nuance from the planner notes: whether backend/model selection is only metadata under the stub adapter or still affects persisted behavior. That decides whether amendment `02` is a correctness fix or merely a consistency cleanup.
The daemon gap is behavioral even with the stub adapter: `AgentExecutionService` stamps the resolved backend/model into invocation metadata, and the stub copies those fields from the resolved target. I’m finishing with a quick look at nearby tests, then I’ll cast the votes.
No local skill fits this review; the cited source inspection is enough. I’ve verified both failure modes against the implementation and existing tests, and I’m writing the vote summary now.
# Vote Results

## Amendment: CRI-20260313-01

### Vote
ACCEPT

### Rationale
The planner’s position is correct. In the late-stage amendment branch, the snapshot queue is updated before the `completion_round_advanced` append, but the durable round/cursor advance happens only after that append succeeds in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1012) and [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1055). If that append fails, `fail_run` writes a resumable failed snapshot with `active_run = None` and the old `completion_rounds` value in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1997).

On resume, `derive_resume_state` rebuilds `current_completion_round` from `snapshot.completion_rounds` and durable `CompletionRoundAdvanced` events only; pending amendments merely force the stage back to planning in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2474) and [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2524). Because history IDs include `cursor.completion_round`, the resumed pass reuses the old `-cr1` identity in [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L114). The filesystem writer then renames staging files into canonical history paths without a duplicate guard in [fs.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L810), so append-only history is not preserved. The proposed fix and regression test are warranted.

## Amendment: CRI-20260313-02

### Vote
ACCEPT

### Rationale
The daemon path is inconsistent with the CLI path in a real, observable way. Both daemon requirements entry points construct the service through `build_requirements_service()` in [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L519) and [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L672), and that helper returns `RequirementsService::new(...)` without `.with_workspace_defaults(...)` in [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L1133). The CLI explicitly loads `EffectiveConfig`, derives `BackendSelectionConfig`, and passes those defaults into the service in [requirements.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs#L32) and [requirements.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs#L65).

`RequirementsService` resolves each invocation target from `self.workspace_defaults` in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs#L880), and the resolver only applies workspace defaults when they are actually supplied in [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L43). This is not dead metadata: the resolved backend/model is propagated into invocation metadata even under the stub adapter. Since the daemon already loads `EffectiveConfig` earlier in task processing in [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L368), threading those defaults through is straightforward and the proposed test coverage is justified.

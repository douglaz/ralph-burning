---
artifact: final-review-planner-positions
loop: 4
project: cursor-history-integrity
backend: codex(gpt-5.4-xhigh)
role: planner
created_at: 2026-03-13T23:43:50Z
---

Reviewing both amendments against the actual Rust sources now. No skill applies here; I’m reading the cited engine and daemon code paths first, then I’ll judge whether each claim matches the implementation.
I’ve confirmed both amendments touch real code paths. Next I’m pulling the exact branches and resume/config resolution logic to check whether the reported failure modes are actually reachable.
The daemon findings are matching so far: CLI wires workspace defaults into `RequirementsService`, daemon does not. I’m checking whether daemon has enough config in scope already and whether there’s any hidden fallback that would make the amendment unnecessary.
The completion-round path is more subtle because `fail_run` persists the in-memory snapshot even after the journal append fails. I’m checking whether resume reconstructs the round from that snapshot or discards it, and whether any existing tests already cover the exact append-failure case.
I’ve confirmed resume would rebuild the cursor at the old completion round. One more check: whether payload/artifact persistence actually overwrites an existing `-cr1` file or rejects duplicates, since that affects the correctness claim in the first amendment.
I’ve confirmed the workflow IDs are reused if resume stays on the old round. The remaining daemon question is whether the resolver can still pick up workspace defaults from somewhere else; I’m reading that resolver path now so the second judgment is grounded in the actual target-selection code.
There’s one more nuance on the daemon amendment: both paths currently instantiate the stub adapter. I’m checking whether the resolved backend/model is still part of real behavior or only dead metadata, because that determines whether the missing defaults are a true correctness gap.
# Planner Positions

## Amendment: CRI-20260313-01

### Position
ACCEPT

### Rationale
The defect is real. In the late-stage amendment branch, the engine adds the new amendments to the snapshot before attempting the `CompletionRoundAdvanced` journal append, but it does not advance `snapshot.completion_rounds` or `active_run.stage_cursor` until after that append succeeds ([engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1012), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1028), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1055)). If that append fails, `fail_run_result` persists a failed, resumable snapshot with `active_run = None`, preserving the pending amendments but not the advanced round ([engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2016), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1997)). On resume, `derive_resume_state` seeds `current_completion_round` from `snapshot.completion_rounds`, only advances it from a durable `CompletionRoundAdvanced` event, and merely rewinds the stage index to planning when amendments are pending ([engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2474), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2498), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L2524)). History IDs are derived from `cursor.completion_round` ([engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L114), [engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs#L1774)), so resumed planning will reuse `-cr1`. The filesystem writer does not guard against duplicate canonical IDs before `rename`, so this is at minimum a non-append-only duplicate-ID bug, and on Unix it can replace the prior file outright ([fs.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/adapters/fs.rs#L774)). The proposed fix direction is technically justified.

## Amendment: CRI-20260313-02

### Position
ACCEPT

### Rationale
The daemon path does ignore workspace backend/model defaults. Both daemon requirements entry points call `build_requirements_service()` with no config input ([daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L506), [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L648)), and that helper returns `RequirementsService::new(...)` without `.with_workspace_defaults(...)` ([daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L1133), [daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L1167)). `RequirementsService` resolves its target from `self.workspace_defaults` ([service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs#L107), [service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs#L880)), and the resolver only applies workspace defaults when that argument is present ([agent_execution/service.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/agent_execution/service.rs#L43)). The CLI path explicitly loads `EffectiveConfig`, derives `BackendSelectionConfig`, and passes it into `.with_workspace_defaults(...)` ([requirements.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs#L40), [requirements.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs#L71)). Since the daemon already loads `EffectiveConfig` in task processing ([daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs#L368)), the omission is a real inconsistency, not dead code. Threading workspace defaults into daemon requirements service construction is technically warranted.

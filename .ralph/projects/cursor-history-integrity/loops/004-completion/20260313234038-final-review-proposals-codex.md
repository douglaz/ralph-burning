---
artifact: final-review-proposals
loop: 4
project: cursor-history-integrity
backend: codex
role: final_reviewer
created_at: 2026-03-13T23:40:38Z
---

Reviewing the branch against `85ccbdb59e406c10e305f26ebfac72d3de3ad731` from `/root/new-ralph-burning/ralph-burning-rewrite`. I’m starting with the full diff, then I’ll read the changed implementation and test files end-to-end and verify the new overflow and history-ID behavior by tracing the code paths.
I’m collecting the exact file set changed in this review scope first, so I can read the implementation and tests without missing any touched paths.
The change set is broader than the two core integrity fixes, so I’m splitting review into two passes: first the overflow/history-ID implementation itself, then the surrounding CLI/conformance/runtime edits that were changed in the same range.
I’ve confirmed the core cursor changes are structurally in place. Next I’m tracing the workflow and requirements services end-to-end to see whether every ID/counter path was updated consistently, including rollback and resume fallbacks.
I’ve found one obvious non-core overflow path in requirements handling and I’m checking whether it is an actual defect versus intentionally out of scope. I’m also tracing history persistence around stage completion and resume reconstruction, since that’s where round-aware IDs can still regress silently.
I’ve isolated the round-aware history logic itself. Now I’m checking for secondary identifiers and persistence paths that still key off the old `stage/cycle/attempt` tuple, because those can quietly reintroduce overwrite behavior outside the payload/artifact files.
I’m now tracing the failure-ordering around completion-round advancement. That path is subtle: if the round bump is not durably reconstructible after a checkpoint failure, the new `-cr{round}` suffix can still collapse back to `-cr1` on resume and overwrite the first branch.
I’ve traced one likely integrity gap in completion-round resume ordering. Before I write that up, I’m sampling the other non-core source changes that landed in the same diff range so I don’t miss a separate regression in the daemon/conformance support code.
I’ve got two likely defects from code tracing. I’m running the project’s own verification commands now to see whether anything else shakes out under the full test/conformance harness before I finalize the review.
The verification jobs are still running. I’m polling them and will fold any failures back into the review if they expose an issue I haven’t already traced manually.
Build and test are clean so far; conformance is halfway through and still green. I haven’t seen anything that changes the two defects from code tracing, but I’m waiting for the suite to finish in case another path fails later in the run.
I have the final set of review items. One is a direct append-only history violation on resume after a round-boundary journal failure; the other is a reachable daemon/CLI behavior split in requirements backend selection. I’m grabbing the exact line references before I write the review.
# Final Review: AMENDMENTS

## Amendment: CRI-20260313-01 [P1]

### Problem
The completion-round fix is still lossy on one resumable failure path. In the late-stage amendment branch, the code adds pending amendments before appending `completion_round_advanced`, but it does not persist the advanced round into the snapshot until after that journal append succeeds ([src/contexts/workflow_composition/engine.rs:1013](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs), [src/contexts/workflow_composition/engine.rs:1028](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs), [src/contexts/workflow_composition/engine.rs:1055](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs)). If that append fails, the failed snapshot remains resumable with pending amendments, but `derive_resume_state` reconstructs planning with the old `completion_round` because it only looks at the snapshot counter or a durable `CompletionRoundAdvanced` event ([src/contexts/workflow_composition/engine.rs:2524](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs), [src/contexts/workflow_composition/engine.rs:2537](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs)). Since history IDs are derived from `cursor.completion_round` ([src/contexts/workflow_composition/engine.rs:114](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs), [src/contexts/workflow_composition/engine.rs:1774](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs)), a resumed planning pass can reuse `-cr1` and overwrite round-1 payload/artifact files, which violates the append-only history goal.

### Proposed Change
Make the advanced completion round durably reconstructible before returning any resumable failure from this branch. The simplest fixes are:
1. persist `snapshot.completion_rounds`/`active_run.stage_cursor` to the next round before the `completion_round_advanced` append can fail, or
2. teach `derive_resume_state` to infer `current_completion_round + 1` when pending late-stage amendments exist but the round-advance event is missing.

Add a regression test that fails exactly on the `completion_round_advanced` append, resumes the run, and asserts that new history files are written under `-cr2` without overwriting the existing `-cr1` files.

### Affected Files
- [src/contexts/workflow_composition/engine.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) - preserve or reconstruct the advanced completion round across a failed `completion_round_advanced` checkpoint.
- [tests/unit/workflow_engine_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/workflow_engine_test.rs) - add a regression test for resume after `completion_round_advanced` append failure.

## Amendment: CRI-20260313-02 [P2]

### Problem
Daemon-initiated requirements runs ignore workspace backend/model defaults. Both daemon requirements entry points build the service with `build_requirements_service()` ([src/contexts/automation_runtime/daemon_loop.rs:519](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs), [src/contexts/automation_runtime/daemon_loop.rs:672](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs)), but that helper never calls `.with_workspace_defaults(...)` ([src/contexts/automation_runtime/daemon_loop.rs:1133](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs)). `RequirementsService` resolves targets from `self.workspace_defaults` ([src/contexts/requirements_drafting/service.rs:888](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs)), and the direct CLI path explicitly populates those defaults ([src/cli/requirements.rs:40](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs), [src/cli/requirements.rs:71](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs)). That means `ralph-burning daemon start` and `ralph-burning requirements ...` can route the same requirements workflow to different backends/models.

### Proposed Change
Thread `EffectiveConfig`/`BackendSelectionConfig` into daemon requirements service construction and call `.with_workspace_defaults(...)` exactly like the CLI handler does. Add an automation-runtime test that sets workspace backend/model defaults and verifies daemon-driven requirements draft/quick uses the same resolved target as the CLI path.

### Affected Files
- [src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) - apply workspace defaults when constructing the requirements service for daemon dispatch.
- [tests/unit/automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add coverage for daemon requirements dispatch honoring workspace defaults.

`nix develop -c cargo build`, `nix develop -c cargo test`, and `nix develop -c ralph-burning conformance run` all passed. The findings above come from code tracing through uncovered failure/configuration paths rather than from an already-failing test.

# Final Review Amendments Applied

## Round 1

### Amendment: CRI-20260313-01

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

### Reviewer
codex

### Amendment: CRI-20260313-02

### Problem
Daemon-initiated requirements runs ignore workspace backend/model defaults. Both daemon requirements entry points build the service with `build_requirements_service()` ([src/contexts/automation_runtime/daemon_loop.rs:519](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs), [src/contexts/automation_runtime/daemon_loop.rs:672](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs)), but that helper never calls `.with_workspace_defaults(...)` ([src/contexts/automation_runtime/daemon_loop.rs:1133](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs)). `RequirementsService` resolves targets from `self.workspace_defaults` ([src/contexts/requirements_drafting/service.rs:888](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs)), and the direct CLI path explicitly populates those defaults ([src/cli/requirements.rs:40](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs), [src/cli/requirements.rs:71](/root/new-ralph-burning/ralph-burning-rewrite/src/cli/requirements.rs)). That means `ralph-burning daemon start` and `ralph-burning requirements ...` can route the same requirements workflow to different backends/models.

### Proposed Change
Thread `EffectiveConfig`/`BackendSelectionConfig` into daemon requirements service construction and call `.with_workspace_defaults(...)` exactly like the CLI handler does. Add an automation-runtime test that sets workspace backend/model defaults and verifies daemon-driven requirements draft/quick uses the same resolved target as the CLI path.

### Affected Files
- [src/contexts/automation_runtime/daemon_loop.rs](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) - apply workspace defaults when constructing the requirements service for daemon dispatch.
- [tests/unit/automation_runtime_test.rs](/root/new-ralph-burning/ralph-burning-rewrite/tests/unit/automation_runtime_test.rs) - add coverage for daemon requirements dispatch honoring workspace defaults.

`nix develop -c cargo build`, `nix develop -c cargo test`, and `nix develop -c ralph-burning conformance run` all passed. The findings above come from code tracing through uncovered failure/configuration paths rather than from an already-failing test.

### Reviewer
codex


---
artifact: prompt-review
project: p0-scope
backend: codex
role: prompt_reviewer
created_at: 2026-03-16T02:04:44Z
---

# Prompt Review

## Issues Found
- The prompt asks for broad P0 parity but only loosely recommends slice-based delivery. Without an explicit “one slice per loop” rule, downstream agents can over-scope and fail to converge.
- “Use the old repo as a behavior reference” is not strong enough by itself. The prompt does not clearly say what to do when old behavior conflicts with the new architecture, so agents may either cargo-cult legacy structure or ignore parity.
- Several key terms are underdefined, including `opposite-family`, `planner-like roles`, `availability`, `capability`, `real backend path`, and `supporting payload/artifact`. That leaves room for incompatible implementations.
- Backend alternation rules do not fully specify fallback behavior for every backend family, especially when `openrouter` is the base backend or when multiple families are disabled.
- The testing model is incomplete. Many scenario IDs are listed, but the prompt does not make automated coverage and test execution mandatory for each slice.
- GitHub and OpenRouter work depend on external tools and credentials, but the prompt does not define what to do when live verification is unavailable.
- Config expansion is detailed, but some merge and failure semantics are still ambiguous, especially around disabled backends, optional panel members, and precedence on resume.
- Resume behavior mixes prompt-change handling, backend drift, and rollback concerns without defining their precedence, which can produce contradictory implementations.
- Validation execution requires timeouts and durable evidence, but the prompt does not define a default timeout policy or how to classify timeout failures.
- The prompt mixes mandatory requirements, design guidance, and optional notes. Downstream implementation loops need a clearer separation between required behavior, sequencing, and acceptance criteria.
- Some failure paths are implied but not explicit, such as what happens when checkpoint reset fails after logical rollback or when filtered panel members no longer meet minimum thresholds.
- The prompt lacks an explicit blocker policy for missing old-repo references, missing tools, or unavailable credentials, which encourages guessing.

## Refined Prompt
# P0 Parity Implementation Spec for `ralph-burning`

## Objective
Implement the scoped P0 parity features in the new repo so that externally observable behavior matches the old repo for the areas listed below.

Use the old repo only as a behavior oracle. Do not restore the old architecture, old storage formats, or old workflow protocol.

## Delivery Rules
1. Execute work in the slice order defined in this prompt.
2. Complete one slice per implementation loop or branch. Do not attempt all of P0 in one pass.
3. A slice is complete only when code, automated coverage, and verification for that slice are all done.
4. For each completed slice, report the changed areas, tests run, and any remaining blockers or unverified paths.
5. If an old-repo behavior reference, required tool, or credential is missing, stop that slice and report the exact blocker instead of guessing.
6. If old behavior conflicts with the architecture constraints below, preserve the new architecture and re-express the behavior through policy services, typed records, and runtime services.
7. Production code must not directly instantiate `StubBackendAdapter` or file-based GitHub watchers. Those are test-only.

## Scope
### In Scope
- Real backend execution for `requirements` and daemon requirements paths
- OpenRouter backend support
- Per-role backend and model overrides
- Backend alternation and opposite-family policy
- Role-specific timeout resolution
- Backend availability and capability checks matching old behavior
- Explicit git checkpoint and rollback semantics
- Prompt review refiner plus validator panel
- Completion panel with `min_completers` and consensus threshold
- Final review reviewer panel, votes, arbiter, and restart cap
- Prompt change detection and policy on resume
- Backend drift detection on resume
- Separate QA, review, and final-review iteration controls
- Real validation runner integration for standard, docs, and CI flows
- Pre-commit checks equivalent to old behavior
- Real GitHub intake, label management, and explicit command routing
- Multi-repo daemon with `--data-dir`
- Draft PR watcher/runtime
- PR review ingestion into amendments
- Rebase/runtime support sufficient for old P0 daemon parity

### Out of Scope
- `tmux` parity
- Streaming UX parity
- Full old PRD pipeline depth
- `auto` and `quick-dev-auto`
- Manual `amend` CLI
- Backend diagnostic CLI
- Template override system
- Legacy storage or artifact compatibility
- User-defined workflow DSLs

## Behavior References
### Old Repo Behavior Reference
- Backend/runtime: `src/backend/mod.rs`, `src/backend/openrouter.rs`, `src/validate/tests_openrouter.rs`, `src/validate/tests_resume_backend_resolution.rs`, `src/validate/tests_role_timeouts.rs`
- Workflow: `src/workflow/orchestrator.rs`, `src/workflow/pre_commit_checks.rs`, `src/project/state.rs`, `src/config/mod.rs`, `src/validate/tests_prompt_review_panel.rs`, `src/validate/tests_completion_panel.rs`, `src/validate/tests_final_review.rs`, `src/validate/tests_final_review_cap_skip.rs`, `src/validate/tests_pre_commit_checks.rs`
- Daemon/GitHub: `src/cli/daemon.rs`, `src/daemon/runtime.rs`, `src/daemon/github.rs`, `src/daemon/rebase_agent.rs`, `src/daemon/worktree.rs`, `src/validate/tests_daemon.rs`, `src/validate/tests_daemon_rebase.rs`, `src/validate/tests_pr_lifecycle.rs`, `src/validate/tests_pr_runtime.rs`, `src/validate/tests_pr_review.rs`

### New Repo Implementation Target
- Workspace/config: `src/contexts/workspace_governance/config.rs`, `src/shared/domain.rs`, `src/cli/config.rs`
- Agent execution: `src/contexts/agent_execution/model.rs`, `src/contexts/agent_execution/service.rs`, `src/contexts/agent_execution/session.rs`, `src/adapters/process_backend.rs`, `src/adapters/stub_backend.rs`, `src/cli/run.rs`, `src/cli/requirements.rs`
- Workflow: `src/contexts/workflow_composition/*`
- Run record/history: `src/contexts/project_run_record/*`, `src/adapters/fs.rs`
- Daemon: `src/contexts/automation_runtime/*`, `src/adapters/worktree.rs`, `src/cli/daemon.rs`
- Conformance: `tests/conformance/features/*`, `src/contexts/conformance_spec/*`

## Non-Negotiable Architecture Constraints
- Do not read or write old `.ralph` files or old artifact formats.
- Do not reintroduce a separate quick-dev orchestrator.
- Do not restore markdown parsing as the workflow protocol.
- Do not use artifact scanning as canonical state reconstruction.
- Preserve the history/log split.
- Durable history means journal entries, payloads, and rendered artifacts.
- Runtime logs remain ephemeral operational/debug logs.
- Structured output is mandatory for all workflow and requirements contracts.
- Add behavior through stage-internal policy, typed payload/artifact metadata, config-driven policy, and daemon/runtime services.
- Do not collapse new behavior into one giant `engine.rs` or daemon loop.
- Daemon state lives under `--data-dir`.
- Project and run state live inside each repo checkout’s `.ralph-burning` workspace.

## Shared Foundations
### Configuration Model
Implement these config files:
- Workspace config: `.ralph-burning/workspace.toml`
- Project policy config: `.ralph-burning/projects/<project-id>/config.toml`
- Project metadata remains in `project.toml`

Implement config precedence exactly:
1. CLI overrides
2. Project config
3. Workspace config
4. Code defaults

Add serializable config structs in `src/shared/domain.rs`:
- `ProjectConfig`
- `WorkflowSettings`
- `PromptReviewSettings`
- `CompletionSettings`
- `FinalReviewSettings`
- `ValidationSettings`
- `BackendRuntimeSettings`
- `BackendRoleModels`
- `BackendRoleTimeouts`

Replace the current narrow effective config with:
- `EffectiveRunPolicy`
- `EffectivePromptReviewPolicy`
- `EffectiveCompletionPolicy`
- `EffectiveFinalReviewPolicy`
- `EffectiveValidationPolicy`
- `EffectiveBackendPolicy`

Add CLI overrides to `run start` and `run resume`:
- `--backend`
- `--planner-backend`
- `--implementer-backend`
- `--reviewer-backend`
- `--qa-backend`

Do not add CLI overrides for panel backends in P0.

Support this config shape as the contract:
```toml
[settings]
default_flow = "standard"
default_backend = "claude"
default_model = "claude-opus-4-6"

[prompt_review]
enabled = true
refiner_backend = "claude"
validator_backends = ["claude", "codex", "?openrouter"]
min_reviewers = 2

[workflow]
planner_backend = "claude"
implementer_backend = "codex"
reviewer_backend = "claude"
qa_backend = "codex"
max_qa_iterations = 3
max_review_iterations = 3
prompt_change_action = "restart_cycle"

[completion]
backends = ["claude", "codex", "?openrouter"]
min_completers = 2
consensus_threshold = 0.66

[final_review]
enabled = true
backends = ["claude", "codex"]
arbiter_backend = "claude"
min_reviewers = 2
consensus_threshold = 0.66
max_restarts = 2

[validation]
standard_commands = []
docs_commands = []
ci_commands = []
pre_commit_fmt = true
pre_commit_clippy = true
pre_commit_nix_build = false
pre_commit_fmt_auto_fix = false
```

Panel backend specs use this rule:
- `backend_name` means required
- `?backend_name` means optional

Filtering rules:
- Required disabled or unavailable backends fail resolution
- Optional disabled or unavailable backends are skipped
- After filtering, the stage must still satisfy `min_reviewers` or `min_completers`
- If the minimum is no longer satisfied, the stage fails

### Backend Policy Rules
Add `src/contexts/agent_execution/policy.rs` with these methods:
- `resolve_role_target`
- `resolve_completion_panel`
- `resolve_prompt_review_panel`
- `resolve_final_review_panel`
- `timeout_for_role`
- `opposite_family`
- `planner_family_for_cycle`

Use this resolution order:
1. CLI role override
2. Project role override
3. Workspace role override
4. Cycle-based default policy

Use these default family rules unless overridden:
- Odd-numbered work cycles use the base backend for planner-like roles
- Even-numbered work cycles use the opposite family
- Implementer uses the opposite of the planner family
- Reviewer uses the planner family
- QA uses the implementer family
- Completion planner uses the planner family
- Default completer uses the opposite of the completion planner

Define `opposite_family` exactly:
- `claude -> codex`, falling back to `openrouter` if `codex` is unavailable and `openrouter` is enabled
- `codex -> claude`, falling back to `openrouter` if `claude` is unavailable and `openrouter` is enabled
- `openrouter -> claude`, falling back to `codex` if `claude` is unavailable
- If no opposite family is available after fallback, fail resolution

Resolve role timeouts from `backends.<name>.role_timeouts.<role>`.
If no role timeout exists, use the backend default timeout if the repo already has one.
If no backend default exists, use the current process-backend default.

### Run State and History Metadata
Expand `ActiveRun` with:
- `prompt_hash_at_cycle_start`
- `prompt_hash_at_stage_start`
- `qa_iterations_current_cycle`
- `review_iterations_current_cycle`
- `final_review_restart_count`
- `stage_resolution_snapshot`

Persist `StageResolutionSnapshot` for:
- Single-role stages
- Completion panels
- Prompt-review panels
- Final-review panels

Expand `PayloadRecord` and `ArtifactRecord` to include:
- `record_kind`: `StagePrimary | StageSupporting | StageAggregate`
- `producer`: agent metadata, local-validation metadata, or system metadata
- `completion_round`

These metadata are required for:
- Resume drift detection
- Panel history
- Better tail/history queries
- Review/debug fidelity

### Recovery and Failure Precedence
Apply these rules in order:
1. On resume, evaluate prompt-change policy first.
2. If resume is still allowed, re-resolve the current stage backend or panel and compare it to the persisted snapshot.
3. If the resolution changed, emit a runtime warning and a journal warning, then continue with the newly resolved backend(s).

Define `prompt_change_action` exactly:
- `continue`: warn and continue
- `abort`: fail resume with a clear error
- `restart_cycle`: reset the in-progress cycle to planning, clear supporting records from the abandoned portion of the cycle, then continue

Rollback rules:
- Logical rollback always happens first
- Hard rollback uses the checkpoint ref if available
- If hard rollback fails, keep the logical rollback and record the VCS failure as durable warning/evidence

General failure rules:
- If a required backend resolves to disabled or unavailable, fail early and explicitly
- Do not silently substitute a different required backend
- If live GitHub or OpenRouter verification is unavailable, validate through mocks and conformance tests and report the missing live prerequisite

## Slice 1: Config and Backend-Policy Foundation
Implement:
- Expanded config structs and merge logic
- Project `config.toml`
- Effective policy types
- CLI backend override surface
- Backend policy service
- Role timeouts
- `InvocationContract` schema API so both workflow and requirements contracts expose structured-output requirements

Add or modify:
- `src/shared/domain.rs`
- `src/contexts/workspace_governance/config.rs`
- `src/contexts/agent_execution/model.rs`
- `src/contexts/workflow_composition/contracts.rs`
- `src/contexts/requirements_drafting/contracts.rs`
- `src/cli/run.rs`

Tests and conformance:
- Port old behavior from `tests_role_timeouts.rs`
- `backend.role_overrides.per_role_override_beats_default`
- `backend.role_timeouts.config_roundtrip`

## Slice 2: Real Requirements Backend Path
Implement:
- Shared `AgentExecutionService` builder in `src/composition/agent_execution_builder.rs`
- Real backend execution for `requirements draft` and `requirements quick`
- Daemon requirements path must use the same builder
- `ProcessBackendAdapter` must support `InvocationContract::Requirements`
- No production path may directly instantiate `StubBackendAdapter`

Add or modify:
- `src/composition/agent_execution_builder.rs`
- `src/cli/requirements.rs`
- `src/cli/run.rs`
- `src/contexts/automation_runtime/daemon_loop.rs`
- `src/adapters/process_backend.rs`
- `src/contexts/agent_execution/service.rs`

Tests and conformance:
- `backend.requirements.real_backend_path`

## Slice 3: OpenRouter Parity
Add `src/adapters/openrouter_backend.rs` and implement:
- Availability check
- Capability check
- Structured invocation
- Explicit model injection
- Timeout support
- Cancellation support
- Session reuse only if explicitly implemented and reported as supported

Integrate OpenRouter into backend config and policy resolution.

Tests and conformance:
- Port old behavior from `tests_openrouter.rs`
- `backend.openrouter.model_injection`
- `backend.openrouter.disabled_default_backend`
- `backend.openrouter.requirements_draft`

## Slice 4: Prompt Review and Completion Panel Parity
Add:
- `src/contexts/workflow_composition/panel_contracts.rs`
- `src/contexts/workflow_composition/prompt_review.rs`
- `src/contexts/workflow_composition/completion.rs`

Prompt review behavior:
1. Run the prompt refiner.
2. Persist the refinement as supporting payload/artifact.
3. Run the validator panel on the refined prompt.
4. If any validator rejects, prompt review fails.
5. If executed validators are fewer than `min_reviewers`, prompt review fails.
6. If prompt review succeeds, persist `prompt.original.md`, replace the project prompt with the refined prompt, update the prompt hash, and continue to planning.

Completion behavior:
1. Resolve completer panel backends.
2. Persist per-completer supporting records.
3. Compute aggregate verdict as `complete` only when `complete_votes >= min_completers` and `complete_votes / total_voters >= consensus_threshold`.
4. Persist the aggregate as the canonical completion result used by resume logic.
5. If aggregate verdict is `complete`, continue to acceptance QA.
6. Otherwise reopen work.

Persist stage-resolution snapshots at stage start for all covered stages and panels.

Tests and conformance:
- Port old prompt-review and completion-panel behavior
- `workflow.prompt_review.panel_accept`
- `workflow.prompt_review.panel_reject`
- `workflow.prompt_review.min_reviewers_enforced`
- `workflow.prompt_review.optional_validator_skip`
- `workflow.prompt_review.prompt_replaced_and_original_preserved`
- `workflow.completion.panel_two_completer_consensus_complete`
- `workflow.completion.panel_continue_verdict`
- `workflow.completion.optional_backend_skip`
- `workflow.completion.required_backend_failure`
- `workflow.completion.threshold_consensus`
- `workflow.completion.insufficient_min_completers`
- `backend.resume_drift.implementation_warns_and_reresolves`
- `backend.resume_drift.qa_warns_and_reresolves`
- `backend.resume_drift.review_warns_and_reresolves`
- `backend.resume_drift.completion_panel_warns_and_reresolves`

## Slice 5: Final Review, Prompt-Change Policy, and Iteration Caps
Add:
- `src/contexts/workflow_composition/final_review.rs`
- `src/contexts/workflow_composition/drift.rs`

Implement final review behavior:
1. Resolve the reviewer panel.
2. Each reviewer proposes amendments.
3. Canonicalize amendment ids as `fr-<completion-round>-<sha256(normalized-body)>[:8]`.
4. Define `normalized-body` as line endings normalized to `\n`, outer whitespace trimmed, and internal whitespace runs collapsed to single spaces.
5. Merge duplicate amendments by normalized-body hash and preserve source reviewer metadata.
6. If no amendments remain after merge, complete immediately.
7. Planner writes positions for all amendments.
8. Reviewers vote `ACCEPT` or `REJECT` per amendment.
9. Per-amendment consensus is `accepted` if `accept_count / total_votes >= threshold`, `rejected` if `accept_count == 0`, and `disputed` otherwise.
10. The arbiter resolves only disputed amendments.
11. Restart planning with the final accepted amendment set and increment `final_review_restart_count`.
12. If `max_restarts` is exceeded, force-complete with an explicit artifact explaining the cap hit.

Implement prompt-change behavior on resume:
- Compare the current `prompt.md` hash against `active_run.prompt_hash_at_cycle_start`
- Apply `continue`, `abort`, or `restart_cycle` exactly as defined in Recovery and Failure Precedence

Implement separate counters and caps:
- QA failure increments `qa_iterations_current_cycle`
- Review change-request increments `review_iterations_current_cycle`
- Final-review restart increments `final_review_restart_count`
- Enforce the three caps independently

Tests and conformance:
- Port old final-review and cap behavior
- `workflow.final_review.no_amendments_complete`
- `workflow.final_review.restart_then_complete`
- `workflow.final_review.planner_completion_with_pending_amendments_fails`
- `workflow.final_review.disputed_amendment_uses_arbiter`
- `workflow.final_review.restart_cap_force_complete`
- `workflow.resume.prompt_change_continue_warns`
- `workflow.resume.prompt_change_abort_fails`
- `workflow.resume.prompt_change_restart_cycle`
- `workflow.resume.backend_drift_warns`

## Slice 6: Validation Runner and Pre-Commit Parity
Add:
- `src/adapters/validation_runner.rs`
- `src/contexts/workflow_composition/validation.rs`

Validation runner requirements:
- Run command groups with `sh -lc` in the repo root
- Capture stdout, stderr, exit code, duration, and pass/fail
- Enforce a per-command timeout
- If the repo already has a validation timeout constant, reuse it
- Otherwise default to 900 seconds per command
- Return structured `ValidationCommandResult` and `ValidationGroupResult`
- Never mutate run state directly

Workflow behavior:
- `docs_change` and `ci_improvement` validation stages are local-validation stages and do not call an agent in P0
- Standard flow must inject local validation evidence into the review context
- After review approval, run pre-commit checks equivalent to old behavior:
  - `cargo fmt --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `nix build`
- Respect config booleans
- Skip cargo checks if there is no `Cargo.toml`
- If `pre_commit_fmt_auto_fix = true`, run `cargo fmt` after a `fmt --check` failure, then rerun the check
- If auto-fix succeeds and the rerun passes, count the fmt check as passed
- On any pre-commit failure, invalidate reviewer approval, store remediation feedback, return to implementation remediation, persist supporting validation evidence, and emit a runtime log entry

Tests and conformance:
- Port old pre-commit behavior
- `validation.docs.commands_pass`
- `validation.docs.command_failure_requests_changes`
- `validation.ci.commands_pass`
- `validation.ci.command_failure_requests_changes`
- `validation.standard.review_context_contains_local_validation`
- `validation.pre_commit.disabled_skips_checks`
- `validation.pre_commit.no_cargo_toml_skips_cargo_checks`
- `validation.pre_commit.fmt_failure_triggers_remediation`
- `validation.pre_commit.fmt_auto_fix_succeeds`
- `validation.pre_commit.nix_build_failure_records_feedback`

## Slice 7: Checkpoints and Hard Rollback Parity
Add `src/contexts/workflow_composition/checkpoints.rs` and implement `VcsCheckpointPort` with:
- `create_checkpoint`
- `find_checkpoint`
- `reset_to_checkpoint`

Create a checkpoint after each successful primary stage that advances run state:
- Prompt review completion
- Implementation, apply-fixes, docs update, or CI update
- Review approval
- Completion aggregate
- Acceptance QA pass
- Final review exit

Use this commit message format exactly:
```text
rb: checkpoint project=<project-id> stage=<stage-id> cycle=<n> round=<m>

RB-Project: <project-id>
RB-Run: <run-id>
RB-Stage: <stage-id>
RB-Cycle: <n>
RB-Completion-Round: <m>
```

Tests and conformance:
- `workflow.rollback.hard_uses_checkpoint`
- `workflow.checkpoint.commit_metadata_stable`

## Slice 8: GitHub Adapter and Multi-Repo Daemon Parity
Add:
- `src/adapters/github.rs`
- `src/contexts/automation_runtime/repo_registry.rs`
- `src/contexts/automation_runtime/github_intake.rs`

Implement this CLI surface:
```text
ralph-burning daemon start --data-dir <dir> --repo <owner/repo>... [--poll-seconds N] [--single-iteration] [--verbose]
ralph-burning daemon status --data-dir <dir> [--repo <owner/repo>...]
ralph-burning daemon abort <issue-number> --data-dir <dir> --repo <owner/repo>
ralph-burning daemon retry <issue-number> --data-dir <dir> --repo <owner/repo>
ralph-burning daemon reconcile --data-dir <dir>
```

Use this daemon data layout:
```text
<data-dir>/
  repos/<owner>/<repo>/
    repo/
    worktrees/
    daemon/
      tasks/
      leases/
      journal.ndjson
```

Required GitHub behavior:
- Ensure labels exist
- Poll candidate issues by labels
- Read, add, remove, and replace labels
- Fetch issue comments
- Fetch PR review comments
- Fetch PR review summaries
- Post idempotent comments
- Create draft PRs
- Mark PRs ready
- Close PRs
- Fetch PR URL and PR state
- Update PR body
- Detect branch ahead of base

Use this label vocabulary:
- `rb:ready`
- `rb:in-progress`
- `rb:failed`
- `rb:completed`
- `rb:flow:standard`
- `rb:flow:quick_dev`
- `rb:flow:docs_change`
- `rb:flow:ci_improvement`
- `rb:requirements`
- `rb:waiting-feedback`

Use this command vocabulary:
- `/rb flow <preset>`
- `/rb requirements`
- `/rb run`
- `/rb retry`
- `/rb abort`

Use this routing precedence:
1. Explicit command
2. Flow label
3. Repo default routing policy

Add repo and task metadata for:
- `RepoRegistration`
- `repo_slug`
- `repo_root`
- `workspace_root`
- `issue_number`
- `pr_url`
- Dedup cursor or equivalent last-seen review/comment state

Use per-task worktrees under `<data-dir>/repos/<owner>/<repo>/worktrees/<task-id>/`.
Use branch names `rb/<issue-number>-<project-id>`.

File-based issue sources remain test-only.

Tests and conformance:
- `daemon.github.start_validates_repos_and_data_dir`
- `daemon.github.multi_repo_status`
- `daemon.routing.command_beats_label`
- `daemon.routing.label_used_when_no_command`
- `daemon.labels.ensure_on_startup`
- `daemon.tasks.abort_by_issue_number`
- `daemon.tasks.retry_failed_issue`
- `daemon.tasks.reconcile_stale_leases`
- `daemon.tasks.worktree_isolation`

## Slice 9: Draft PR Runtime, Review Ingestion, and Rebase Parity
Add:
- `src/contexts/automation_runtime/pr_runtime.rs`
- `src/contexts/automation_runtime/pr_review.rs`

Draft PR behavior:
- When a task branch first moves ahead of base, push the branch and create a draft PR
- Do not create duplicate PRs
- Persist the PR URL on task metadata
- If the task completes with no diff, close or skip PR creation according to policy
- Support clean cancellation

PR review ingestion behavior:
- Ingest inline review comments, top-level PR comments, and review summary comments
- Restrict ingestion to whitelisted users
- Deduplicate using `pull_comment:<id>`, `issue_comment:<id>`, and `review:<id>`
- Convert accepted review text into amendments
- Persist staged amendments before dispatch
- Preserve staged amendments across restart and transient API failures
- If new review amendments arrive for a completed project, reopen the project into the correct active preset state and dispatch remediation work

Rebase parity behavior:
- Allow task rebase onto the default branch
- Support backend-assisted conflict resolution when enabled
- If rebase fails terminally, move the task to failed while preserving the worktree
- Journal rebase state and outcome

Tests and conformance:
- `daemon.pr_runtime.create_draft_when_branch_ahead`
- `daemon.pr_runtime.push_before_create`
- `daemon.pr_runtime.clean_shutdown_on_cancel`
- `daemon.pr_runtime.no_diff_close_or_skip`
- `daemon.pr_review.whitelist_filters_comments`
- `daemon.pr_review.dedup_across_restart`
- `daemon.pr_review.transient_error_preserves_staged`
- `daemon.pr_review.completed_project_reopens_with_amendments`
- `daemon.rebase.agent_resolves_conflict`
- `daemon.rebase.disabled_agent_aborts_conflict`
- `daemon.rebase.timeout_classification`

## Testing and Verification Requirements
For every slice:
- Add or update unit tests for pure logic and policy resolution
- Add or update conformance scenarios for every scenario id listed for that slice
- Run the narrowest meaningful automated test set during development
- Run the repo’s broad regression suite before marking the slice complete
- If a full suite cannot run, report the exact reason and run the narrowest meaningful subset instead
- If live GitHub or OpenRouter verification cannot run locally, validate through mocks and conformance tests and report the missing live prerequisite

## Final Definition of Done
P0 is complete only when all of the following are true:
- `requirements` and daemon requirements paths use real backends
- OpenRouter works end to end
- Per-role backend overrides work
- Role-specific timeouts work
- Resume backend drift warnings work
- Prompt review uses a refiner plus validator panel
- Completion uses `min_completers` and threshold consensus
- Final review uses reviewer proposals, votes, and arbiter resolution
- Prompt change policy works
- QA, review, and final-review caps work independently
- Git checkpoints are created and used for hard rollback
- Docs and CI flows run real local validation commands
- Standard flow injects local validation evidence
- Pre-commit checks gate review approval
- Daemon supports multi-repo `--data-dir`, GitHub labels and commands, draft PRs, PR review amendments, and rebase parity
- Conformance coverage exists for every behavior listed in this prompt

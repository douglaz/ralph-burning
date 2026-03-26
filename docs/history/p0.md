# P0 scope

## In scope

### Backend/runtime

* real backend execution for `requirements` and daemon requirements paths
* OpenRouter support
* per-role backend/model overrides
* backend alternation / opposite-family policy
* role-specific timeout configuration
* backend availability and capability checks that match old behavior

### Workflow semantics

* explicit git checkpoint/commit semantics
* prompt review refiner + validator panel
* completion panel with min/threshold consensus
* final review reviewer panel + votes + arbiter + restart cap
* prompt change detection and policy
* backend drift detection on resume
* separate QA/review/final-review iteration controls

### Validation/runtime

* real validation runner integration for:

  * standard flow
  * docs flow
  * CI flow
* pre-commit checks equivalent to old behavior

### Daemon/GitHub

* real GitHub issue intake
* label management
* explicit command routing
* multi-repo daemon with `--data-dir`
* draft PR watcher/runtime
* PR review ingestion into amendments
* rebase/runtime support sufficient for parity with the old P0 daemon surface

## Explicitly out of scope for this spec

* tmux parity
* streaming UX parity
* full old PRD pipeline depth
* `auto` / `quick-dev-auto`
* manual `amend` CLI
* backend diagnostic CLI
* template override system
* legacy storage compatibility
* user-defined workflow DSLs

---

# Reference sources

Use the old repo only as a **behavior reference**.

## Old repo files that define the target behavior

### Backend/runtime

* `src/backend/mod.rs`
* `src/backend/openrouter.rs`
* `src/validate/tests_openrouter.rs`
* `src/validate/tests_resume_backend_resolution.rs`
* `src/validate/tests_role_timeouts.rs`

### Workflow semantics

* `src/workflow/orchestrator.rs`
* `src/workflow/pre_commit_checks.rs`
* `src/project/state.rs`
* `src/config/mod.rs`
* `src/validate/tests_prompt_review_panel.rs`
* `src/validate/tests_completion_panel.rs`
* `src/validate/tests_final_review.rs`
* `src/validate/tests_final_review_cap_skip.rs`
* `src/validate/tests_pre_commit_checks.rs`

### Daemon/GitHub

* `src/cli/daemon.rs`
* `src/daemon/runtime.rs`
* `src/daemon/github.rs`
* `src/daemon/rebase_agent.rs`
* `src/daemon/worktree.rs`
* `src/validate/tests_daemon.rs`
* `src/validate/tests_daemon_rebase.rs`
* `src/validate/tests_pr_lifecycle.rs`
* `src/validate/tests_pr_runtime.rs`
* `src/validate/tests_pr_review.rs`

## New repo files that are the implementation target

### Workspace/config

* `src/contexts/workspace_governance/config.rs`
* `src/shared/domain.rs`
* `src/cli/config.rs`

### Backend/runtime

* `src/contexts/agent_execution/model.rs`
* `src/contexts/agent_execution/service.rs`
* `src/contexts/agent_execution/session.rs`
* `src/adapters/process_backend.rs`
* `src/adapters/stub_backend.rs`
* `src/cli/run.rs`
* `src/cli/requirements.rs`

### Workflow

* `src/contexts/workflow_composition/mod.rs`
* `src/contexts/workflow_composition/engine.rs`
* `src/contexts/workflow_composition/contracts.rs`
* `src/contexts/workflow_composition/payloads.rs`
* `src/contexts/workflow_composition/renderers.rs`
* `src/contexts/workflow_composition/retry_policy.rs`

### Project/run record

* `src/contexts/project_run_record/model.rs`
* `src/contexts/project_run_record/service.rs`
* `src/contexts/project_run_record/queries.rs`
* `src/adapters/fs.rs`

### Daemon

* `src/contexts/automation_runtime/mod.rs`
* `src/contexts/automation_runtime/model.rs`
* `src/contexts/automation_runtime/routing.rs`
* `src/contexts/automation_runtime/task_service.rs`
* `src/contexts/automation_runtime/lease_service.rs`
* `src/contexts/automation_runtime/daemon_loop.rs`
* `src/adapters/issue_watcher.rs`
* `src/adapters/worktree.rs`
* `src/cli/daemon.rs`

### Conformance

* `tests/conformance/features/*`
* `src/contexts/conformance_spec/*`

---

# Cross-cutting design decisions

These apply to every P0 workstream.

## 1. No legacy compatibility

Do not read or write old `.ralph` files or old artifact formats.

## 2. Keep the new architecture

Do not reintroduce:

* a separate quick-dev orchestrator
* markdown parsing as workflow protocol
* artifact scanning as canonical state reconstruction

## 3. Add behavior, not architectural regressions

When old `ralph` behavior is richer than current `ralph-burning`, implement it as:

* stage-internal policies
* supporting payload/artifact records
* config-driven policy
* daemon/runtime services

Do not collapse everything back into one giant `engine.rs` or daemon loop.

## 4. Preserve the new history/log split

* **durable history** = journal + payloads + rendered artifacts
* **runtime logs** = ephemeral debug/operational logs

## 5. Structured output remains mandatory

All supported backends must return structured outputs for workflow and requirements contracts.

---

# Shared foundational changes required before the four workstreams

These are prerequisites for nearly every P0 feature.

## A. Expand configuration model

Current `EffectiveConfig` only resolves:

* `prompt_review.enabled`
* `default_flow`
* `default_backend`
* `default_model`

That is insufficient for P0.

## Target config model

### Workspace config file

`.ralph-burning/workspace.toml`

### Project config file

`.ralph-burning/projects/<project-id>/config.toml`

Project metadata remains in `project.toml`.
Project-specific mutable policy belongs in `config.toml`.

## Effective config precedence

1. run command overrides
2. project config
3. workspace config
4. code defaults

## New config structure

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

[backends.claude]
enabled = true
command = "claude"
args = []
[backends.claude.role_models]
planner = "claude-opus-4-6"
implementer = "claude-sonnet-4-5"
reviewer = "claude-opus-4-6"
qa = "claude-opus-4-6"
completer = "claude-opus-4-6"
final_reviewer = "claude-opus-4-6"
prompt_reviewer = "claude-opus-4-6"
prompt_validator = "claude-opus-4-6"
arbiter = "claude-opus-4-6"
acceptance_qa = "claude-opus-4-6"
[backends.claude.role_timeouts]
planner = 7200
implementer = 7200
reviewer = 7200
qa = 7200
completer = 7200
final_reviewer = 7200
prompt_reviewer = 7200
prompt_validator = 7200
arbiter = 7200
acceptance_qa = 7200

[backends.codex]
enabled = true
command = "codex"
args = []
# same role_models / role_timeouts shape

[backends.openrouter]
enabled = false
command = "goose"
args = []
# same role_models / role_timeouts shape
```

## New CLI override surface

Add to `run start` and `run resume`:

* `--backend <spec>` meaning base/starting backend
* `--planner-backend <spec>`
* `--implementer-backend <spec>`
* `--reviewer-backend <spec>`
* `--qa-backend <spec>`

Do **not** add panel overrides in CLI for P0. Those remain config-driven.

## New config types to add

### In `src/shared/domain.rs`

Add serializable config structs:

* `ProjectConfig`
* `WorkflowSettings`
* `PromptReviewSettings` (expanded)
* `CompletionSettings`
* `FinalReviewSettings`
* `ValidationSettings`
* `BackendRuntimeSettings`
* `BackendRoleModels`
* `BackendRoleTimeouts`

### In `src/contexts/workspace_governance/config.rs`

Replace the current narrow `EffectiveConfig` with:

* `EffectiveRunPolicy`
* `EffectivePromptReviewPolicy`
* `EffectiveCompletionPolicy`
* `EffectiveFinalReviewPolicy`
* `EffectiveValidationPolicy`
* `EffectiveBackendPolicy`

## B. Expand run state for P0 semantics

Current `ActiveRun` is too small.

## Required `ActiveRun` additions

Add:

```rust
pub struct ActiveRun {
    pub run_id: String,
    pub stage_cursor: StageCursor,
    pub started_at: DateTime<Utc>,

    pub prompt_hash_at_cycle_start: String,
    pub prompt_hash_at_stage_start: String,

    pub qa_iterations_current_cycle: u32,
    pub review_iterations_current_cycle: u32,
    pub final_review_restart_count: u32,

    pub stage_resolution_snapshot: Option<StageResolutionSnapshot>,
}
```

## New supporting types

```rust
pub enum StageResolutionSnapshot {
    Single {
        role: String,
        backend_spec: String,
        model_id: String,
    },
    CompletionPanel {
        planner_backend: String,
        completer_backends: Vec<String>,
    },
    PromptReviewPanel {
        refiner_backend: String,
        validator_backends: Vec<String>,
    },
    FinalReviewPanel {
        planner_backend: String,
        reviewer_backends: Vec<String>,
        arbiter_backend: String,
    },
}
```

## Payload/artifact record metadata expansion

Current payload/artifact records do not store enough execution metadata.

Add to `PayloadRecord`:

```rust
pub struct PayloadRecord {
    pub payload_id: String,
    pub stage_id: StageId,
    pub cycle: u32,
    pub attempt: u32,
    pub created_at: DateTime<Utc>,
    pub payload: serde_json::Value,

    pub record_kind: HistoryRecordKind,
    pub producer: ProducerMetadata,
    pub completion_round: u32,
}
```

```rust
pub enum HistoryRecordKind {
    StagePrimary,
    StageSupporting,
    StageAggregate,
}
```

```rust
pub enum ProducerMetadata {
    Agent {
        role: String,
        backend_spec: String,
        model_id: String,
        session_id: Option<String>,
        session_reused: bool,
    },
    LocalValidation {
        command_group: String,
    },
    System {
        source: String,
    },
}
```

Do the same for `ArtifactRecord`.

This is required for:

* resume drift detection
* per-backend panel artifacts
* better history/tail
* review/debug fidelity

---

# Workstream 1: Real backend parity

## Goal

Bring `ralph-burning` backend behavior to P0 parity with old `ralph` by implementing:

* real agent execution for requirements paths
* OpenRouter support
* per-role backend/model resolution
* role-specific timeout resolution
* backend alternation/opposite-family policy
* proper capability/availability checks

## Current gap

### In the new repo today

* `src/cli/requirements.rs` uses `StubBackendAdapter`
* `src/contexts/automation_runtime/daemon_loop.rs` uses stub-backed requirements services
* `src/adapters/process_backend.rs` supports only Claude and Codex
* `ProcessBackendAdapter` rejects `InvocationContract::Requirements`
* there is no project config layer
* there is no per-role override surface
* there is no alternation/opposite-family policy

## Target behavior

## 1. Requirements uses real backends

`requirements draft` and `requirements quick` must use the same real `AgentExecutionService` family as workflow stages.

Test-only stub behavior should remain available only under explicit test wiring.

## 2. OpenRouter is a supported backend

OpenRouter must work end-to-end for:

* capability checks
* availability checks
* model injection
* structured output
* runtime invocation

## 3. Role-specific backend resolution exists

For each stage role, effective backend resolution must follow:

1. explicit run override for that role
2. project config role override
3. workspace config role override
4. flow/preset default role policy derived from base backend and alternation policy

## 4. Role-specific timeouts exist

Timeouts must be resolved from backend family + role.

## 5. Alternation/opposite-family policy exists

Implement the old semantics in the new engine:

* base backend is `default_backend` / `--backend`
* odd-numbered work cycles use base backend for planner-like roles
* even-numbered work cycles use the opposite family
* implementer uses the opposite of planner
* reviewer follows planner family by default
* QA defaults to implementer family unless explicitly overridden
* completion planner follows planner alternation
* default completer follows the opposite of completion planner
* if Claude’s opposite Codex is unavailable but OpenRouter is enabled, OpenRouter is used as Claude’s opposite

This policy is only the default. Explicit role overrides always win.

## Detailed design

## 1. Generalize `InvocationContract`

### Problem

`ProcessBackendAdapter` currently only knows how to build a schema for workflow stage contracts.

### Change

Extend `InvocationContract` so every contract type can expose structured-output requirements.

Add methods:

```rust
impl InvocationContract {
    pub fn json_schema_value(&self) -> serde_json::Value;
    pub fn family_name(&self) -> &'static str; // "workflow" | "requirements"
}
```

For workflow stage contracts:

* use existing stage schema

For requirements contracts:

* use requirements contract schema

### Files

* `src/contexts/agent_execution/model.rs`
* `src/contexts/workflow_composition/contracts.rs`
* `src/contexts/requirements_drafting/contracts.rs`

## 2. Real requirements backend path

### Change

Remove direct construction of `StubBackendAdapter` from:

* `src/cli/requirements.rs`
* `src/contexts/automation_runtime/daemon_loop.rs`

Replace with the same backend-service builder used by `run`.

### Required refactor

Move agent-service construction out of `src/cli/run.rs` into a shared builder module:

* `src/composition/agent_execution_builder.rs`

That builder must:

* read workspace effective config
* build backend config
* choose process vs stub mode only from explicit test environment
* return an `AgentExecutionService<BackendAdapter, ...>`

### Acceptance rule

No production path may instantiate `StubBackendAdapter` directly.

Only:

* tests
* conformance harness
* explicit env-gated test builder

may use the stub.

## 3. OpenRouter adapter

Add:

* `src/adapters/openrouter_backend.rs`

### Responsibilities

* build CLI invocation from configured command/args
* pass model selection explicitly
* return structured JSON payload
* capture raw output
* support timeout and cancellation
* support session reuse only if explicitly implemented; otherwise capability says no reuse

### Transport contract

Keep OpenRouter behavior consistent with the other backends:

* capability check
* availability check
* invoke
* cancel

### Required config support

* enabled
* command
* args
* role models
* role timeouts

### Required tests

Port old behavior reference from:

* `src/validate/tests_openrouter.rs`

New conformance ids:

* `backend.openrouter.model_injection`
* `backend.openrouter.disabled_default_backend`
* `backend.openrouter.requirements_draft`

## 4. Backend policy service extraction

Add:

* `src/contexts/agent_execution/policy.rs`

### Responsibilities

* parse backend specs
* role-model injection
* timeout resolution
* opposite-family lookup
* alternation policy
* panel backend effective resolution

### Methods to implement

```rust
resolve_role_target(...)
resolve_completion_panel(...)
resolve_prompt_review_panel(...)
resolve_final_review_panel(...)
timeout_for_role(...)
opposite_family(...)
planner_family_for_cycle(...)
```

### Panel rules

#### Completion panel

* optional backend specs prefixed with `?`
* unavailable optional backends are skipped
* unavailable required backends fail
* after filtering, available backends must still satisfy `min_completers`

#### Prompt review validator panel

* same optional/required filtering
* available validators must still satisfy `min_reviewers`

#### Final review panel

* same optional/required filtering
* available reviewers must still satisfy `min_reviewers`

## 5. Role timeouts

Add support for config keys like:

* `backends.claude.role_timeouts.planner`
* `backends.codex.role_timeouts.prompt_reviewer`
* `backends.openrouter.role_timeouts.final_reviewer`

Port the old behavior reference from:

* `src/validate/tests_role_timeouts.rs`

## 6. Backend drift snapshot and warning support

Every time a stage begins, persist `StageResolutionSnapshot`.

On resume:

* re-resolve the current stage’s backend(s)
* compare against persisted snapshot
* if different, append runtime warning and journal warning event
* proceed with the newly resolved value

This must cover:

* implementation
* QA
* review
* completion planner
* completion panel backends
* final review planner
* prompt review refiner/validators

Port behavior reference from:

* `src/validate/tests_resume_backend_resolution.rs`

## Files to modify/add

### Add

* `src/composition/agent_execution_builder.rs`
* `src/adapters/openrouter_backend.rs`
* `src/contexts/agent_execution/policy.rs`

### Modify

* `src/cli/run.rs`
* `src/cli/requirements.rs`
* `src/contexts/automation_runtime/daemon_loop.rs`
* `src/contexts/agent_execution/model.rs`
* `src/contexts/agent_execution/service.rs`
* `src/adapters/process_backend.rs`
* `src/shared/domain.rs`
* `src/contexts/workspace_governance/config.rs`
* `tests/unit/agent_execution_test.rs`
* `tests/conformance/features/*`

## Acceptance scenarios to add

* `backend.requirements.real_backend_path`
* `backend.openrouter.model_injection`
* `backend.openrouter.disabled_default_backend`
* `backend.role_overrides.per_role_override_beats_default`
* `backend.role_timeouts.config_roundtrip`
* `backend.resume_drift.implementation_warns_and_reresolves`
* `backend.resume_drift.qa_warns_and_reresolves`
* `backend.resume_drift.review_warns_and_reresolves`
* `backend.resume_drift.completion_panel_warns_and_reresolves`

---

# Workstream 2: Workflow semantics parity

## Goal

Bring workflow behavior to P0 parity by adding the missing high-value semantics from old `ralph`:

* git checkpoint/commit semantics
* prompt review panel
* completion panel consensus
* full final-review panel/vote/arbiter behavior
* prompt change policy
* separate QA/review/final-review caps

## Current gap

The shared flow engine exists, but it currently simplifies several old behaviors:

* prompt review is a single stage, not refiner + validator panel
* completion panel is not a real multi-backend consensus system
* final review is not a reviewer panel with votes and arbiter
* checkpoint/commit semantics are much simpler than old loop checkpoints
* prompt change policy is not active
* QA/review/final-review caps are not distinct enough

## Target behavior

## 1. Prompt review parity

### Flow

1. run prompt refiner backend
2. persist supporting payload/artifact for refinement
3. run validator panel on the refined prompt
4. if any validator rejects, prompt review fails
5. if available validators < min_reviewers, fail
6. if all validators accept and min_reviewers satisfied:

   * persist `prompt.original.md`
   * replace project prompt with refined prompt
   * update project prompt hash
   * continue into planning

### Optional validator rules

* optional validators may be skipped if unavailable
* required validator unavailability fails

### Aggregation rule

* any rejection fails
* all executed validators must accept
* executed validator count must be `>= min_reviewers`

## 2. Completion panel parity

### Flow

1. planner-like stage requests completion
2. resolve effective completer panel backends
3. each completer returns a structured verdict
4. persist per-completer supporting payload/artifact
5. compute consensus:

   * `complete_votes >= min_completers`
   * `complete_votes / total_voters >= threshold`
6. persist aggregate stage-primary payload/artifact
7. if aggregate verdict is `complete`, continue to acceptance QA
8. else reopen work

### Exact consensus function

Use the old math:

```text
complete if:
  complete_votes >= min_completers
  AND
  complete_votes / total_voters >= consensus_threshold
```

Comparison is inclusive.

## 3. Final review parity

### Flow

1. resolve reviewer panel backends
2. each reviewer returns proposed amendments
3. merge amendments into a canonical amendment set with deterministic ids
4. if no amendments proposed, complete immediately
5. planner stage writes positions for all amendments
6. each reviewer votes ACCEPT/REJECT per amendment
7. compute consensus:

   * accepted if accept_ratio >= threshold
   * rejected if accept_count == 0
   * disputed otherwise
8. arbiter stage resolves only disputed amendments
9. final accepted set =

   * consensus.accepted
   * plus arbiter.accepted
10. if final accepted set empty, complete
11. else enqueue amendments, increment final-review restart count, restart planning
12. if restart count exceeds max, force-complete with explicit artifact

### Canonical amendment ids

Use a deterministic id strategy:

```text
fr-<completion-round>-<sha256(normalized-body)>[:8]
```

Do not use positional ids.

### Reviewer merge rule

Amendments with identical normalized body hash collapse into one canonical amendment id, preserving source reviewers in metadata.

## 4. Prompt change policy parity

Add config:

```toml
[workflow]
prompt_change_action = "continue" | "abort" | "restart_cycle"
```

### Resume behavior

Before resuming:

* hash current `prompt.md`
* compare against `active_run.prompt_hash_at_cycle_start`

If different:

* `continue` → log warning and continue
* `abort` → fail resume with clear error
* `restart_cycle` → roll back logical cycle state to planning stage for the current cycle, clear supporting stage data for the in-progress portion, optionally clear sessions according to session policy

## 5. Distinct iteration controls

Add config:

```toml
[workflow]
max_qa_iterations = 3
max_review_iterations = 3

[final_review]
max_restarts = 2
```

### Rules

* QA failure increments `qa_iterations_current_cycle`
* review change request increments `review_iterations_current_cycle`
* final review restart increments `final_review_restart_count`
* hitting a cap yields a defined failure or rollback path
* caps are separate and independently enforced

## 6. Explicit VCS checkpoint semantics

Current SHA capture is not enough.

### Add port

Add `VcsCheckpointPort` with:

```rust
create_checkpoint(...)
find_checkpoint(...)
reset_to_checkpoint(...)
```

### Checkpoint creation rule

Create a logical+VCS checkpoint after each successful **primary** stage that changes run progression.

Minimum required checkpoints:

* prompt review completion
* implementation / apply-fixes / docs update / ci update
* review approval
* completion round aggregate
* acceptance QA pass
* final review exit

### Commit message format

Use a stable internal format:

```text
rb: checkpoint project=<project-id> stage=<stage-id> cycle=<n> round=<m>
```

Include machine-readable trailers:

```text
RB-Project: <project-id>
RB-Run: <run-id>
RB-Stage: <stage-id>
RB-Cycle: <n>
RB-Completion-Round: <m>
```

### Rollback rule

* logical rollback always happens first
* hard rollback uses checkpoint ref if available
* hard rollback failure does not undo logical rollback

## Implementation design

## 1. Extract panel behavior out of `engine.rs`

Add:

* `src/contexts/workflow_composition/prompt_review.rs`
* `src/contexts/workflow_composition/completion.rs`
* `src/contexts/workflow_composition/final_review.rs`
* `src/contexts/workflow_composition/checkpoints.rs`
* `src/contexts/workflow_composition/drift.rs`

`engine.rs` should orchestrate stage transitions, not hold the entire panel logic inline.

## 2. Add supporting contract types

Keep public `StageId` unchanged for high-level flow topology.

Add stage-internal supporting contracts:

* `PromptRefinementContract`
* `PromptReviewValidatorContract`
* `CompletionVerdictContract`
* `FinalReviewProposalContract`
* `PlannerPositionContract`
* `FinalReviewVoteContract`
* `FinalReviewArbiterContract`

Place them in:

* `src/contexts/workflow_composition/panel_contracts.rs`

These should:

* produce structured payloads
* render supporting history artifacts
* never replace the primary stage result for resume logic

## 3. State changes

### `RunSnapshot`

Add:

* `last_failure: Option<FailedStageSummary>`
* `prompt_hash_at_last_successful_cycle_start: Option<String>` if needed for simpler queries

### `ActiveRun`

Add fields described earlier.

## 4. Query behavior

Update history/tail queries to:

* show supporting panel records
* distinguish primary vs supporting artifacts
* expose completion round and backend details

## Files to modify/add

### Add

* `src/contexts/workflow_composition/panel_contracts.rs`
* `src/contexts/workflow_composition/prompt_review.rs`
* `src/contexts/workflow_composition/completion.rs`
* `src/contexts/workflow_composition/final_review.rs`
* `src/contexts/workflow_composition/checkpoints.rs`
* `src/contexts/workflow_composition/drift.rs`

### Modify

* `src/contexts/workflow_composition/engine.rs`
* `src/contexts/workflow_composition/contracts.rs`
* `src/contexts/workflow_composition/payloads.rs`
* `src/contexts/workflow_composition/renderers.rs`
* `src/contexts/project_run_record/model.rs`
* `src/contexts/project_run_record/service.rs`
* `src/contexts/project_run_record/queries.rs`
* `src/adapters/fs.rs`
* `src/adapters/worktree.rs`
* `src/cli/run.rs`

## Acceptance scenarios to add

### Prompt review

* `workflow.prompt_review.panel_accept`
* `workflow.prompt_review.panel_reject`
* `workflow.prompt_review.min_reviewers_enforced`
* `workflow.prompt_review.optional_validator_skip`
* `workflow.prompt_review.prompt_replaced_and_original_preserved`

### Completion

* `workflow.completion.panel_two_completer_consensus_complete`
* `workflow.completion.panel_continue_verdict`
* `workflow.completion.optional_backend_skip`
* `workflow.completion.required_backend_failure`
* `workflow.completion.threshold_consensus`
* `workflow.completion.insufficient_min_completers`

### Final review

* `workflow.final_review.no_amendments_complete`
* `workflow.final_review.restart_then_complete`
* `workflow.final_review.planner_completion_with_pending_amendments_fails`
* `workflow.final_review.disputed_amendment_uses_arbiter`
* `workflow.final_review.restart_cap_force_complete`

### Prompt change and drift

* `workflow.resume.prompt_change_continue_warns`
* `workflow.resume.prompt_change_abort_fails`
* `workflow.resume.prompt_change_restart_cycle`
* `workflow.resume.backend_drift_warns`

### Checkpoints

* `workflow.rollback.hard_uses_checkpoint`
* `workflow.checkpoint.commit_metadata_stable`

---

# Workstream 3: Validation/runtime parity

## Goal

Bring local validation behavior to P0 parity for:

* standard flow
* docs flow
* CI flow
* pre-commit checks

## Current gap

Current validation profiles are descriptive but not truly operational.

What is missing:

* local command execution for docs/CI/standard validation
* pre-commit gate after approval
* repo-specific command configuration
* durable validation evidence records

## Target behavior

## 1. Validation runner port

Add `ValidationRunnerPort` that can execute configured command groups and return structured results.

### Result shape

```rust
pub struct ValidationCommandResult {
    pub command: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub passed: bool,
}

pub struct ValidationGroupResult {
    pub group_name: String,
    pub passed: bool,
    pub commands: Vec<ValidationCommandResult>,
}
```

## 2. Docs and CI validation stages are local-validation stages

For `docs_change` and `ci_improvement`:

* `DocsValidation`
* `CiValidation`

should execute local validation commands directly.

### Stage result mapping

* if all commands pass → `ValidationPayload.outcome = approved`
* if any command fails → `ValidationPayload.outcome = request_changes`

The payload should include:

* evidence summaries
* failing command excerpts
* follow-up items

No backend call is required for these local validation stages in P0.

## 3. Standard flow validation integration

For `standard`:

* keep QA and review as agent stages
* add local standard validation evidence before or during review prompt construction
* include local validation results in the review context
* keep pre-commit gate after review approval

## 4. Pre-commit parity

Implement old behavior equivalent to `src/workflow/pre_commit_checks.rs`.

### Required checks

* cargo fmt --check
* cargo clippy --all-targets -- -D warnings
* nix build

Controlled by config booleans.

### Auto-fix rule

If `pre_commit_fmt_auto_fix = true`:

* on `cargo fmt --check` failure, run `cargo fmt`
* if auto-fix succeeds, fmt check counts as passed
* otherwise it fails

### Behavior on failure

* reviewer approval is invalidated
* a remediation request is stored
* run returns to implementation remediation
* a durable supporting validation artifact is written
* a runtime log entry is written

## Design

## 1. Add validation adapter

Add:

* `src/adapters/validation_runner.rs`

It should:

* run command groups using `sh -lc` in repo root
* capture stdout/stderr
* enforce per-command timeouts
* return structured results
* never mutate run state directly

## 2. Add workflow policy wrapper

Add:

* `src/contexts/workflow_composition/validation.rs`

Responsibilities:

* resolve which validation group to run
* convert `ValidationGroupResult` into `ValidationPayload`
* attach supporting artifacts/logs
* perform post-review pre-commit gate

## 3. Config keys

Use the config model from the shared config foundation:

```toml
[validation]
standard_commands = ["cargo test -q"]
docs_commands = ["markdownlint docs/**/*.md", "lychee docs/**/*.md"]
ci_commands = ["actionlint", "shellcheck .github/scripts/*.sh"]

pre_commit_fmt = true
pre_commit_clippy = true
pre_commit_nix_build = false
pre_commit_fmt_auto_fix = false
```

## Files to modify/add

### Add

* `src/adapters/validation_runner.rs`
* `src/contexts/workflow_composition/validation.rs`

### Modify

* `src/contexts/workflow_composition/engine.rs`
* `src/contexts/workflow_composition/payloads.rs`
* `src/contexts/workflow_composition/renderers.rs`
* `src/shared/domain.rs`
* `src/contexts/workspace_governance/config.rs`

## Acceptance scenarios to add

* `validation.docs.commands_pass`
* `validation.docs.command_failure_requests_changes`
* `validation.ci.commands_pass`
* `validation.ci.command_failure_requests_changes`
* `validation.standard.review_context_contains_local_validation`
* `validation.pre_commit.disabled_skips_checks`
* `validation.pre_commit.no_cargo_toml_skips_cargo_checks`
* `validation.pre_commit.fmt_failure_triggers_remediation`
* `validation.pre_commit.fmt_auto_fix_succeeds`
* `validation.pre_commit.nix_build_failure_records_feedback`

---

# Workstream 4: GitHub/daemon parity

## Goal

Bring daemon automation to P0 parity by adding:

* real GitHub intake
* multi-repo `--data-dir`
* labels and explicit command routing
* draft PR lifecycle
* PR review/amendment ingestion
* rebase/runtime support

## Current gap

Current daemon behavior is local-only and file-watcher-based:

* `FileIssueWatcher` reads `.ralph-burning/daemon/watched/*.json`
* no real GitHub adapter
* no multi-repo controller
* no draft PR watcher
* no PR review ingestion
* no real label management

## Target behavior

## 1. New daemon command surface

Add parity-oriented CLI:

```text
ralph-burning daemon start --data-dir <dir> --repo <owner/repo>... [--poll-seconds N] [--single-iteration] [--verbose]
ralph-burning daemon status --data-dir <dir> [--repo <owner/repo>...]
ralph-burning daemon abort <issue-number> --data-dir <dir> --repo <owner/repo>
ralph-burning daemon retry <issue-number> --data-dir <dir> --repo <owner/repo>
ralph-burning daemon reconcile --data-dir <dir>
```

This replaces the current workspace-local daemon assumption for P0 parity.

## 2. Multi-repo daemon data model

Use this layout:

```text
<data-dir>/
  repos/
    <owner>/
      <repo>/
        repo/               # cloned repo checkout
        worktrees/
        daemon/
          tasks/
          leases/
          journal.ndjson
```

Each cloned repo must contain its own `.ralph-burning/` workspace for project/run state.

Daemon state lives in `data-dir`, not inside the repo workspace.

## 3. GitHub adapter

Add:

* `src/adapters/github.rs`

### Required port

Add `GitHubPort` with methods for:

* ensure labels exist
* list candidate issues by labels
* read issue labels
* add/remove/replace labels
* fetch issue comments
* fetch PR review comments
* fetch PR review summaries
* post idempotent comments
* create draft PR
* mark PR ready
* close PR
* get PR URL
* detect branch ahead-of-base
* update PR description/body
* fetch current PR state

Provide:

* `GhCliGitHubAdapter`
* `MockGitHubAdapter`

## 4. Routing model

### Label vocabulary

Use:

* `rb:ready`
* `rb:in-progress`
* `rb:failed`
* `rb:completed`
* `rb:flow:standard`
* `rb:flow:quick_dev`
* `rb:flow:docs_change`
* `rb:flow:ci_improvement`
* `rb:requirements`
* `rb:waiting-feedback`

### Explicit commands

Use:

* `/rb flow <preset>`
* `/rb requirements`
* `/rb run`
* `/rb retry`
* `/rb abort`

### Routing precedence

1. explicit command
2. flow label
3. repo default routing policy

## 5. Repo bootstrap and clone behavior

At daemon start:

* validate `--repo owner/repo`
* clone repo into `data-dir/repos/<owner>/<repo>/repo` if absent
* if clone target exists and is a valid git repo, reuse it
* ensure `.ralph-burning` workspace exists inside the repo checkout
* load repo-local workspace config
* ensure labels exist on the remote repo

## 6. Worktree lifecycle

Use per-task worktrees under:

```text
<data-dir>/repos/<owner>/<repo>/worktrees/<task-id>/
```

Each task gets:

* one lease
* one branch
* one worktree root

Branch naming rule:

```text
rb/<issue-number>-<project-id>
```

## 7. Draft PR watcher parity

### Required behavior

* when task branch first moves ahead of base, push branch and create a draft PR
* do not create duplicate PRs
* persist PR URL on task metadata
* if task completes with no diff, close or skip PR creation according to policy
* on successful task completion, optionally mark PR ready
* watcher must cancel cleanly

## 8. PR review/amendment ingestion

### Required behavior

* fetch:

  * inline review comments
  * top-level PR comments
  * review summary comments
* restrict ingestion to whitelisted users
* deduplicate by source kind + source id
* convert accepted review text into amendments
* persist staged amendments before dispatch
* do not lose staged amendments on restart or transient API failure

### Amendment source keys

Use stable dedup keys:

```text
pull_comment:<id>
issue_comment:<id>
review:<id>
```

### Behavior on existing completed project

If new PR review amendments are ingested for a completed project:

* reset the project to an active state appropriate for the preset
* enqueue amendments
* dispatch remediation work

## 9. Rebase/runtime support

### Required behavior

* task may rebase onto default branch
* rebase conflicts may be resolved via configured backend policy
* if rebase fails terminally, task moves to failed with preserved worktree
* rebase state is journaled

This is P0 only to the extent needed for old daemon rebase parity. Do not add broader branch-sync features beyond that.

## Design

## 1. Add GitHub port and adapter

### Add

* `src/contexts/automation_runtime/github.rs` for port definitions if preferred
* `src/adapters/github.rs` for adapter implementation

### Do not reuse `FileIssueWatcher` as the production path

Keep it for tests only.

## 2. Refactor daemon store root

Current daemon store is workspace-root-based.

Change daemon CLI and store composition so daemon runtime receives:

* `data_dir`
* repo registration metadata
* repo checkout path
* repo workspace path

Do not let daemon runtime assume current directory is the only repo root.

## 3. Add repo registry model

Add to daemon model:

```rust
pub struct RepoRegistration {
    pub owner: String,
    pub repo: String,
    pub repo_slug: String,
    pub repo_root: PathBuf,
    pub workspace_root: PathBuf,
    pub base_branch: String,
}
```

## 4. Update `DaemonTask`

Add fields:

* `repo_slug`
* `repo_root`
* `workspace_root`
* `issue_number`
* `pr_url`
* `dedup_cursor` or `last_seen_comment_ids`

## 5. Replace watcher abstraction

Current `IssueWatcherPort::poll(base_dir)` is too small.

Replace with:

```rust
pub trait IssueSourcePort {
    fn poll_candidates(&self, repo: &RepoRegistration) -> AppResult<Vec<WatchedIssueMeta>>;
}
```

Provide:

* `GitHubIssueSource`
* `FileIssueSource` for tests

## Files to modify/add

### Add

* `src/adapters/github.rs`
* `src/contexts/automation_runtime/repo_registry.rs`
* `src/contexts/automation_runtime/pr_runtime.rs`
* `src/contexts/automation_runtime/pr_review.rs`
* `src/contexts/automation_runtime/github_intake.rs`

### Modify

* `src/cli/daemon.rs`
* `src/contexts/automation_runtime/model.rs`
* `src/contexts/automation_runtime/mod.rs`
* `src/contexts/automation_runtime/routing.rs`
* `src/contexts/automation_runtime/task_service.rs`
* `src/contexts/automation_runtime/lease_service.rs`
* `src/contexts/automation_runtime/daemon_loop.rs`
* `src/adapters/worktree.rs`

## Acceptance scenarios to add

### Intake and routing

* `daemon.github.start_validates_repos_and_data_dir`
* `daemon.github.multi_repo_status`
* `daemon.routing.command_beats_label`
* `daemon.routing.label_used_when_no_command`
* `daemon.labels.ensure_on_startup`

### Task lifecycle

* `daemon.tasks.abort_by_issue_number`
* `daemon.tasks.retry_failed_issue`
* `daemon.tasks.reconcile_stale_leases`
* `daemon.tasks.worktree_isolation`

### Draft PR runtime

* `daemon.pr_runtime.create_draft_when_branch_ahead`
* `daemon.pr_runtime.push_before_create`
* `daemon.pr_runtime.clean_shutdown_on_cancel`
* `daemon.pr_runtime.no_diff_close_or_skip`

### PR review ingestion

* `daemon.pr_review.whitelist_filters_comments`
* `daemon.pr_review.dedup_across_restart`
* `daemon.pr_review.transient_error_preserves_staged`
* `daemon.pr_review.completed_project_reopens_with_amendments`

### Rebase

* `daemon.rebase.agent_resolves_conflict`
* `daemon.rebase.disabled_agent_aborts_conflict`
* `daemon.rebase.timeout_classification`

---

# Implementation sequence

This is the recommended order for an agent.

## Slice 1: Config and backend policy foundation

Implement:

* expanded config structs
* project `config.toml`
* effective config merge
* backend policy service
* role timeouts
* run CLI overrides

Do not touch daemon/GitHub yet.

## Slice 2: Real requirements backend path

Implement:

* shared `AgentExecutionService` builder
* remove direct stub use in requirements CLI
* support requirements contracts in process adapter

## Slice 3: OpenRouter support

Implement:

* OpenRouter adapter
* availability/capability checks
* tests

## Slice 4: Prompt review + completion panel

Implement:

* prompt review panel
* completion panel consensus
* supporting payload/artifact records
* drift snapshot persistence

## Slice 5: Final review + prompt-change + iteration caps

Implement:

* final-review reviewer proposals
* vote stage
* arbiter stage
* cap semantics
* prompt change policy
* QA/review/final-review iteration counters

## Slice 6: Validation runner + pre-commit

Implement:

* local validation runner
* docs/CI validation stage integration
* standard validation evidence
* pre-commit gate

## Slice 7: Checkpoint commits and hard rollback fidelity

Implement:

* VCS checkpoint port
* checkpoint commit creation
* hard rollback against checkpoint refs

## Slice 8: GitHub adapter + multi-repo daemon

Implement:

* `--data-dir`
* `--repo`
* repo registry
* GitHub intake
* label ensure/status/abort/retry

## Slice 9: Draft PR runtime + PR review ingestion + rebase

Implement:

* draft PR watcher
* PR URL plumbing
* PR review ingestion into amendments
* rebase/runtime parity

## Slice 10: Conformance catch-up

Add conformance scenarios for every item above before marking P0 complete.

---

# Definition of done

P0 is complete only when all of the following are true:

## Backend/runtime

* `requirements` and daemon requirements paths use real backends
* OpenRouter works end-to-end
* per-role backend overrides work
* role timeouts work
* backend drift warnings on resume work

## Workflow

* prompt review uses refiner + validator panel
* completion panel supports min/threshold consensus
* final review supports reviewer proposals + votes + arbiter
* prompt change policy works
* QA/review/final-review caps work
* git checkpoints are created and used by hard rollback

## Validation

* docs and CI presets run real local validation commands
* standard flow can inject local validation evidence
* pre-commit checks gate review approval

## Daemon/GitHub

* daemon can run against multiple repos from `--data-dir`
* GitHub labels and commands drive routing
* draft PR watcher works
* PR review comments become amendments
* rebase path works at parity level

## Conformance

New conformance coverage exists for all P0 behaviors listed in this spec.

---

# Agent execution notes

## Safe implementation boundaries

An agent should not try to do all P0 work in one branch.

Preferred branch sequence:

1. config + agent execution
2. prompt review + completion
3. final review + drift + prompt change
4. validation runner
5. checkpoints/rollback
6. daemon/GitHub
7. conformance backfill

## Areas that must be stabilized before parallel work

* config schema
* `ActiveRun` additions
* `PayloadRecord` / `ArtifactRecord` metadata expansion
* `InvocationContract` schema API
* daemon data-dir layout
* GitHub label/command vocabulary

## Areas where agents are likely to conflict

* `src/shared/domain.rs`
* `src/contexts/workspace_governance/config.rs`
* `src/contexts/workflow_composition/engine.rs`
* `src/contexts/project_run_record/model.rs`
* `src/cli/daemon.rs`

Those should have a single owner per slice.

---

# Non-goals reminder

This spec does not include:

* tmux
* streaming UI parity
* `auto`
* manual `amend`
* backend CLI diagnostics
* template override system
* full old PRD pipeline depth
* legacy workspace/artifact compatibility

Those should remain out of the agent’s scope for this work.

If you want, I’ll turn this into a **slice-by-slice agent task pack** with concrete file edits, DTO changes, and conformance cases for each slice.


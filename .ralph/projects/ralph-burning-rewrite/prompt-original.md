# Repository Layout

```
new-ralph-burning/
├── multibackend-orchestration/   # legacy codebase (read-only reference)
├── ralph-burning-rewrite/        # new code we are creating
└── rb.md                         # this spec
```

* **`multibackend-orchestration/`** — the original codebase. Treat as **read-only**. All `src/` paths referenced in this document (e.g. `src/workflow/orchestrator.rs`) live under this directory.
* **`ralph-burning-rewrite/`** — the greenfield rewrite target. All new code goes here.

# Executive Summary

The legacy codebase (`multibackend-orchestration/`) is a local AI engineering orchestrator with a much broader scope than “a Rust CLI that runs planner / implementer / reviewer loops.” It currently contains at least these real capabilities:

* structured delivery orchestration with planning, implementation, QA, review, completion, and final review
* a separate quick-dev workflow
* workspace and project state management under `.ralph/`
* PRD generation pipelines
* daemon / GitHub issue and PR automation
* a large binary-level conformance suite

That picture is visible in `src/workflow/{orchestrator,quick_dev_orchestrator}.rs`, `src/project/*`, `src/backend/*`, `src/prd/*`, `src/daemon/*`, and `src/validate/*` (all under `multibackend-orchestration/`).

The rewrite lives in **`ralph-burning-rewrite/`**, with a new workspace root **`.ralph-burning/`** and a fully new storage model. The legacy codebase should be used as a **behavior and domain reference only**. The rewrite should not preserve or read/write legacy `.ralph` state, legacy markdown artifact formats, legacy loop directories, or legacy git checkpoint encodings.

The recommended target is a **Rust modular monolith** with explicit bounded contexts and selective hexagonal architecture around real side-effect boundaries. The most important design changes are:

* replace “standard orchestrator vs quick-dev orchestrator” with a **shared workflow engine**
* make **flow presets** first-class and fixed per project
* support exactly these built-in flow presets in v1:

  * `standard`
  * `quick_dev`
  * `docs_change`
  * `ci_improvement`
* replace markdown-as-machine-protocol with **structured stage contracts** using validated JSON payloads
* keep markdown as a **rendered human artifact**, not as the workflow protocol
* separate **durable project history** from **ephemeral runtime/debug logs**
* modernize the CLI around coherent nouns and verbs

Recommended bounded contexts:

* **Workspace Governance**
* **Project & Run Record**
* **Workflow Composition**
* **Agent Execution**
* **Requirements Drafting**
* **Automation Runtime**
* **Conformance Specification**

The biggest risks are architectural, not technical:

* rebuilding giant orchestrators under new names
* over-generalizing into a workflow DSL too early
* letting stage markdown remain a protocol instead of rendered output
* allowing flow-specific state to leak back into the project model
* under-specifying operator-facing behavior like `history`, `tail`, `rollback`, `flow show`, and daemon routing

The right rewrite is decisive: a greenfield modular monolith, typed workflow kernel, built-in flow presets, mandatory structured stage payloads across all supported backends, and a new conformance suite that captures desired behavior rather than legacy file formats.

# Current System Summary

## Core purpose

Today the repo implements a local-first AI software-delivery orchestrator that coordinates multiple AI backends to perform engineering work inside a repository. It persists state locally, integrates with git, produces readable artifacts, and exposes operator controls for running, inspecting, resuming, validating, rolling back, generating PRDs, and automating GitHub-driven work.

## Main user/operator workflows in the current system

### Standard delivery flow

The main workflow currently includes:

* optional prompt review
* planning
* implementation
* QA
* review
* commit/checkpoint progression
* completion panel / acceptance QA
* final review and amendment restart logic

This is concentrated in `src/workflow/orchestrator.rs`, with persistence and artifact support spread across `src/project/*`, `src/git/*`, `src/workspace/*`, and `src/config/*`.

### Quick-dev flow

The current repo has a separate flow with its own orchestration path:

* PlanAndImplement
* CodexReview
* ApplyFixes
* FinalReview

This is implemented in `src/workflow/quick_dev_orchestrator.rs` and surfaced through separate CLI commands in `src/cli/quick_dev_*`.

### PRD workflows

The repo contains at least three requirements-related paths:

* staged PRD pipeline in `src/prd/pipeline.rs`
* quick PRD in `src/prd/quick.rs`
* interactive PRD issue workflow in `src/daemon/interactive_prd.rs`

### Automation / daemon workflows

The daemon is a real subsystem, not a thin wrapper. It includes:

* polling and claiming GitHub issues
* worktree management
* task dispatch
* draft PR lifecycle
* rebase handling
* interactive PRD issue advancement

That behavior is concentrated in `src/daemon/{runtime,tasks,github,worktree,rebase_agent,interactive_prd}.rs`.

## Runtime shape

The current runtime is one Rust binary with multiple operating modes:

* direct CLI commands
* long-running daemon
* PRD pipeline execution
* validate/conformance execution

It depends on:

* local workspace/project state under `.ralph/`
* local git repository/worktrees
* external backend CLIs
* optional tmux
* GitHub / `gh` integration for daemon paths

## Main moving parts

### `src/backend/`

Contains backend spec parsing, backend selection, model injection, subprocess execution, tmux integration, output normalization, and backend-specific quirks.

### `src/workflow/`

Contains the main orchestration logic and the separate quick-dev orchestration logic.

### `src/project/`

Contains state types, lifecycle logic, artifact writing, amendment queueing, and state reconstruction behavior.

### `src/workspace/` and `src/config/`

Contain workspace discovery, active project selection, config loading, template path resolution, and effective policy assembly.

### `src/prd/`

Contains staged PRD generation, validation, caching, questions, and quick-PRD review loops.

### `src/daemon/`

Contains issue routing, task lifecycle, worktree behavior, PR watcher behavior, rebase, and interactive PRD.

### `src/validate/`

Contains a broad executable behavior suite and is the strongest evidence of current external behavior.

## Important current strengths

* The repo already captures real product behavior, not just internal architecture ideas.
* The conformance suite is broad and valuable.
* The system has meaningful operator recovery concepts: status, history, tail, resume, rollback.
* Backend handling is explicit and more mature than a typical “single model” CLI.
* PRD and daemon are real capabilities, not side scripts.
* Readable artifacts are genuinely useful for debugging and audit.

## Important current weaknesses

* The biggest weakness is the split between the standard/full workflow and quick-dev. That makes adding new flows expensive and invasive.
* `ProjectState` is carrying flow-specific state instead of hosting a clean, generic run model.
* The standard orchestrator mixes policy with filesystem, git, backend, template, and parsing mechanics.
* Markdown structure is being used as a machine contract, which creates reformat and retry problems.
* Current state handling leans too heavily on reconstruction and artifacts rather than canonical state.
* The current CLI surface encodes implementation history more than a coherent product model.

## What is intentional vs accidental

### Intentional

* project/workspace model
* role-based AI orchestration
* durable readable artifacts
* backend abstraction and model selection
* PRD as a separate capability
* daemon automation
* conformance validation

### Accidental

* separate orchestrators for different flows
* flow-specific fields embedded in shared project state
* markdown syntax as the workflow protocol
* giant policy-mechanism files
* command names and surfaces reflecting historical growth rather than product shape
* storage formats overly tied to old runtime behavior

# Capability Map

## Core capabilities

### Delivery workflow orchestration

**Value**
Moves a project from prompt to completed engineering work through explicit stages.

**Owns behaviors**

* stage progression
* planning, implementation, QA, review
* stop conditions
* resume behavior
* completion entry
* final acceptance entry

### Flow preset execution

**Value**
Allows different kinds of work to use different stage topologies without becoming separate products.

**Owns behaviors**

* selecting a built-in flow preset
* enforcing stage legality for that preset
* keeping the preset fixed per project
* using preset-specific validation and final-review rules

### Project and run state management

**Value**
Gives each project durable identity, prompt, flow, run state, history, rollback points, and operator read models.

**Owns behaviors**

* project creation/selection/deletion
* run state persistence
* work cycle tracking
* completion round tracking
* sessions
* amendment queue
* history/status/tail/rollback

### Structured stage output handling

**Value**
Makes workflow behavior depend on validated structured payloads instead of markdown syntax.

**Owns behaviors**

* stage payload validation
* semantic validation after schema validation
* deterministic artifact rendering
* durable storage of payloads and artifacts
* rejection of malformed stage outputs

### Backend and model execution

**Value**
Lets roles run on multiple backends while keeping invocation behavior predictable.

**Owns behaviors**

* backend spec parsing
* model selection
* per-role backend assignment
* invocation
* streaming
* sessions
* cancellation/timeouts
* normalized output envelopes

### Completion and final acceptance

**Value**
Prevents “done” from being one agent’s opinion.

**Owns behaviors**

* completion rounds
* completion panel verdicts
* acceptance QA
* final review when enabled by preset
* amendment acceptance/rejection
* restart and cap behavior

## Supporting capabilities

### Prompt review

**Value**
Improves project input before execution begins.

**Owns behaviors**

* prompt review stage
* validator voting/acceptance
* prompt replacement when approved

### Validation profile execution

**Value**
Applies different repo checks based on the type of work.

**Owns behaviors**

* `standard` validation profile
* `quick_dev` validation profile
* `docs_change` validation profile
* `ci_improvement` validation profile
* pre-commit and targeted repo checks

### Requirements drafting

**Value**
Produces implementation-ready requirements before project execution.

**Owns behaviors**

* staged requirements draft flow
* quick requirements draft flow
* question/answer cycles
* validation reports
* resume/cache

### Project bootstrapping from requirements

**Value**
Lets approved requirements become executable projects cleanly.

**Owns behaviors**

* project creation from requirements output
* prompt/project handoff
* optional convenience workflow composition

### Automation runtime

**Value**
Turns issues, labels, and commands into orchestrated execution.

**Owns behaviors**

* daemon polling and dispatch
* label-based routing
* explicit command routing
* worktree leasing
* task lifecycle
* PR/watcher behavior
* rebase flows

### Operator inspection and recovery surface

**Value**
Makes the tool safe to operate.

**Owns behaviors**

* `run status`
* `run history`
* `run tail`
* `run rollback`
* daemon status / abort / retry / reconcile
* flow discovery commands

## Generic / infrastructure capabilities

### Filesystem persistence

Stores workspace config, project state, payloads, artifacts, logs, PRD state, and daemon state.

### Git/worktree integration

Supports branch setup, checkpoints, resets, worktree isolation, rebase, and repository synchronization.

### Prompt/template loading

Loads templates for workflow stages and requirements drafting.

### Process/tmux management

Runs external CLIs, manages windows/sessions, captures logs, and handles timeout/cancel cleanup.

### Conformance harness

Runs the real binary in temp workspaces/repos with mock backends and mock GitHub behavior.

# Ubiquitous Language

**ralph-burning**
The rewritten product name. Use this consistently in docs, CLI, binary naming, and workspace naming.

**workspace**
The orchestration root at `.ralph-burning/`.
Do not use it to mean the repository root.

**repository root**
The git or worktree root where code lives. This is distinct from the workspace root.

**project**
A durable work item with:

* id
* name
* prompt
* fixed flow preset
* run state
* history
* artifacts
* logs
* rollback points

**flow preset**
A built-in, code-defined workflow topology such as:

* `standard`
* `quick_dev`
* `docs_change`
* `ci_improvement`

A flow preset is selected at project creation and is immutable for that project.

**run**
An execution instance for a project under its fixed flow preset.

**stage**
A named step in a flow preset. Stage is the normalized term replacing today’s overloaded phase language.

**stage cursor**
The current durable position in a run:

* current stage
* current cycle
* current iteration/attempt metadata

**work cycle**
The normal unit of iterative delivery work. This replaces today’s overloaded “loop” internally. CLI may still expose “cycle” rather than “loop.”

**completion round**
A distinct round for deciding complete vs continue. It is not the same as a work cycle.

**role**
A logical actor in a stage, such as planner, implementer, reviewer, QA, completer, or final reviewer.

**backend family**
A backend provider family such as Claude, Codex, or OpenRouter.

**backend spec**
A typed backend selection plus optional model spec.

**model spec**
The backend-native model identifier.

**stage contract**
The machine contract for a stage’s output:

* schema
* required fields
* semantic validation rules

**stage payload**
The canonical structured output for a stage after validation.

**history artifact**
A durable, human-readable markdown rendering derived from a validated stage payload and stored as project history.

**runtime log**
Ephemeral operational or debug output for immediate troubleshooting. Runtime logs are not part of durable project history.

**journal**
An append-only event stream of run and project transitions.

**validation profile**
A reusable set of validation behaviors, such as:

* `standard`
* `quick_dev`
* `docs_change`
* `ci_improvement`

**requirements draft**
The modernized external CLI term for what the old repo often called PRD generation.

**session record**
A durable record for backend session continuity, if used.

**amendment request**
A requested change to be reintroduced into work, often after final review or external direction.

**rollback point**
A durable reference to a logical and optionally VCS-backed rewind target.

**task**
A daemon-owned unit of automated work, usually associated with an issue or PR.

**routing command**
An explicit issue/PR command such as `/rb run` or `/rb flow docs_change`.

**routing label**
A label used by the daemon to infer dispatch behavior, such as `rb:flow:ci_improvement`.

# Bounded Contexts

Before defining contexts, the repo-specific starting hypotheses should be resolved like this:

* **Workflow Orchestration** → becomes **Workflow Composition**
* **Backend Execution** → becomes **Agent Execution**
* **Project Lifecycle** → becomes part of **Project & Run Record**
* **Workspace Management** → becomes **Workspace Governance**
* **Validation/Conformance** → split into:

  * **Validation Profiles** inside Workflow Composition
  * **Conformance Specification** as its own context
* **PRD Pipeline** → becomes **Requirements Drafting**
* **Git Integration** → not a bounded context; it is an adapter cluster
* **Prompt/Template Management** → not a bounded context; it is an adapter cluster
* **CLI / User Command Surface** → not a bounded context; it is a driving adapter
* **Configuration / Policy Resolution** → becomes part of **Workspace Governance**

## Workspace Governance

* **Name**
  Workspace Governance

* **Purpose**
  Own workspace initialization, workspace versioning, active project selection, effective config resolution, and flow/template discovery.

* **Why this boundary exists**
  These rules are shared across run, requirements, and daemon paths, but they are not delivery policy.

* **Responsibilities**

  * initialize `.ralph-burning/`
  * persist workspace config
  * resolve active project
  * resolve effective config from defaults/workspace/project/runtime layers
  * expose flow preset catalog metadata
  * resolve prompt/template sources
  * validate workspace version

* **What it owns**

  * workspace metadata
  * active project semantics
  * config precedence
  * workspace version contract
  * flow preset discovery surface

* **What it explicitly does not own**

  * run transitions
  * project history
  * backend invocation
  * requirements stage logic
  * GitHub task lifecycle

* **Core concepts**

  * Workspace
  * ActiveProjectRef
  * EffectiveConfig
  * FlowCatalogView
  * TemplateSource
  * WorkspaceVersion

* **Entities**

  * Workspace

* **Value objects**

  * WorkspaceRoot
  * ProjectSelector
  * ConfigScope
  * EffectiveWorkflowPolicy
  * EffectiveValidationPolicy
  * EffectiveBackendPolicy

* **Aggregates**

  * None worth forcing. This is largely application/configuration logic.

* **Domain/application services**

  * InitializeWorkspace
  * ValidateWorkspace
  * ResolveActiveProject
  * ResolveEffectiveConfig
  * ListFlows
  * ShowFlow

* **Invariants/business rules**

  * runtime overrides beat project config
  * project config beats workspace config
  * workspace config beats code defaults
  * unsupported workspace versions fail clearly
  * active project must exist or resolution fails
  * only built-in flow presets are selectable in v1

* **Commands**

  * InitializeWorkspace
  * SetActiveProject
  * UpdateConfig
  * ValidateWorkspace

* **Events**

  * WorkspaceInitialized
  * ActiveProjectChanged
  * ConfigUpdated

* **Queries/read models**

  * workspace summary
  * effective config
  * active project summary
  * flow list
  * flow details

* **Inbound dependencies**

  * CLI
  * Workflow Composition
  * Requirements Drafting
  * Automation Runtime
  * Project & Run Record

* **Outbound dependencies**

  * WorkspaceStorePort
  * ConfigStorePort
  * TemplateCatalogPort
  * LockPort

* **Data ownership**

  * `.ralph-burning/workspace.toml`
  * active-project marker
  * template source metadata if persisted

* **Trust/sensitivity concerns**

  * local config and template paths are untrusted input
  * path traversal and malformed config must fail safely

* **Likely code/package/module boundary**

  * `contexts/workspace_governance/*`

## Project & Run Record

* **Name**
  Project & Run Record

* **Purpose**
  Own canonical project metadata, run state, journal, durable history, runtime log classification, sessions, amendments, rollback points, and operator queries.

* **Why this boundary exists**
  The current repo spreads durable behavior across `src/project/*` and reconstructs too much from artifacts. The rewrite needs a clean source of truth.

* **Responsibilities**

  * create/list/show/delete/select projects
  * persist canonical project state
  * persist canonical run state
  * append journal entries
  * store stage payload records
  * store rendered history artifacts
  * classify/store runtime logs separately
  * manage session records
  * manage amendment queue
  * manage rollback points
  * serve status/history/tail/rollback queries

* **What it owns**

  * project identity
  * selected flow preset
  * prompt reference/hash
  * active run
  * stage cursor
  * work cycle history
  * completion round history
  * durable payload index
  * durable artifact index
  * runtime log metadata
  * session records
  * amendment queue
  * rollback points

* **What it explicitly does not own**

  * stage transition policy
  * backend CLI mechanics
  * prompt rendering
  * GitHub routing decisions
  * validation profile selection logic

* **Core concepts**

  * ProjectRecord
  * RunRecord
  * StageCursor
  * WorkCycle
  * CompletionRound
  * PayloadRecord
  * ArtifactRecord
  * RuntimeLogRecord
  * SessionRecord
  * AmendmentQueue
  * RollbackPoint

* **Entities**

  * ProjectRecord
  * RunRecord
  * WorkCycleRecord
  * CompletionRoundRecord
  * PayloadRecord
  * ArtifactRecord
  * RuntimeLogRecord
  * SessionRecord
  * AmendmentRequest

* **Value objects**

  * ProjectId
  * RunId
  * FlowId
  * StageId
  * CycleNumber
  * CompletionRoundNumber
  * PayloadId
  * ArtifactId
  * LogId
  * PromptHash
  * CheckpointRef

* **Aggregates**

  * `ProjectRecord` is the main aggregate
  * `RunRecord` can be a nested aggregate

* **Domain/application services**

  * CreateProject
  * DeleteProject
  * StartRun
  * LoadProject
  * SaveProject
  * RecordStageResult
  * AppendJournalEvent
  * EnqueueAmendment
  * ClaimAmendments
  * RecordRuntimeLog
  * QueryStatus
  * QueryHistory
  * QueryTail
  * RollbackProject

* **Invariants/business rules**

  * one active run per project
  * one immutable flow preset per project
  * stage cursor must reference a legal stage in that flow
  * work cycle numbering is monotonic
  * completion round numbering is monotonic and separate
  * history artifacts are durable project history
  * runtime logs are not durable history
  * snapshot writes are atomic
  * journal append precedes exposed read-model updates
  * rollback points are durable and queryable

* **Commands**

  * CreateProject
  * DeleteProject
  * StartRun
  * RecordStageResult
  * EnqueueAmendment
  * RecordRuntimeLog
  * RollbackProject
  * ResetSessions

* **Events**

  * ProjectCreated
  * RunStarted
  * StageCompleted
  * StageRejected
  * CycleCompleted
  * CompletionRoundRecorded
  * ProjectCompleted
  * ProjectRolledBack
  * AmendmentQueued
  * RuntimeLogRecorded
  * SessionUpserted

* **Queries/read models**

  * project summary
  * run summary
  * status view
  * history view
  * event tail
  * rollback candidate list
  * amendment queue view

* **Inbound dependencies**

  * Workspace Governance
  * Workflow Composition
  * Automation Runtime
  * CLI

* **Outbound dependencies**

  * ProjectStorePort
  * JournalStorePort
  * ArtifactStorePort
  * RuntimeLogStorePort
  * LockPort
  * VcsSnapshotPort

* **Data ownership**

  * `.ralph-burning/projects/<project-id>/project.toml`
  * `run.json`
  * `journal.ndjson`
  * `history/payloads/*`
  * `history/artifacts/*`
  * `runtime/logs/*`
  * `sessions.json`
  * amendment state
  * rollback state

* **Trust/sensitivity concerns**

  * payloads, history artifacts, and amendments may contain proprietary product or code context
  * corruption must be visible and fail-fast

* **Likely code/package/module boundary**

  * `contexts/project_run_record/*`

## Workflow Composition

* **Name**
  Workflow Composition

* **Purpose**
  Own the shared workflow engine, built-in flow presets, stage library, stage contracts, validation profiles, retry policy, completion policy, and final-review policy.

* **Why this boundary exists**
  The current system’s largest architectural flaw is the split between full workflow and quick-dev. This context replaces separate orchestrators with one composable engine.

* **Responsibilities**

  * define built-in flow presets
  * define reusable stage handlers
  * define stage contracts
  * manage stage progression
  * manage remediation loops
  * manage prompt review
  * manage completion and acceptance
  * manage final review when enabled by preset
  * select validation profiles
  * classify retryable failures
  * decide session reuse/reset rules

* **What it owns**

  * FlowDefinition
  * StageDefinition
  * StageContract
  * StageTransitionPolicy
  * ValidationProfile
  * CompletionPolicy
  * FinalReviewPolicy
  * PromptReviewPolicy
  * RetryPolicy

* **What it explicitly does not own**

  * filesystem layout
  * raw git commands
  * backend CLI flags
  * workspace config parsing
  * GitHub issue state

* **Core concepts**

  * FlowPreset
  * Stage
  * StageHandler
  * StageContract
  * StagePayload
  * StageOutcome
  * ValidationProfile
  * StopMode
  * CompletionRound
  * FinalReviewRound

* **Entities**

  * FlowDefinition
  * StageDefinition
  * StageExecutionPlan

* **Value objects**

  * FlowId
  * StageId
  * RoleId
  * TransitionRule
  * ValidationProfileId
  * StopMode
  * RetryDecision
  * FailureClass

* **Aggregates**

  * None necessary; this is policy-heavy.

* **Domain/application services**

  * StartRun
  * ResumeRun
  * AdvanceStage
  * ExecuteStage
  * DetermineNextStage
  * RunPromptReview
  * RunCompletionRound
  * RunFinalReview
  * SelectValidationProfile
  * ChangeFlowWithMigration is out of scope for v1 because flow is fixed per project

* **Invariants/business rules**

  * every run uses exactly one flow preset
  * flow preset is fixed for the project lifetime
  * every stage has a stage contract
  * schema validation precedes domain validation
  * markdown is rendered from payloads and never parsed as protocol
  * only declared transitions are legal
  * final review exists only if enabled in the preset
  * supported backends must provide structured stage payloads

* **Commands**

  * StartRun
  * ResumeRun
  * AdvanceStage
  * RetryStage
  * AbortRun

* **Events**

  * RunStarted
  * StageStarted
  * StageOutputValidated
  * StageCompleted
  * StageRetried
  * StageRejected
  * CompletionConsensusReached
  * FinalReviewRestarted
  * RunCompleted

* **Queries/read models**

  * next action preview
  * flow definition summary
  * stage contract summary
  * validation plan preview

* **Inbound dependencies**

  * CLI
  * Automation Runtime

* **Outbound dependencies**

  * Project & Run Record
  * Agent Execution
  * PromptCatalogPort
  * ValidationRunnerPort
  * VcsWorkspacePort
  * ClockPort
  * IdPort
  * EventSinkPort

* **Data ownership**

  * flow definitions in code
  * stage contracts and renderer definitions in code
  * validation profile registry in code

* **Trust/sensitivity concerns**

  * backend outputs are untrusted until validated
  * template content may influence prompts but must not bypass stage contracts

* **Likely code/package/module boundary**

  * `contexts/workflow_composition/*`

## Agent Execution

* **Name**
  Agent Execution

* **Purpose**
  Own backend selection, structured-output invocation, session continuity rules, timeout/cancel handling, and transport normalization.

* **Why this boundary exists**
  The current backend subsystem mixes backend semantics with process mechanics. The rewrite needs a stable invocation contract for all stages.

* **Responsibilities**

  * parse backend specs
  * resolve per-role backends/models
  * check availability
  * invoke backend with structured output requirements
  * normalize transport envelopes
  * manage timeouts and cancellation
  * manage session continuity support
  * preserve raw output for audit/debug

* **What it owns**

  * BackendSpec
  * ModelSpec
  * Availability state
  * InvocationRequest
  * InvocationResult
  * SessionContinuationContract

* **What it explicitly does not own**

  * workflow transitions
  * completion policy
  * project state mutation
  * PRD stage policy

* **Core concepts**

  * BackendFamily
  * BackendSpec
  * ModelSpec
  * InvocationRequest
  * InvocationEnvelope
  * StructuredOutputResult
  * SessionKey
  * TimeoutPolicy

* **Entities**

  * BackendCatalogEntry
  * InvocationHandle

* **Value objects**

  * BackendSpec
  * ModelSpec
  * SessionKey
  * TimeoutPolicy
  * AvailabilityStatus

* **Aggregates**

  * None needed.

* **Domain/application services**

  * ResolveBackendForRole
  * ResolvePanelBackends
  * CheckAvailability
  * InvokeStage
  * CancelInvocation
  * NormalizeOutput

* **Invariants/business rules**

  * all supported backends must return structured stage payloads
  * explicit model beats default model
  * timeout/cancel cleanup is mandatory
  * raw output is preserved when invocation reaches adapter execution
  * session reuse is permitted only when backend/role policy allows it

* **Commands**

  * ResolveBackend
  * CheckAvailability
  * Invoke
  * Cancel

* **Events**

  * BackendInvocationStarted
  * BackendInvocationCompleted
  * BackendInvocationTimedOut
  * BackendInvocationFailed
  * BackendInvocationCanceled

* **Queries/read models**

  * resolved backend assignments
  * backend health/availability
  * supported backend catalog

* **Inbound dependencies**

  * Workflow Composition
  * Requirements Drafting
  * Automation Runtime

* **Outbound dependencies**

  * BackendTransportPort
  * ProcessRunnerPort
  * TmuxPort
  * LogSinkPort
  * ClockPort

* **Data ownership**

  * ephemeral invocation metadata
  * optional adapter logs
  * raw output capture before handoff

* **Trust/sensitivity concerns**

  * all backend CLIs are untrusted external programs
  * adapter isolation and cleanup correctness matter

* **Likely code/package/module boundary**

  * `contexts/agent_execution/*`

## Requirements Drafting

* **Name**
  Requirements Drafting

* **Purpose**
  Own staged requirements generation and quick requirements generation as separate AI workflows.

* **Why this boundary exists**
  The current PRD subsystem is already distinct from delivery orchestration and should remain so.

* **Responsibilities**

  * run staged requirements flows
  * run quick requirements flows
  * manage question/answer rounds
  * validate generated requirements
  * cache and resume requirements runs
  * hand off approved requirements to project creation

* **What it owns**

  * requirements run state
  * quick requirements run state
  * question backlog
  * revision history
  * validation reports
  * cache eligibility rules

* **What it explicitly does not own**

  * project work cycles
  * delivery stage transitions
  * daemon task routing
  * VCS rollback

* **Core concepts**

  * RequirementsRun
  * QuickRequirementsRun
  * Stage
  * QuestionRound
  * Revision
  * ValidationReport

* **Entities**

  * RequirementsRun
  * QuickRequirementsRun

* **Value objects**

  * RequirementsRunId
  * StageId
  * Question
  * AnswerSet
  * ReviewIssue
  * ValidationIssue
  * InputHash

* **Aggregates**

  * `RequirementsRun` is useful

* **Domain/application services**

  * StartRequirementsDraft
  * ResumeRequirementsDraft
  * StartQuickDraft
  * ApplyAnswers
  * ValidateRequirements
  * ProduceProjectSeed

* **Invariants/business rules**

  * stage sequence is fixed by mode
  * cache reuse requires matching dependency/input hashes
  * approval terminates quick draft review loop
  * validation must occur before handoff into project creation

* **Commands**

  * RunRequirementsDraft
  * RunQuickRequirements
  * ApplyAnswers
  * ShowRequirements

* **Events**

  * RequirementsStageCompleted
  * QuestionsGenerated
  * AnswersApplied
  * QuickDraftApproved
  * ValidationFailed

* **Queries/read models**

  * requirements run summary
  * outstanding question list
  * validation report
  * revision history

* **Inbound dependencies**

  * CLI
  * Automation Runtime
  * Workspace Governance

* **Outbound dependencies**

  * Agent Execution
  * PromptCatalogPort
  * RequirementsStorePort
  * InteractionPort
  * ClockPort

* **Data ownership**

  * `.ralph-burning/requirements/*`

* **Trust/sensitivity concerns**

  * requirements may contain sensitive strategy or product context
  * generated output is untrusted until validated

* **Likely code/package/module boundary**

  * `contexts/requirements_drafting/*`

## Automation Runtime

* **Name**
  Automation Runtime

* **Purpose**
  Own long-running issue/PR/task orchestration, worktree leasing, daemon lifecycle, and routing by labels and explicit commands.

* **Why this boundary exists**
  The current daemon is its own subsystem with real runtime concerns and needs a clean boundary.

* **Responsibilities**

  * poll and claim work
  * parse routing labels and commands
  * resolve dispatch target
  * allocate worktree leases
  * launch tasks
  * manage task status
  * manage draft PR watcher
  * manage rebase behavior
  * manage interactive requirements issue behavior

* **What it owns**

  * task lifecycle
  * worktree lease lifecycle
  * routing precedence
  * daemon status
  * issue/PR automation policy
  * interactive requirements issue progression

* **What it explicitly does not own**

  * stage transition rules
  * project aggregate invariants
  * requirements stage internals
  * backend structured-output semantics

* **Core concepts**

  * DaemonTask
  * WorktreeLease
  * RoutingRule
  * RoutingCommand
  * RoutingLabel
  * DraftPrWatcher
  * RebaseAttempt
  * InteractiveRequirementsState

* **Entities**

  * TaskRecord
  * WorktreeRecord
  * InteractiveRequirementsRecord

* **Value objects**

  * TaskId
  * LeaseId
  * RepoSlug
  * IssueNumber
  * LabelSet
  * CommandSet
  * RoutingDecision

* **Aggregates**

  * `TaskRecord`
  * `InteractiveRequirementsRecord`

* **Domain/application services**

  * PollAndClaim
  * ParseRouting
  * ResolveDispatch
  * DispatchTask
  * AbortTask
  * RetryTask
  * ReconcileDaemonState
  * RunDraftPrWatcher
  * RunRebaseFlow
  * AdvanceInteractiveRequirements

* **Invariants/business rules**

  * one active task per issue
  * one worktree lease per task
  * explicit routing command beats routing label
  * routing label beats repo default
  * task terminal state must persist before cleanup
  * cancellation must stop child work and release watchers

* **Commands**

  * StartDaemon
  * AbortTask
  * RetryTask
  * Reconcile
  * AdvanceInteractiveRequirements

* **Events**

  * TaskClaimed
  * TaskStarted
  * TaskCompleted
  * TaskFailed
  * TaskAborted
  * WorktreeAllocated
  * DraftPrUpdated
  * RebaseStarted
  * InteractiveRequirementsAdvanced

* **Queries/read models**

  * daemon status
  * task inventory
  * worktree inventory
  * routing decision preview

* **Inbound dependencies**

  * CLI

* **Outbound dependencies**

  * GitHubPort
  * WorktreePort
  * WorkflowPort
  * RequirementsPort
  * Project & Run Record
  * Workspace Governance
  * Agent Execution
  * TaskProcessPort
  * SchedulerPort

* **Data ownership**

  * `.ralph-burning/daemon/*`

* **Trust/sensitivity concerns**

  * GitHub comments, labels, and PR state are untrusted external inputs
  * task runtime must isolate processes and worktree paths carefully

* **Likely code/package/module boundary**

  * `contexts/automation_runtime/*`

## Conformance Specification

* **Name**
  Conformance Specification

* **Purpose**
  Own executable behavior definition and acceptance testing for `ralph-burning`.

* **Why this boundary exists**
  The old validate suite is too important to leave informal. The rewrite needs a first-class conformance subsystem.

* **Responsibilities**

  * map Gherkin scenarios to cases
  * provision temp workspaces/repos
  * provide mock backends
  * provide mock GitHub behavior
  * run the real binary
  * assert observable behavior

* **What it owns**

  * scenario registry
  * fixture builders
  * mock adapters
  * assertion helpers

* **What it explicitly does not own**

  * product runtime behavior

* **Core concepts**

  * Feature
  * Scenario
  * ConformanceCase
  * Fixture
  * MockBackend
  * HarnessRun

* **Entities**

  * ConformanceCase
  * HarnessRun

* **Value objects**

  * ScenarioId
  * FixtureSpec
  * AssertionResult

* **Aggregates**

  * None needed

* **Domain/application services**

  * ListCases
  * RunCases
  * BuildFixture
  * ExecuteCase

* **Invariants/business rules**

  * conformance asserts public behavior only
  * new public behavior requires scenario coverage
  * tests must be deterministic
  * no legacy storage compatibility is implied by conformance

* **Commands**

  * ConformanceList
  * ConformanceRun

* **Events**

  * CasePassed
  * CaseFailed

* **Queries/read models**

  * case inventory
  * suite summary
  * failure report

* **Inbound dependencies**

  * CLI

* **Outbound dependencies**

  * BinaryRunnerPort
  * FixturePort
  * MockBackendPort
  * AssertionPort

* **Data ownership**

  * test fixtures and temp workspaces only

* **Trust/sensitivity concerns**

  * mock environments must be deterministic and isolated

* **Likely code/package/module boundary**

  * `contexts/conformance_spec/*`
  * `tests/conformance/*`

# Context Interaction Model

## Context map

* **Workspace Governance** is upstream of all runtime contexts.
* **Project & Run Record** is the durable source of truth for project and run state.
* **Workflow Composition** drives run behavior using Project & Run Record and Agent Execution.
* **Requirements Drafting** is separate from delivery workflow but shares Workspace Governance and Agent Execution.
* **Automation Runtime** routes into Workflow Composition or Requirements Drafting using labels and explicit commands.
* **Conformance Specification** stays outside all runtime contexts and drives the binary through public surfaces.

## Upstream/downstream relationships

* Workspace Governance -> Project & Run Record
  for project selection and effective config

* Workspace Governance -> Workflow Composition
  for preset visibility and effective policy

* Project & Run Record -> Workflow Composition
  as the mutable source of run state

* Agent Execution -> Workflow Composition
  as a driven service: workflow decides why/when; execution decides how

* Requirements Drafting -> Project & Run Record
  only through explicit handoff into project creation, not through shared mutable state

* Workflow Composition and Requirements Drafting -> Automation Runtime
  as services invoked by daemon task routing

## Synchronous vs asynchronous interactions

* Context interactions inside the monolith should remain synchronous and in-process.
* The daemon introduces asynchronous runtime behavior, but not a distributed design.
* No internal message bus is necessary for correctness in v1.
* Domain events are useful as typed internal signals for logging and tests, but not as distributed integration contracts.

## Anti-corruption layers

Even without legacy storage compatibility, the rewrite needs ACLs around external systems:

### Backend transport ACL

Converts backend-specific CLI behaviors, structured-output formats, session tokens, and streaming events into a normalized invocation result.

### Git/worktree ACL

Converts raw git commands and worktree management into typed checkpoint, reset, diff, and lease operations.

### GitHub ACL

Converts labels, commands, issue state, review state, and PR signals into typed daemon routing and task lifecycle inputs.

### Template/prompt ACL

Converts filesystem/embedded templates into typed stage prompt sources without leaking file paths into workflow policy.

## Cross-context contracts

Stabilize these early:

* `EffectiveConfig`
* `FlowDefinition`
* `StageDefinition`
* `StageContract`
* `RunSnapshot`
* `StageCursor`
* `StagePayloadEnvelope`
* `BackendSpec`
* `InvocationResult`
* `ArtifactRecord`
* `RuntimeLogRecord`
* `RollbackPoint`
* `RequirementsRunState`
* `TaskRecord`

## Eventual consistency concerns

Most state should be strongly consistent because it is local and in-process. Eventual consistency exists only at boundaries:

* GitHub labels/comments/PR states
* remote git sync and push state
* backend session continuity stored outside this process

Rule: **persist local durable state first, then perform remote side effects, and make remote side effects idempotent.**

## Idempotency concerns

* `run start` on an active project should resume or fail clearly, not fork a second active run
* daemon dispatch must be idempotent per issue/task
* draft PR update must be idempotent
* routing commands should be safe to repeat
* rendered artifacts should be deterministic from stored payloads
* requirements question posting and answer application must be safe to retry

## Retry semantics

* transport failure retry belongs to Agent Execution + Workflow Composition policy
* schema validation retry belongs to Workflow Composition
* domain-validation failure handling belongs to Workflow Composition
* GitHub transient retry belongs to Automation Runtime
* requirements question loop retry belongs to Requirements Drafting

These should remain distinct failure classes.

## Failure/compensation handling

* if payload storage succeeds but artifact rendering fails, the stage is not complete
* if local durable state succeeds but a remote side effect fails, local state remains authoritative
* cancellation must persist terminal or paused state before cleanup
* logical rollback must succeed even if hard VCS reset fails afterward
* amendment queue claims must not be lost across crashes

## Shared kernel risks

The shared kernel should stay very small:

* ids
* small enums
* DTOs
* schema/rendering primitives
* common error/result wrappers if necessary

Do not create a wide shared library that silently becomes the real architecture.

## Current boundary leaks in the old repo

The rewrite specifically addresses these leaks:

* `src/workflow/orchestrator.rs` mixes policy with backend calls, git, persistence, parsing, templates, and sessions
* `src/workflow/quick_dev_orchestrator.rs` duplicates orchestration instead of reusing a shared engine
* `src/project/lifecycle.rs` mixes project creation, reconstruction, git scanning, and state assembly
* `src/backend/mod.rs` mixes backend semantics with transport/process mechanics
* `src/daemon/runtime.rs` mixes routing policy with worktree, GitHub, and child-process control

# Target Architecture

## Why modular monolith

A modular monolith is the right target because:

* `ralph-burning` is one binary
* it coordinates local filesystem and git state
* it uses local worktrees
* it runs local external backend tools
* it does not need independent service scaling

A service split would add distributed-state complexity without solving the main architecture problem.

## Top-level package/module layout

```text
src/
  main.rs

  composition/
    wiring.rs
    command_router.rs

  shared/
    ids.rs
    time.rs
    result.rs
    contracts/
      stage_contracts/
      schemas/
      artifact_rendering/
      vcs.rs

  contexts/
    workspace_governance/
      application/
      model.rs
      ports.rs

    project_run_record/
      application/
      model/
      queries/
      ports.rs

    workflow_composition/
      application/
      domain/
        flow_engine/
        flow_catalog/
        stages/
        policies/
        validation_profiles/
      ports.rs

    agent_execution/
      application/
      domain/
      ports.rs

    requirements_drafting/
      application/
      domain/
      ports.rs

    automation_runtime/
      application/
      domain/
      ports.rs

    conformance_spec/
      application/
      model.rs

  adapters/
    cli/
    fs_workspace/
    fs_project_store/
    fs_requirements_store/
    fs_daemon_store/
    prompts/
    git/
    github/
    process/
    tmux/
    backend_claude/
    backend_codex/
    backend_openrouter/
    locks/
    logging/
```

## What belongs in domain / application / adapters / shared

### Domain

Only real policy and invariants:

* flow definitions and transitions
* stage contracts
* validation profile behavior
* completion and final review rules
* project/run aggregate invariants
* requirements stage progression
* automation routing policy

### Application

Use-case orchestration:

* create/select project
* start/resume run
* execute next stage
* rollback
* run requirements draft
* daemon poll and dispatch

### Adapters

All side effects:

* CLI
* filesystem
* git/worktrees
* GitHub
* backend CLIs
* tmux
* logging
* template loading
* locks
* process control

### Shared

Small, stable primitives only:

* ids
* small enums
* DTOs
* schema/rendering helpers
* general result/time utilities

## How to keep boundaries explicit

* core contexts expose application services and DTOs only
* adapters depend inward, not sideways
* no context directly manipulates raw filesystem layout except its store adapters
* workflow logic does not parse markdown artifacts
* git is not a context
* CLI is not a context

## New workspace and storage model

The rewrite should use a new workspace root:

```text
.ralph-burning/
  workspace.toml
  active-project

  projects/
    <project-id>/
      project.toml
      prompt.md
      run.json
      journal.ndjson
      sessions.json

      history/
        payloads/
        artifacts/

      runtime/
        logs/
        backend/
        temp/

      amendments/
      rollback/

  requirements/
    <requirements-run-id>/

  daemon/
    tasks/
    leases/
```

### Meaning of each area

**`history/payloads/`**
Canonical, structured, validated stage outputs that are part of the durable project record.

**`history/artifacts/`**
Readable markdown rendered from payloads. These are durable and part of project history.

**`runtime/logs/`**
Ephemeral operational logs for immediate debugging. These are important, but they are not durable workflow history.

## State model

### Canonical durable state

Each project should persist:

* metadata
* prompt reference/hash
* fixed flow preset
* active run state
* stage cursor
* work cycle history
* completion round history
* session records
* amendment queue
* rollback points
* journal

### Structured outputs and rendered artifacts

Every successful stage produces:

* canonical payload JSON
* rendered markdown artifact
* optional raw invocation envelope
* optional runtime logs

The canonical state machine consumes the payload JSON, never the markdown.

## CLI surface

Recommended modernized CLI:

### Workspace and config

* `ralph-burning init`
* `ralph-burning config show`
* `ralph-burning config get <key>`
* `ralph-burning config set <key> <value>`
* `ralph-burning config edit`

### Flow discovery

* `ralph-burning flow list`
* `ralph-burning flow show <flow-id>`

### Project lifecycle

* `ralph-burning project create --id <id> --name <name> --prompt <file> --flow <flow-id>`
* `ralph-burning project select <id>`
* `ralph-burning project list`
* `ralph-burning project show [<id>]`
* `ralph-burning project delete <id>`

### Run lifecycle

* `ralph-burning run start`
* `ralph-burning run resume`
* `ralph-burning run status`
* `ralph-burning run history`
* `ralph-burning run tail`
* `ralph-burning run rollback --to <target> [--hard]`

### Requirements

* `ralph-burning requirements draft --idea "..."`
* `ralph-burning requirements quick --idea "..."`
* `ralph-burning requirements show <run-id>`
* `ralph-burning requirements answer <run-id>`

### Daemon

* `ralph-burning daemon start`
* `ralph-burning daemon status`
* `ralph-burning daemon abort <task-id>`
* `ralph-burning daemon retry <task-id>`
* `ralph-burning daemon reconcile`

### Conformance

* `ralph-burning conformance list`
* `ralph-burning conformance run [--filter <scenario-id>]`

## Flow preset catalog for v1

The v1 built-in flow presets should be exactly:

* `standard`
* `quick_dev`
* `docs_change`
* `ci_improvement`

These are code-defined and versioned. User-defined arbitrary workflow DSLs are out of scope for v1.

## Validation profile defaults

### `standard`

Default required behavior:

* repo-defined fast validation target
* feature QA
* feature review
* optional pre-commit checks if enabled by config/policy

Optional stricter add-ons:

* heavy integration test suite
* acceptance QA
* final review

### `quick_dev`

Default required behavior:

* targeted validation on changed area
* review
* remediation cycle on failure

Default exclusions:

* heavy acceptance QA
* heavyweight final review panel

### `docs_change`

Default required behavior:

* markdown lint
* link check
* docs/site build if repo exposes one
* frontmatter/schema validation where applicable

Warning-only by default:

* spelling
* prose/style lint

### `ci_improvement`

Default required behavior:

* YAML validation
* `actionlint` where applicable
* shell lint for changed CI shell scripts
* repo-specific CI config validation command if available

Opt-in stricter add-ons:

* workflow smoke run
* container build smoke test
* matrix dry-run

## Final review defaults by preset

Recommended defaults:

* `standard` → enabled
* `quick_dev` → enabled but lightweight
* `docs_change` → disabled by default
* `ci_improvement` → disabled by default, but easy to enable by policy

## Observability, logging, and events

* keep structured tracing for diagnostics
* persist journal events for durable history and operator queries
* keep runtime logs separate from history artifacts
* make `run tail` primarily about recent durable history, with an explicit option to include runtime logs if desired
* do not make business logic depend on human log text

## Concurrency and parallel work support

Runtime:

* one writer lock per project
* many projects can run concurrently
* one worktree lease per daemon task
* child processes must be cancellable and cleaned up

Implementation:

* one coding agent per context or slice
* contract-first coordination
* context packs checked into docs
* minimal shared kernel

# Hexagonal Architecture Design

## Workspace Governance

* **Core policy/application logic that should stay inside**

  * workspace version validation
  * config precedence
  * active project resolution
  * flow discovery

* **Ports**

  * `WorkspaceStorePort`
  * `ConfigStorePort`
  * `TemplateCatalogPort`
  * `LockPort`

* **Adapters**

  * filesystem workspace adapter
  * TOML config adapter
  * prompt/template loader
  * file lock adapter

* **Primary/driving vs secondary/driven**

  * Primary: CLI, daemon startup, workflow entry, requirements entry
  * Secondary: filesystem/config/template adapters

* **What should remain outside the core**

  * TOML syntax details
  * directory layout details
  * CLI output formatting

* **How tests should target the core through ports**

  * service tests with fake stores
  * adapter tests for file and config roundtrip behavior

## Project & Run Record

* **Core policy/application logic that should stay inside**

  * project/run invariants
  * stage cursor mutation
  * journal ordering
  * durable history vs runtime log classification
  * rollback semantics

* **Ports**

  * `ProjectStorePort`
  * `JournalStorePort`
  * `ArtifactStorePort`
  * `RuntimeLogStorePort`
  * `LockPort`
  * `VcsSnapshotPort`

* **Adapters**

  * filesystem project store
  * NDJSON journal store
  * payload/artifact filesystem adapter
  * runtime log store
  * git snapshot adapter
  * file lock adapter

* **Primary/driving vs secondary/driven**

  * Primary: CLI project/run queries, Workflow Composition, Automation Runtime
  * Secondary: filesystem/git/lock adapters

* **What should remain outside the core**

  * actual filenames
  * serialization details
  * git command syntax

* **How tests should target the core through ports**

  * aggregate mutation tests
  * service tests with fake stores
  * temp-dir and temp-repo adapter tests

## Workflow Composition

* **Core policy/application logic that should stay inside**

  * flow engine
  * preset definitions
  * stage transitions
  * stage contract enforcement
  * validation profile selection
  * retry classification
  * completion/final-review policy

* **Ports**

  * `ProjectRunPort`
  * `AgentExecutionPort`
  * `PromptCatalogPort`
  * `ValidationRunnerPort`
  * `VcsWorkspacePort`
  * `ClockPort`
  * `IdPort`
  * `EventSinkPort`

* **Adapters**

  * project/run store adapter
  * backend execution adapter
  * prompt/template adapter
  * validation command adapter
  * git workspace adapter

* **Primary/driving vs secondary/driven**

  * Primary: CLI run commands, daemon task dispatcher
  * Secondary: project/backend/template/validation/git adapters

* **What should remain outside the core**

  * shell commands
  * prompt file locations
  * artifact file writing mechanics
  * tmux mechanics

* **How tests should target the core through ports**

  * pure transition tests
  * stage contract tests
  * use-case tests with fake backends and fake project store

## Agent Execution

* **Core policy/application logic that should stay inside**

  * backend spec parsing
  * model resolution
  * availability checks
  * session-continuity rules
  * structured invocation normalization

* **Ports**

  * `BackendTransportPort`
  * `ProcessRunnerPort`
  * `TmuxPort`
  * `ClockPort`
  * `LogSinkPort`

* **Adapters**

  * Claude adapter
  * Codex adapter
  * OpenRouter adapter
  * subprocess adapter
  * tmux adapter

* **Primary/driving vs secondary/driven**

  * Primary: Workflow Composition, Requirements Drafting, Automation Runtime
  * Secondary: backend/process/tmux adapters

* **What should remain outside the core**

  * CLI flags
  * environment variables
  * backend-specific stdout parsing quirks

* **How tests should target the core through ports**

  * backend policy tests
  * contract tests per backend adapter
  * subprocess mock tests

## Requirements Drafting

* **Core policy/application logic that should stay inside**

  * requirements stage machine
  * cache reuse rules
  * answer application
  * quick draft review loop
  * validation rules

* **Ports**

  * `AgentExecutionPort`
  * `PromptCatalogPort`
  * `RequirementsStorePort`
  * `InteractionPort`
  * `ClockPort`

* **Adapters**

  * filesystem requirements store
  * prompt/template adapter
  * stdin/stdout interaction adapter
  * GitHub comment interaction adapter for daemon path

* **Primary/driving vs secondary/driven**

  * Primary: CLI requirements commands, daemon interactive requirements
  * Secondary: backend/template/store/interaction adapters

* **What should remain outside the core**

  * cache layout
  * CLI prompt formatting
  * GitHub comment syntax

* **How tests should target the core through ports**

  * state machine tests
  * fake execution tests
  * cache adapter tests

## Automation Runtime

* **Core policy/application logic that should stay inside**

  * label/command routing
  * task lifecycle
  * worktree lease lifecycle
  * dispatch rules
  * watcher/rebase policy
  * interactive requirements progression

* **Ports**

  * `GitHubPort`
  * `WorktreePort`
  * `WorkflowPort`
  * `RequirementsPort`
  * `TaskProcessPort`
  * `SchedulerPort`
  * `ClockPort`

* **Adapters**

  * GitHub CLI/API adapter
  * git worktree adapter
  * process launcher/watcher
  * filesystem daemon state store

* **Primary/driving vs secondary/driven**

  * Primary: CLI daemon commands
  * Secondary: GitHub/worktree/process/store adapters

* **What should remain outside the core**

  * `gh` syntax
  * OS process polling details
  * filesystem scanning details

* **How tests should target the core through ports**

  * task state-machine tests with fake ports
  * integration tests with temp repos and mock GitHub scripts

## Where hexagonal architecture would be overkill

Do not force ports/adapters for:

* clap parsing itself
* tiny slug/id utilities
* pure markdown renderers
* simple status/history formatters
* immutable DTO definitions
* schema constants

# Domain Invariants and Behavioral Rules

1. Every project has exactly one immutable flow preset.
2. Every active run has exactly one stage cursor.
3. A stage cursor must point to a stage declared in the project’s flow preset.
4. Work cycle numbering is monotonic.
5. Completion round numbering is monotonic and separate from work cycle numbering.
6. Flow preset selection occurs at project creation and cannot be changed for that project in v1.
7. Every stage has a declared stage contract.
8. Every successful stage result must pass schema validation before any state mutation.
9. Every successful stage result must pass domain validation before any transition.
10. Markdown artifacts are rendered from validated payloads and never parsed back into canonical state.
11. Runtime logs are not canonical state.
12. Runtime logs are not durable history artifacts.
13. Durable project history consists of journal entries, payloads, and rendered artifacts.
14. Starting a run on a project with an active run must resume or fail clearly; it must not silently create a second active run.
15. Resume occurs only from durable stage boundaries.
16. Previously durable stage results must not be duplicated on resume.
17. Validation profile resolution must be deterministic for a given preset and stage unless explicitly overridden.
18. `standard`, `quick_dev`, `docs_change`, and `ci_improvement` are the only built-in v1 presets.
19. User-defined arbitrary workflow scripts are out of scope for v1.
20. Final review exists only for presets that enable it.
21. `docs_change` and `ci_improvement` do not require final review by default.
22. `standard` requires final review by default.
23. `quick_dev` uses a lighter final review by default.
24. QA failure returns control to the configured remediation stage for the same work cycle.
25. Review-requested changes return control to the configured remediation stage for the same work cycle.
26. Review approval is invalidated if required validation or pre-commit checks fail afterward.
27. Review and QA caps are enforced by policy and produce a defined failure or rollback path.
28. Completion consensus requires configured minimum participants and threshold rules.
29. Acceptance QA runs only after completion consensus indicates completion.
30. Acceptance QA failure reopens work according to flow policy.
31. Accepted final-review amendments reopen work unless the preset/policy reaches an explicit cap outcome.
32. Completion/final-review cap outcomes must be explicit and durable.
33. Transport failure, schema validation failure, and domain-validation failure are different failure classes.
34. Retry policy must classify failures by type and apply only legal retries for that class.
35. Cancellation stops further retries immediately.
36. All supported backends must return structured stage payloads for declared contracts.
37. Explicit model selection overrides default role-model mapping.
38. Session reuse is allowed only for roles/backends that explicitly support it.
39. Session reset conditions must be explicitly defined and enforced.
40. Canonical state writes must be atomic.
41. Journal append must happen before status/history/tail expose a transition as completed.
42. One writer lock governs one project at a time.
43. One daemon task may own one worktree lease at a time.
44. One issue may have only one active daemon task at a time.
45. Explicit routing command has higher precedence than routing label.
46. Routing label has higher precedence than repo default routing.
47. Task terminal state must be persisted before cleanup.
48. Logical rollback must update canonical state before hard VCS reset.
49. Hard VCS rollback failure must not silently undo logical rollback.
50. Requirements cache reuse is allowed only when dependency hashes match.
51. Requirements output must pass its own validation rules before project handoff.
52. `flow list` and `flow show` are part of the public operator contract.
53. Unsupported workspace versions fail clearly and do not trigger implicit migration.
54. New flow presets require conformance coverage before release.
55. Stage contract changes require contract tests and conformance updates.
56. Validation profile changes require affected scenario updates.

# Gherkin Features and Scenarios

```gherkin
Feature: Workspace initialization

  Scenario: Initialize a new Ralph Burning workspace
    Given I am in an empty directory that is not already a Ralph Burning workspace
    When I run "ralph-burning init"
    Then a ".ralph-burning" directory is created
    And the workspace version is recorded
    And workspace configuration is initialized
    And the command exits successfully

  Scenario: Reject an unsupported workspace version
    Given the current directory contains a ".ralph-burning" workspace with an unsupported version
    When I run "ralph-burning run status"
    Then the command fails clearly
    And it does not attempt implicit migration
```

```gherkin
Feature: Flow discovery

  Scenario: List available flow presets
    Given a Ralph Burning workspace exists
    When I run "ralph-burning flow list"
    Then the output includes "standard"
    And the output includes "quick_dev"
    And the output includes "docs_change"
    And the output includes "ci_improvement"

  Scenario: Show one flow preset
    Given a Ralph Burning workspace exists
    When I run "ralph-burning flow show docs_change"
    Then the output describes the stage sequence for "docs_change"
    And the output describes its default validation profile
    And the output states whether final review is enabled
```

```gherkin
Feature: Project creation and immutable flow selection

  Scenario: Create a project with a fixed flow preset
    Given a workspace exists
    And a prompt file "PROMPT.md" exists
    When I run "ralph-burning project create --id demo --name Demo --prompt ./PROMPT.md --flow standard"
    Then project "demo" is created
    And its flow preset is "standard"

  Scenario: Reject flow change after project creation
    Given project "demo" was created with flow "standard"
    When I attempt to change its flow preset to "docs_change"
    Then the command is rejected
    And the project keeps flow "standard"
```

```gherkin
Feature: Standard workflow happy path

  Scenario: One standard work cycle succeeds end to end
    Given project "demo" uses flow "standard"
    And the planner returns a valid structured feature specification
    And the implementer returns valid structured implementation notes
    And feature QA returns "pass"
    And feature review returns "approved"
    And required validation checks pass
    When I run "ralph-burning run start"
    Then cycle 1 completes successfully
    And the run advances according to the standard preset
    And durable payloads exist for plan, implementation, QA, and review
    And rendered history artifacts exist for plan, implementation, QA, and review
```

```gherkin
Feature: Structured outputs are canonical

  Scenario: Workflow state consumes payloads and not markdown
    Given project "demo" is ready for review
    And the reviewer backend returns a schema-valid structured result
    When the review stage executes
    Then the review payload is stored as canonical stage output
    And a markdown artifact is rendered from that payload
    And workflow state transition uses the payload result rather than parsing markdown

  Scenario: Schema-invalid output blocks the stage
    Given project "demo" is ready for QA
    And the QA backend returns output that violates the QA schema
    When the QA stage executes
    Then the stage is rejected as a schema-validation failure
    And no successful stage transition is recorded
    And retry handling follows the configured failure policy
```

```gherkin
Feature: QA and review remediation loops

  Scenario: QA fails once and passes after remediation
    Given project "demo" is in cycle 2 under flow "standard"
    And QA first returns "fail" with remediation items
    And the implementer then returns a valid remediation payload
    And QA next returns "pass"
    And review returns "approved"
    When I run "ralph-burning run resume"
    Then cycle 2 remains the active cycle throughout remediation
    And the remediation history is recorded durably
    And the cycle advances only after QA passes

  Scenario: Review keeps requesting changes until the cap is reached
    Given project "demo" has a review iteration cap of 2
    And review returns "changes_requested" on every review attempt
    When I run "ralph-burning run start"
    Then the run ends with a review-cap outcome
    And canonical run state reflects the rollback or failure policy
```

```gherkin
Feature: Completion and final acceptance

  Scenario: Completion consensus says complete but acceptance QA fails
    Given project "demo" is eligible for completion review
    And the completion panel satisfies the configured threshold
    And acceptance QA returns "fail"
    When I run "ralph-burning run resume"
    Then a completion round is recorded
    And the project is not marked complete
    And the run reopens work according to standard flow policy

  Scenario: Final review restarts work after accepted amendments
    Given project "demo" passed completion consensus and acceptance QA
    And final review is enabled for the preset
    And final review accepts amendments
    When I run "ralph-burning run resume"
    Then the amendments are recorded in the amendment queue
    And the project is not marked complete
    And a new work cycle becomes required
```

```gherkin
Feature: Preset-specific behavior

  Scenario: Quick dev uses the shared workflow engine
    Given project "fast-fix" uses flow "quick_dev"
    When I run "ralph-burning run start"
    Then the shared flow engine executes the quick_dev stage sequence
    And quick_dev state is stored in the canonical run model

  Scenario: Documentation flow uses docs validation
    Given project "docs-fix" uses flow "docs_change"
    When I run "ralph-burning run start"
    Then markdown lint and link checks are executed
    And full code QA is not required unless explicitly enabled

  Scenario: CI improvement flow uses CI-specific validation
    Given project "gha-fix" uses flow "ci_improvement"
    When I run "ralph-burning run start"
    Then CI validation is executed
    And failures return the current cycle to remediation

  Scenario: CI improvement skips final review by default
    Given project "gha-fix" uses flow "ci_improvement"
    And project policy does not override final review
    When the run reaches a successful terminal state
    Then final review is not required
```

```gherkin
Feature: Backend resolution

  Scenario: Per-role override beats default mapping
    Given project "demo" uses flow "standard"
    And the effective config sets reviewer backend to "codex(gpt-5.3-codex-xhigh)"
    When I run a review stage
    Then the reviewer stage uses "codex(gpt-5.3-codex-xhigh)"

  Scenario: Required backend must support structured stage payloads
    Given a backend is configured for a stage role
    And that backend cannot satisfy the stage contract
    When I attempt to start the run
    Then the command fails during backend validation
    And the backend is treated as unsupported
```

```gherkin
Feature: Config precedence

  Scenario: Runtime override beats project and workspace config
    Given workspace config defines a reviewer backend
    And project config defines a different reviewer backend
    And I run with an explicit reviewer backend override
    When I execute "ralph-burning run start"
    Then the explicit reviewer backend is used for that run
```

```gherkin
Feature: Durable history versus runtime logs

  Scenario: Runtime logs are separate from durable project history
    Given a stage execution produced runtime debug logs
    And the stage also produced a successful stage payload
    When I inspect run history
    Then the durable payload and rendered artifact are shown as project history
    And runtime logs are not treated as durable history artifacts
```

```gherkin
Feature: Resume and rollback

  Scenario: Resume continues from a durable stage boundary
    Given project "demo" was interrupted after recording review feedback for cycle 3
    When I run "ralph-burning run resume"
    Then the run resumes from the remediation stage for cycle 3
    And previously completed stage results are not duplicated

  Scenario: Logical rollback rewinds the run
    Given project "demo" has completed cycles 1 and 2
    When I run "ralph-burning run rollback --to cycle-1"
    Then cycle 2 is removed from active run state
    And history records the rollback event

  Scenario: Hard rollback resets repository state after logical rollback
    Given project "demo" has a VCS rollback point for cycle 1
    When I run "ralph-burning run rollback --to cycle-1 --hard"
    Then canonical run state is rolled back first
    And the repository is reset to the selected checkpoint
```

```gherkin
Feature: Requirements drafting

  Scenario: Full requirements draft resumes from cache
    Given no cached requirements draft exists for idea "Build feature X"
    When I run "ralph-burning requirements draft --idea 'Build feature X'"
    Then requirements stages execute in order
    And stage outputs are stored durably
    When I rerun the same command with unchanged inputs
    Then reusable cached stages are not recomputed

  Scenario: Quick requirements draft produces project seed material
    Given I run "ralph-burning requirements quick --idea 'Fix docs pipeline'"
    And the quick draft is approved
    Then the result can be used to create a project with flow "docs_change"
```

```gherkin
Feature: Daemon routing

  Scenario: Explicit routing command beats routing label
    Given an issue has label "rb:flow:docs_change"
    And a new comment "/rb flow ci_improvement" is added
    When the daemon evaluates routing
    Then it routes the task to flow "ci_improvement"

  Scenario: Label routes work when no explicit command exists
    Given an issue has label "rb:flow:docs_change"
    And no explicit routing command is present
    When the daemon evaluates routing
    Then it routes the task to flow "docs_change"
```

```gherkin
Feature: Conformance execution

  Scenario: Conformance lists and runs scenarios
    Given a built Ralph Burning binary exists
    When I run "ralph-burning conformance list"
    Then available scenario ids are listed
    When I run "ralph-burning conformance run --filter workflow.standard.single_cycle"
    Then only matching scenarios execute
    And the command exits non-zero if a selected scenario fails
```

# Testing and Validation Strategy

## Core principles

* treat conformance as the top acceptance layer
* make stage transitions and validation rules testable without filesystem, git, or subprocesses
* test stage contracts separately from renderers separately from adapters
* test each flow preset directly
* do not write tests around legacy `.ralph` compatibility

## Workspace Governance

* **Unit tests**

  * config precedence
  * workspace version validation
  * active project resolution
  * flow catalog listing/show behavior

* **Domain behavior tests**

  * init rules
  * unsupported workspace behavior
  * override resolution

* **Application/service tests**

  * initialize workspace
  * set active project
  * resolve effective config
  * list/show flows

* **Contract tests**

  * config serialization/deserialization
  * flow catalog read-model output shape

* **Adapter tests**

  * filesystem workspace store
  * config file roundtrip
  * template path resolution

* **Integration tests**

  * `init`
  * `config`
  * `project select`
  * `flow list`
  * `flow show`

* **Property/invariant tests**

  * precedence determinism
  * unsupported versions always fail

## Project & Run Record

* **Unit tests**

  * project aggregate mutation
  * stage cursor updates
  * cycle/completion numbering
  * durable history vs runtime log classification
  * rollback-point selection

* **Domain behavior tests**

  * one active run per project
  * immutable flow preset
  * journal ordering
  * amendment queue behavior

* **Application/service tests**

  * create/delete/select project
  * start run
  * record stage result
  * record runtime log
  * query status/history/tail
  * rollback

* **Contract tests**

  * project state serialization
  * run state serialization
  * journal event schema
  * artifact/payload index format

* **Adapter tests**

  * filesystem project store
  * atomic snapshot writes
  * log store behavior
  * VCS snapshot adapter

* **Integration tests**

  * temp-dir project persistence
  * temp-repo rollback/status/history tests

* **Property/invariant tests**

  * any legal event sequence reconstructs a valid read model
  * runtime logs never appear as durable history records

## Workflow Composition

* **Unit tests**

  * transition tables per preset
  * stage legality
  * validation profile selection
  * completion consensus
  * final-review consensus
  * retry classification

* **Domain behavior tests**

  * prompt review entry
  * QA remediation
  * review remediation
  * pre-commit invalidation
  * final-review enable/disable by preset
  * flow immutability

* **Application/service tests**

  * advance stage with fake ports
  * resume behavior
  * stop conditions
  * structured-output failure handling
  * artifact rendering handoff

* **Contract tests**

  * schema tests for every stage contract
  * semantic validation tests
  * renderer golden tests

* **Adapter tests**

  * template adapter
  * validation command adapter
  * VCS workspace adapter

* **Integration tests**

  * standard preset
  * quick_dev preset
  * docs_change preset
  * ci_improvement preset

* **Property/invariant tests**

  * all legal transitions preserve run invariants
  * illegal transitions are rejected

## Agent Execution

* **Unit tests**

  * backend spec parsing
  * model resolution
  * role override rules
  * session continuity rules
  * timeout and cancel classification

* **Domain behavior tests**

  * structured-output requirement enforcement
  * backend availability handling
  * explicit model precedence

* **Application/service tests**

  * invocation with fake transports
  * normalized output envelope mapping
  * cancel semantics

* **Contract tests**

  * one contract suite per backend adapter:

    * invocation request
    * structured stage payload result
    * timeout/cancel behavior
    * raw output capture

* **Adapter tests**

  * Claude CLI adapter
  * Codex CLI adapter
  * OpenRouter adapter
  * tmux adapter
  * process cleanup

* **Integration tests**

  * subprocess mocks for:

    * schema-valid result
    * malformed JSON
    * schema-invalid payload
    * timeout
    * non-zero exit
    * cancellation

* **Property/invariant tests**

  * backend spec roundtrip
  * resolved model determinism

## Requirements Drafting

* **Unit tests**

  * stage progression
  * question/answer routing
  * cache eligibility
  * quick-draft review loop

* **Domain behavior tests**

  * validation failure handling
  * answer application
  * approval termination

* **Application/service tests**

  * run/resume with fake execution and interaction ports
  * project-seed handoff

* **Contract tests**

  * requirements stage schemas
  * quick-draft review schema
  * validation report schema

* **Adapter tests**

  * requirements store
  * interaction adapter
  * prompt loader adapter

* **Integration tests**

  * `requirements draft`
  * `requirements quick`
  * project creation from requirements output

* **Property/invariant tests**

  * cache reuse only when input/dependency hashes match

## Automation Runtime

* **Unit tests**

  * routing precedence
  * task lifecycle
  * worktree lease rules
  * rebase eligibility
  * interactive requirements transitions

* **Domain behavior tests**

  * label-only routing
  * command-over-label routing
  * cancellation
  * terminal-state persistence ordering

* **Application/service tests**

  * poll-and-claim
  * dispatch
  * abort
  * retry
  * reconcile with fake ports

* **Contract tests**

  * GitHub adapter
  * worktree adapter
  * task launcher adapter

* **Adapter tests**

  * `gh` wrapper
  * git worktree wrapper
  * child-process watcher cleanup

* **Integration tests**

  * daemon start/status
  * label routing
  * command routing
  * draft PR behavior
  * rebase flow
  * interactive requirements issue flow

* **Property/invariant tests**

  * no duplicate active task per issue
  * no lease collision across active tasks

## Conformance Specification

* **Unit tests**

  * scenario registry
  * assertion helpers
  * fixture builders

* **Application/service tests**

  * list/run behavior
  * case filtering
  * scenario registration integrity

* **Contract tests**

  * mock backend conventions
  * mock GitHub conventions
  * fixture directory conventions

* **Integration tests**

  * `conformance list`
  * `conformance run`

* **End-to-end/conformance tests**

  * the Gherkin scenarios above

## How to test workflow/state machines

* model transitions as pure functions where possible
* use table-driven tests per preset and per stage
* explicitly test forbidden transitions
* test final-review-enabled and final-review-disabled variants separately

## How to test ports/adapters

* define contract suites per port
* run against fake adapters and real adapters when feasible
* backend adapters must prove structured payload compliance and cleanup behavior

## How to test persistence and recovery

* inject failures around atomic writes
* resume from every durable stage boundary
* test rollback with and without VCS
* test that journal + canonical state power queries, not artifact scanning

## How to map Gherkin to conformance

* store features under `tests/conformance/features/`
* each scenario gets a stable scenario id
* `conformance list` exposes those ids
* every public behavior change must update mapped scenarios

## Fast/local vs broader/slower tests

**Fast/local**

* aggregate tests
* flow transition tests
* schema tests
* renderer tests
* config tests
* backend spec tests

**Broader/slower**

* temp-dir state store tests
* temp-repo VCS tests
* subprocess backend tests
* daemon/GitHub mock tests
* full conformance suite

This is TDD-friendly because the inner loop stays fast and the acceptance layer stays explicit.

# Agent-Friendly Context Packs

## Workspace Governance

* **Context name**
  Workspace Governance

* **Short summary**
  Owns `.ralph-burning` initialization, workspace versioning, effective config, active project resolution, and flow discovery.

* **Glossary**
  Workspace, ActiveProjectRef, EffectiveConfig, WorkspaceVersion, FlowCatalogView

* **Responsibilities**

  * init workspace
  * validate workspace version
  * merge config
  * resolve active project
  * expose `flow list/show`

* **Invariants**

  * precedence is deterministic
  * unsupported version fails
  * active project must exist

* **Public contracts**

  * `InitializeWorkspace`
  * `ResolveEffectiveConfig`
  * `ResolveActiveProject`
  * `ListFlows`
  * `ShowFlow`

* **Key scenarios**

  * init
  * config precedence
  * flow listing/show
  * unsupported workspace rejection

* **Ports/adapters involved**

  * workspace store
  * config store
  * template catalog
  * lock

* **Implementation notes**

  * keep TOML and path details in adapters
  * expose typed policies only

* **Test notes**

  * mostly fast service tests and a few CLI integrations

* **Out-of-scope items**

  * run state
  * backend execution
  * requirements stage rules
  * daemon routing

* **Contamination risks**

  * pulling workflow or project-state logic into config resolution

* **Dependencies on other contexts**

  * upstream of all runtime contexts

## Project & Run Record

* **Context name**
  Project & Run Record

* **Short summary**
  Canonical project and run state, durable history, runtime logs, sessions, amendments, rollback, and operator queries.

* **Glossary**
  ProjectRecord, RunRecord, StageCursor, WorkCycle, CompletionRound, ArtifactRecord, RuntimeLogRecord, RollbackPoint

* **Responsibilities**

  * project CRUD
  * run snapshot/journal
  * payload and artifact indexing
  * runtime log storage
  * session/amendment state
  * status/history/tail/rollback

* **Invariants**

  * one active run per project
  * immutable flow preset
  * runtime logs are not durable history
  * atomic writes
  * valid stage cursor

* **Public contracts**

  * `CreateProject`
  * `LoadProject`
  * `SaveProject`
  * `StartRun`
  * `RecordStageResult`
  * `RecordRuntimeLog`
  * `RollbackProject`
  * `QueryStatus`
  * `QueryHistory`
  * `QueryTail`

* **Key scenarios**

  * project creation
  * status/history/tail
  * resume
  * rollback
  * log/history separation

* **Ports/adapters involved**

  * project store
  * journal store
  * artifact store
  * runtime log store
  * VCS snapshot adapter
  * lock

* **Implementation notes**

  * state+journal are authoritative
  * artifacts are evidence, not state source

* **Test notes**

  * aggregate tests plus temp-dir/temp-repo adapters

* **Out-of-scope items**

  * transition policy
  * backend transport
  * requirements stage logic
  * GitHub routing

* **Contamination risks**

  * embedding workflow behavior inside the aggregate

* **Dependencies on other contexts**

  * Workspace Governance for project selection/config semantics

## Workflow Composition

* **Context name**
  Workflow Composition

* **Short summary**
  Shared workflow engine with built-in presets, stage handlers, structured contracts, validation profiles, completion rules, and final-review rules.

* **Glossary**
  FlowPreset, Stage, StageContract, StagePayload, ValidationProfile, CompletionRound, FinalReviewRound

* **Responsibilities**

  * define preset catalog
  * advance stages
  * validate outputs
  * apply retry policy
  * manage completion/final review
  * choose validation profile

* **Invariants**

  * every stage uses a contract
  * schema validation precedes domain validation
  * markdown is derived from payloads
  * flow preset is fixed per project
  * final review only when preset enables it

* **Public contracts**

  * `StartRun`
  * `ResumeRun`
  * `AdvanceStage`
  * `RetryStage`
  * `AbortRun`

* **Key scenarios**

  * standard happy path
  * QA remediation
  * review cap
  * completion
  * final-review reopen
  * quick_dev shared engine
  * docs/CI presets

* **Ports/adapters involved**

  * project/run port
  * agent execution
  * prompt catalog
  * validation runner
  * VCS workspace
  * clock/id/event sink

* **Implementation notes**

  * use typed built-in presets, not a user DSL
  * keep stage handlers small and composable
  * stage contracts should be easy to test in isolation

* **Test notes**

  * table-driven transition tests
  * schema tests
  * renderer goldens
  * fake-port use-case tests

* **Out-of-scope items**

  * filesystem layout
  * git command strings
  * backend CLI flags

* **Contamination risks**

  * directly calling filesystem/git/processes from policy code

* **Dependencies on other contexts**

  * Workspace Governance
  * Project & Run Record
  * Agent Execution

## Agent Execution

* **Context name**
  Agent Execution

* **Short summary**
  Resolves and invokes supported backends, guaranteeing structured stage payloads and handling timeout/cancel/session concerns.

* **Glossary**
  BackendSpec, ModelSpec, InvocationRequest, StructuredOutputResult, SessionKey, TimeoutPolicy

* **Responsibilities**

  * parse specs
  * resolve role/model assignments
  * check availability
  * invoke backends
  * normalize structured output
  * capture raw output
  * cleanup on cancel/timeout

* **Invariants**

  * supported backends must satisfy stage contracts
  * explicit model wins
  * raw output is preserved
  * cleanup always runs on cancel/timeout

* **Public contracts**

  * `ResolveBackendForRole`
  * `ResolvePanelBackends`
  * `CheckAvailability`
  * `Invoke`
  * `Cancel`

* **Key scenarios**

  * role override behavior
  * structured-output success
  * malformed output failure
  * timeout cleanup
  * unsupported backend rejection

* **Ports/adapters involved**

  * backend transport
  * process runner
  * tmux
  * logging

* **Implementation notes**

  * isolate backend quirks in adapters
  * return one normalized envelope shape

* **Test notes**

  * policy tests plus adapter contract tests

* **Out-of-scope items**

  * workflow transitions
  * project state mutation
  * final-review policy

* **Contamination risks**

  * letting backend adapters infer workflow outcomes

* **Dependencies on other contexts**

  * driven by Workflow Composition, Requirements Drafting, Automation Runtime

## Requirements Drafting

* **Context name**
  Requirements Drafting

* **Short summary**
  Handles staged and quick requirements generation, validation, caching, and handoff into project creation.

* **Glossary**
  RequirementsRun, QuickRequirementsRun, QuestionRound, Revision, ValidationReport

* **Responsibilities**

  * run/resume requirements drafts
  * manage question loops
  * validate requirements
  * cache stage outputs
  * produce project seed outputs

* **Invariants**

  * stage order is fixed
  * cache reuse requires matching hashes
  * approval ends quick-draft review

* **Public contracts**

  * `RunRequirementsDraft`
  * `RunQuickRequirements`
  * `ApplyAnswers`
  * `ShowRequirements`
  * `ProduceProjectSeed`

* **Key scenarios**

  * staged draft run
  * cached resume
  * quick draft approval
  * create project from approved draft

* **Ports/adapters involved**

  * agent execution
  * prompt catalog
  * requirements store
  * interaction

* **Implementation notes**

  * keep requirements separate from delivery workflow until explicit handoff

* **Test notes**

  * state-machine tests
  * cache tests
  * schema tests
  * CLI integration

* **Out-of-scope items**

  * run cycles
  * GitHub routing
  * VCS rollback

* **Contamination risks**

  * pushing delivery workflow concepts into requirements flow

* **Dependencies on other contexts**

  * Workspace Governance
  * Agent Execution

## Automation Runtime

* **Context name**
  Automation Runtime

* **Short summary**
  Daemon task routing and execution via labels and explicit commands, with worktree leases, draft PR behavior, rebase, and interactive requirements support.

* **Glossary**
  DaemonTask, WorktreeLease, RoutingCommand, RoutingLabel, RoutingDecision, DraftPrWatcher

* **Responsibilities**

  * poll/claim
  * parse labels and commands
  * route to flow preset or requirements path
  * manage worktree leases
  * launch and monitor tasks
  * abort/retry/reconcile
  * draft PR watcher
  * rebase

* **Invariants**

  * one active task per issue
  * one lease per task
  * command beats label
  * label beats repo default
  * terminal state persists before cleanup

* **Public contracts**

  * `PollAndClaim`
  * `ResolveDispatch`
  * `DispatchTask`
  * `AbortTask`
  * `RetryTask`
  * `Reconcile`

* **Key scenarios**

  * label routing
  * explicit command routing
  * task abort
  * retry
  * draft PR update
  * rebase flow

* **Ports/adapters involved**

  * GitHub
  * worktree
  * workflow port
  * requirements port
  * task launcher

* **Implementation notes**

  * keep routing policy separate from process supervision
  * encode label/command vocabulary centrally

* **Test notes**

  * fake-port lifecycle tests plus integration with temp repos and mock GitHub

* **Out-of-scope items**

  * stage internals
  * schema definitions
  * project aggregate mutation rules

* **Contamination risks**

  * embedding GitHub concepts into other contexts

* **Dependencies on other contexts**

  * Workspace Governance
  * Project & Run Record
  * Workflow Composition
  * Requirements Drafting
  * Agent Execution

## Conformance Specification

* **Context name**
  Conformance Specification

* **Short summary**
  Binary-level acceptance harness and scenario registry for `ralph-burning`.

* **Glossary**
  ScenarioId, ConformanceCase, Fixture, MockBackend, HarnessRun

* **Responsibilities**

  * register scenarios
  * build fixtures
  * execute binary
  * assert public behavior

* **Invariants**

  * deterministic tests
  * public behavior only
  * new behavior requires scenario coverage

* **Public contracts**

  * `ListCases`
  * `RunCases`

* **Key scenarios**

  * init
  * flow discovery
  * project creation
  * standard run
  * quick_dev/docs/CI presets
  * completion/final review
  * requirements
  * daemon routing
  * rollback
  * conformance command

* **Ports/adapters involved**

  * binary runner
  * fixture builder
  * mock backends
  * mock GitHub
  * assertion helpers

* **Implementation notes**

  * every scenario gets a stable id
  * fixtures should remain readable and minimal

* **Test notes**

  * harness unit tests plus full conformance suite in CI

* **Out-of-scope items**

  * runtime internals

* **Contamination risks**

  * asserting file layout or internal implementation instead of observable behavior

* **Dependencies on other contexts**

  * only their public surfaces

# Rewrite Roadmap

## Slice 1: Rewrite charter, scenario baseline, and CLI vocabulary

* **Slice name**
  Rewrite charter and public vocabulary

* **Bounded context**
  Conformance Specification + shared contracts

* **Why it exists**
  The rewrite needs a stable target language before code starts fragmenting.

* **Goal**

  * freeze the new product vocabulary around `ralph-burning`
  * define scenario ids
  * define the modernized CLI surface
  * explicitly record that legacy `.ralph` compatibility is out of scope

* **Prerequisites**

  * none

* **Exact scope**

  * Gherkin scenario catalog
  * conformance harness skeleton
  * CLI command naming decisions
  * glossary and flow preset ids

* **Out-of-scope**

  * runtime implementation

* **Files/modules likely affected**

  * `tests/conformance/features/*`
  * `contexts/conformance_spec/*`
  * `docs/rewrite/*`

* **Ports/contracts introduced or changed**

  * scenario registry
  * initial shared ids/enums

* **Scenarios covered**

  * init
  * flow list/show
  * project create/select
  * conformance list/run

* **Test strategy**

  * scenario registration tests
  * conformance harness smoke tests

* **Migration strategy**

  * none; this is planning/scaffolding

* **Rollback or safety considerations**

  * zero runtime behavior change

* **Risks**

  * weak public vocabulary leading to later architectural drift

## Slice 2: Workflow kernel primitives and stage contract framework

* **Slice name**
  Workflow kernel primitives

* **Bounded context**
  Workflow Composition + shared contracts

* **Why it exists**
  The shared engine needs a clear type system before persistence and execution are built.

* **Goal**

  * define flow presets, stage definitions, stage contracts, validation profile types, stage cursor, and renderer framework

* **Prerequisites**

  * Slice 1

* **Exact scope**

  * `FlowDefinition`
  * `StageDefinition`
  * `StageContract`
  * payload envelope
  * renderer abstraction
  * retry/failure class primitives
  * preset registry scaffolding

* **Out-of-scope**

  * project persistence
  * backend invocation
  * full engine execution

* **Files/modules likely affected**

  * `shared/contracts/*`
  * `contexts/workflow_composition/domain/*`

* **Ports/contracts introduced or changed**

  * stage contract DTOs
  * flow preset DTOs
  * validation profile DTOs

* **Scenarios covered**

  * flow show
  * schema/renderer determinism basics

* **Test strategy**

  * pure unit tests
  * schema tests
  * renderer golden tests

* **Migration strategy**

  * new module tree only

* **Rollback or safety considerations**

  * keep abstractions narrow
  * do not introduce a user DSL

* **Risks**

  * over-generalization

## Slice 3: Workspace Governance and flow discovery

* **Slice name**
  Workspace foundation

* **Bounded context**
  Workspace Governance

* **Why it exists**
  Every other slice depends on clean workspace and config behavior.

* **Goal**

  * implement `.ralph-burning` initialization
  * implement effective config
  * implement active project
  * implement `flow list/show`

* **Prerequisites**

  * Slice 1

* **Exact scope**

  * workspace init
  * workspace versioning
  * config get/set/show/edit
  * active project selection
  * flow discovery commands
  * template source resolution

* **Out-of-scope**

  * project storage
  * run execution

* **Files/modules likely affected**

  * `contexts/workspace_governance/*`
  * `adapters/fs_workspace/*`
  * `adapters/cli/*`

* **Ports/contracts introduced or changed**

  * `WorkspaceStorePort`
  * `ConfigStorePort`
  * `TemplateCatalogPort`

* **Scenarios covered**

  * init
  * unsupported workspace
  * config precedence
  * flow list/show

* **Test strategy**

  * service tests
  * config adapter tests
  * CLI integration tests

* **Migration strategy**

  * new workspace only

* **Rollback or safety considerations**

  * explicit workspace version file

* **Risks**

  * config sprawl if raw config objects leak across contexts

## Slice 4: Canonical project and run store

* **Slice name**
  Project and run record foundation

* **Bounded context**
  Project & Run Record

* **Why it exists**
  The new engine needs durable truth before execution.

* **Goal**

  * implement project CRUD
  * canonical run state
  * journal
  * durable payload/artifact storage
  * runtime log separation
  * query surfaces

* **Prerequisites**

  * Slices 1-3

* **Exact scope**

  * `project create/select/list/show/delete`
  * `run status/history/tail`
  * state/journal schema
  * payload store
  * artifact store
  * runtime log store
  * initial rollback-point model

* **Out-of-scope**

  * full workflow engine
  * hard VCS rollback behavior

* **Files/modules likely affected**

  * `contexts/project_run_record/*`
  * `adapters/fs_project_store/*`
  * CLI project/run query commands

* **Ports/contracts introduced or changed**

  * `ProjectStorePort`
  * `JournalStorePort`
  * `ArtifactStorePort`
  * `RuntimeLogStorePort`

* **Scenarios covered**

  * project creation
  * log/history separation
  * status/history/tail basics

* **Test strategy**

  * aggregate tests
  * filesystem store tests
  * query read-model tests

* **Migration strategy**

  * none; new format only

* **Rollback or safety considerations**

  * atomic write and corruption visibility

* **Risks**

  * poor state shape infecting later slices

## Slice 5: Agent Execution with structured stage payload guarantee

* **Slice name**
  Structured backend execution facade

* **Bounded context**
  Agent Execution

* **Why it exists**
  Workflow code must depend on one stable execution contract.

* **Goal**

  * implement backend spec parsing
  * enforce structured stage output support
  * normalize invocation results
  * preserve raw output
  * handle timeout/cancel/session rules

* **Prerequisites**

  * Slice 2

* **Exact scope**

  * supported backend catalog
  * role/model resolution
  * availability checks
  * structured invocation path
  * cancellation
  * timeout cleanup
  * raw output capture

* **Out-of-scope**

  * workflow transitions
  * requirements stage logic

* **Files/modules likely affected**

  * `contexts/agent_execution/*`
  * `adapters/backend_*/*`
  * `adapters/process/*`
  * `adapters/tmux/*`

* **Ports/contracts introduced or changed**

  * `AgentExecutionPort`
  * `BackendTransportPort`
  * `ProcessRunnerPort`
  * `TmuxPort`

* **Scenarios covered**

  * role override
  * structured output success/failure
  * timeout cleanup
  * unsupported backend rejection

* **Test strategy**

  * policy tests
  * adapter contract tests
  * subprocess mocks

* **Migration strategy**

  * new facade lives independently until engine uses it

* **Rollback or safety considerations**

  * raw output capture must survive failures

* **Risks**

  * backend adapters under-specifying structured output behavior

## Slice 6: First vertical slice on the shared engine

* **Slice name**
  Standard happy-path execution

* **Bounded context**
  Workflow Composition

* **Why it exists**
  The rewrite needs an early end-to-end proof that the new architecture works.

* **Goal**

  * run one successful standard cycle using:

    * new workspace
    * new project/run store
    * structured payloads
    * rendered history artifacts

* **Prerequisites**

  * Slices 2-5

* **Exact scope**

  * `standard` preset
  * stages: planning, implementation, QA, review
  * `run start`
  * durable payloads + rendered artifacts
  * journal updates

* **Out-of-scope**

  * retries
  * prompt review
  * completion
  * final review
  * docs/CI/quick_dev presets

* **Files/modules likely affected**

  * `contexts/workflow_composition/*`
  * `shared/contracts/stage_contracts/*`
  * CLI `run start`

* **Ports/contracts introduced or changed**

  * stage handler contracts
  * renderer contracts

* **Scenarios covered**

  * standard happy path
  * structured output canonicality
  * history artifact rendering

* **Test strategy**

  * transition tests
  * fake-port service tests
  * conformance scenarios for standard happy path

* **Migration strategy**

  * none; this is the first real runtime path

* **Rollback or safety considerations**

  * keep scope narrow
  * do not add completion/final review yet

* **Risks**

  * too much scope in the first vertical slice

## Slice 7: Resilience semantics

* **Slice name**
  Retry, remediation, prompt review, and resume

* **Bounded context**
  Workflow Composition + Project & Run Record + Agent Execution

* **Why it exists**
  Safety and recovery are core product value, not optional polish.

* **Goal**

  * implement QA remediation
  * implement review remediation
  * implement prompt review
  * implement pre-commit invalidation
  * implement resume
  * implement failure-class retry policy

* **Prerequisites**

  * Slice 6

* **Exact scope**

  * QA fail/pass loop
  * review change-request loop
  * prompt review stage
  * pre-commit validation
  * timeout/schema/domain failure classification
  * session reuse/reset rules
  * `run resume`

* **Out-of-scope**

  * completion/final review
  * quick_dev/docs/CI presets
  * requirements
  * daemon

* **Files/modules likely affected**

  * workflow policies
  * stage handlers
  * validation adapters
  * project/run state mutation

* **Ports/contracts introduced or changed**

  * `ValidationRunnerPort`
  * richer failure/result envelopes

* **Scenarios covered**

  * QA remediation
  * review cap
  * prompt review
  * resume
  * pre-commit invalidation

* **Test strategy**

  * transition tables
  * contract tests
  * conformance scenarios

* **Migration strategy**

  * broaden new engine coverage

* **Rollback or safety considerations**

  * every durable boundary must be resumable

* **Risks**

  * muddled retry categories

## Slice 8: Completion and final review

* **Slice name**
  Completion and preset-driven final acceptance

* **Bounded context**
  Workflow Composition + Project & Run Record

* **Why it exists**
  Completion/final review are distinct policy clusters and should come after core cycle stability.

* **Goal**

  * add completion rounds
  * add acceptance QA
  * add final review for presets that enable it
  * add amendment queue integration
  * add cap behavior

* **Prerequisites**

  * Slice 7

* **Exact scope**

  * completion panel
  * acceptance QA
  * final review handlers/contracts
  * amendment acceptance/rejection
  * preset-specific final review enablement
  * force-complete rules where applicable

* **Out-of-scope**

  * daemon routing
  * requirements

* **Files/modules likely affected**

  * completion/final-review policies
  * amendment queue integration
  * stage contracts/renderers

* **Ports/contracts introduced or changed**

  * completion policy contracts
  * final-review contracts

* **Scenarios covered**

  * completion success/failure
  * acceptance QA fail
  * final-review reopen
  * final-review disabled presets
  * cap behavior

* **Test strategy**

  * consensus tests
  * stage contract tests
  * conformance scenarios

* **Migration strategy**

  * standard preset becomes fully capable

* **Rollback or safety considerations**

  * accepted amendments must persist before reopening work

* **Risks**

  * complex final-review contract design

## Slice 9: Preset library expansion

* **Slice name**
  Quick dev, docs, and CI presets

* **Bounded context**
  Workflow Composition

* **Why it exists**
  This is the architecture payoff: new flows become cheap.

* **Goal**

  * implement `quick_dev`, `docs_change`, and `ci_improvement` on the same engine
  * wire their validation profiles and final-review defaults

* **Prerequisites**

  * Slices 2, 6, 7, 8

* **Exact scope**

  * quick_dev preset
  * docs_change preset
  * ci_improvement preset
  * preset-specific validation profiles
  * preset-specific final-review policy

* **Out-of-scope**

  * user-defined workflows

* **Files/modules likely affected**

  * flow catalog
  * validation profile registry
  * stage library
  * CLI project creation validation

* **Ports/contracts introduced or changed**

  * preset registry API
  * validation profile registry API

* **Scenarios covered**

  * quick_dev shared engine
  * docs validation flow
  * CI validation flow
  * final review disabled by preset

* **Test strategy**

  * preset transition tests
  * validation profile tests
  * conformance scenarios

* **Migration strategy**

  * preset catalog becomes operator-visible through `flow show`

* **Rollback or safety considerations**

  * avoid preset-specific fields in state model

* **Risks**

  * hiding preset-specific behavior in ad hoc hooks

## Slice 10: Requirements Drafting and project handoff

* **Slice name**
  Requirements drafting and project seed handoff

* **Bounded context**
  Requirements Drafting + Workspace Governance + Project & Run Record

* **Why it exists**
  Requirements are a real product capability and should plug cleanly into the new architecture.

* **Goal**

  * implement staged requirements draft
  * implement quick draft
  * implement validation and cache
  * hand off approved drafts into project creation

* **Prerequisites**

  * Slices 3-5 and basic project creation

* **Exact scope**

  * `requirements draft`
  * `requirements quick`
  * `requirements show`
  * `requirements answer`
  * project creation from requirements seed

* **Out-of-scope**

  * daemon interactive requirements routing

* **Files/modules likely affected**

  * `contexts/requirements_drafting/*`
  * CLI requirements commands
  * project creation handoff logic

* **Ports/contracts introduced or changed**

  * `RequirementsPort`
  * project-seed DTOs

* **Scenarios covered**

  * staged draft run
  * cached resume
  * quick draft approval
  * create project from requirements output

* **Test strategy**

  * state-machine tests
  * cache tests
  * conformance scenarios

* **Migration strategy**

  * none; new command surface only

* **Rollback or safety considerations**

  * validated requirements required before project handoff

* **Risks**

  * requirements and delivery workflows becoming too tightly coupled

## Slice 11: Automation Runtime

* **Slice name**
  Daemon, routing, worktrees, and issue automation

* **Bounded context**
  Automation Runtime

* **Why it exists**
  It depends on most other contracts and should come late.

* **Goal**

  * implement daemon lifecycle
  * implement label and explicit command routing
  * implement worktree leases
  * dispatch workflow or requirements tasks
  * implement draft PR watcher and rebase
  * implement interactive requirements issue path

* **Prerequisites**

  * Slices 3-10

* **Exact scope**

  * `daemon start/status/abort/retry/reconcile`
  * routing precedence
  * task state
  * worktree allocation
  * task execution
  * draft PR update
  * rebase flow
  * interactive requirements

* **Out-of-scope**

  * service extraction

* **Files/modules likely affected**

  * `contexts/automation_runtime/*`
  * `adapters/github/*`
  * `adapters/git/*`
  * CLI daemon commands

* **Ports/contracts introduced or changed**

  * `GitHubPort`
  * `WorktreePort`
  * `TaskProcessPort`
  * `WorkflowPort`
  * `RequirementsPort`

* **Scenarios covered**

  * command-over-label routing
  * label-only routing
  * task abort/retry
  * draft PR behavior
  * rebase
  * interactive requirements

* **Test strategy**

  * fake-port lifecycle tests
  * integration with temp repos and mock GitHub
  * conformance scenarios

* **Migration strategy**

  * none; new daemon only

* **Rollback or safety considerations**

  * terminal task state must persist before cleanup

* **Risks**

  * highest integration complexity in the rewrite

## Slice 12: Cutover, documentation, and cleanup

* **Slice name**
  Cutover and cleanup

* **Bounded context**
  All

* **Why it exists**
  The repo should not keep two architectures indefinitely.

* **Goal**

  * switch the binary fully to the new runtime
  * remove legacy orchestrator code
  * make conformance mandatory in CI
  * publish final architecture docs and context packs

* **Prerequisites**

  * Slices 1-11

* **Exact scope**

  * final command router cutover
  * docs refresh
  * CI conformance gating
  * removal of old `src/workflow/*` orchestrator-heavy paths and reconstruction-heavy state logic

* **Out-of-scope**

  * legacy workspace compatibility

* **Files/modules likely affected**

  * command routing
  * docs
  * CI config
  * old orchestration modules

* **Ports/contracts introduced or changed**

  * none major

* **Scenarios covered**

  * full supported behavior matrix

* **Test strategy**

  * full conformance suite
  * adapter contract suite
  * integration suite

* **Migration strategy**

  * code cutover only
  * existing old workspaces unsupported

* **Rollback or safety considerations**

  * maintain the old runtime as a separate release branch if needed, not mixed into the new architecture

* **Risks**

  * long-lived dual architecture if cleanup is deferred

# Parallelization Strategy for Multiple Agents

## Which slices can run concurrently

After Slice 1 stabilizes public vocabulary:

* Slice 2 (workflow primitives) and Slice 3 (workspace) can proceed in parallel.
* Slice 4 (project/run store) can proceed in parallel with Slice 5 (agent execution) once shared IDs/contracts are stable.
* Requirements Drafting can begin once Workspace Governance and Agent Execution ports exist.
* Conformance harness work can continue in parallel throughout.

After Slices 2-5:

* one agent can take the standard-flow vertical slice
* one can implement artifact renderers and stage contract schemas
* one can implement project query/read models and rollback infrastructure
* one can implement validation profile adapters

After Slice 7:

* one agent can work on completion/final review
* one can implement quick_dev/docs/CI presets
* one can implement requirements drafting

Automation Runtime should start only after workflow and requirements ports are stable.

## Handoff boundaries

Use contracts, not folders, as handoff boundaries:

* `FlowDefinition`
* `StageDefinition`
* `StageContract`
* `RunSnapshot`
* `StagePayloadEnvelope`
* `BackendSpec`
* `InvocationResult`
* `EffectiveConfig`
* `RequirementsRunState`
* `TaskRecord`

## Contract-first coordination points

Stabilize these before broad parallel work:

* shared IDs and enums
* stage contract conventions
* flow preset registry shape
* canonical run state shape
* runtime log vs history artifact model
* CLI vocabulary and scenario ids

## Merge/conflict risks

Highest-risk shared areas:

* shared DTOs
* flow catalog
* stage contract registry
* command router
* conformance fixture helpers

Mitigations:

* one owner for shared contracts
* one owner for command routing
* one owner for fixture DSL
* contract changes merged before downstream implementation branches

## What should be stabilized before parallel work starts

* product rename to `ralph-burning`
* workspace root `.ralph-burning`
* CLI surface
* flow preset list
* immutable flow-per-project rule
* structured payload rule
* log/history separation rule

## How to avoid context contamination

* give each agent only:

  * its context pack
  * public contracts
  * relevant Gherkin scenarios
* forbid direct filesystem/git calls from policy layers
* centralize adapter ownership when practical
* do not allow ad hoc utilities to become cross-context shortcuts

## How to keep prompts/context packs compact and local

* store context packs under `docs/rewrite/context-packs/`
* each slice should reference one or two context packs maximum
* each coding prompt should link to scenario ids rather than restating the whole architecture
* do not hand coding agents the old repo wholesale unless they are extracting behavior references

# Risks and Tradeoffs

* **Where DDD helps**
  Strongly in Workflow Composition, Project & Run Record, Requirements Drafting, and Automation Runtime because these contain real policy, language, and invariants.

* **Where DDD would be overkill**
  In CLI parsing, tiny render helpers, simple config serialization, and thin transport wrappers.

* **Where hexagonal architecture helps**
  Around workflow policy, project state, backend execution, requirements flows, and daemon routing because these must be insulated from filesystem, git, GitHub, and subprocess concerns.

* **Where ports/adapters would add unnecessary indirection**
  For pure renderers, small DTO mappers, tiny formatters, and static schema modules.

* **Major abstraction risk**
  Building a workflow language instead of a workflow engine. V1 should use typed built-in presets, not arbitrary user-authored flow DSLs.

* **Structured output tradeoff**
  Structured payloads remove syntax/reformat fragility, but they do not eliminate semantic validation. The system still needs domain-level checks.

* **Markdown tradeoff**
  Keeping markdown artifacts is useful for humans, but only if they remain derived outputs and never become the source of truth again.

* **Greenfield storage tradeoff**
  Dropping compatibility makes the architecture cleaner and safer, but it means no in-place upgrade path for legacy workspaces.

* **Flow immutability tradeoff**
  Fixing a flow preset per project reduces flexibility, but it sharply simplifies run-state correctness and operator expectations.

* **Final review tradeoff**
  Preset-specific final review prevents meaningless ritual on low-risk flows, but it requires thoughtful defaults and clear operator visibility.

* **CLI modernization tradeoff**
  A cleaner CLI improves usability, but it requires explicit documentation because old command muscle memory no longer applies.

# Assumptions

* The rewrite product name is `ralph-burning`.
* The new workspace root is `.ralph-burning/`.
* The rewrite is greenfield in storage and artifact format.
* The old repository is used for behavior/domain reference only.
* Legacy `.ralph` state and artifact compatibility is out of scope.
* Rust remains the implementation language.
* The target deployment model is a modular monolith.
* Flow preset is fixed per project for v1.
* The only built-in flow presets in v1 are:

  * `standard`
  * `quick_dev`
  * `docs_change`
  * `ci_improvement`
* All supported backends must return structured stage payloads.
* Human-readable markdown artifacts remain desirable and durable.
* Runtime logs remain important but are not part of durable project history.
* `flow list` and `flow show` are part of the public CLI.
* Daemon routing uses both labels and explicit commands, with commands taking precedence.
* Final review is preset-specific rather than universal.
* The suggested validation defaults in this spec are acceptable unless explicitly revised during detailed design.

# Open Questions

* Should the CLI expose `cycle` terminology everywhere, or keep a compatibility-facing `loop` alias purely as user vocabulary?
* Should `run tail` show only durable history by default, with `--logs` opt-in, or should it show a mixed recent activity stream?
* Should `quick_dev` use a single lightweight final reviewer by default, or a very small review panel?
* How should repo-specific validation targets be discovered:

  * explicit project config
  * conventional script names
  * auto-detection with override
* Should `requirements` support a convenience command that directly creates and starts a project, or should the v1 surface keep those steps explicit?
* What should the exact default label vocabulary be if `ralph-burning` must coexist with other bots on the same repository?
* How much raw backend output should be retained by default before runtime log cleanup?
* Should the workspace include a first-class `backend list` / `backend check` CLI in v1, or is backend validation during run start sufficient?

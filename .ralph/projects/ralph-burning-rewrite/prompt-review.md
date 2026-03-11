---
artifact: prompt-review
project: ralph-burning-rewrite
backend: codex
role: prompt_reviewer
created_at: 2026-03-11T17:48:31Z
---

# Prompt Review

## Issues Found
- No explicit priority/decomposition for v1 scope. The prompt mixes architecture vision, roadmap, and behavior details, which makes first implementation loop unclear about what is required immediately versus later slices.
- Multiple unresolved open questions are deferred instead of resolved (e.g., `cycle` vs `loop`, `run tail` behavior, `quick_dev` reviewer shape, daemon label vocabulary). This leaves downstream agents forced to invent defaults and causes divergence across teams.
- The prompt assumes existing terms without enforcing a canonical vocabulary. Terms like `loop`, `phase`, `artifact`, `run`, and `history` appear inconsistently, which increases parser and spec ambiguity.
- `flow` behavior is described conceptually but not in a machine-executable contract. There is no single, precise definition of legal stage transitions and state transitions per preset, so implementation teams can interpret flow sequencing differently.
- Testing requirements are broad but not operationalized as gating criteria. The prompt lacks explicit “must pass before advancing slice” and concrete conformance-run expectations for each milestone.
- Conformance is described as top-level but not linked as a hard acceptance rule. Without a required CI/test gate, “greenfield rewrite” can regress without visible failure.
- Storage model is extensive but lacks explicit file-level contracts (e.g., exact field names and required/optional keys for `project.toml`, `run.json`, journal events, payload metadata), which weakens deterministic implementation.
- Backend requirements are strong (structured payloads, structured contract) but no single source-of-truth schema format and validation process is required, inviting adapter-level inconsistency.
- Daemon/runtime behavior is specified as a context but not sufficiently constrained for safe concurrency (e.g., task claim race, lease release order, cancellation timing), creating hidden reliability gaps.
- Markdown rendering is repeatedly emphasized, but there is no explicit statement that all Markdown outputs are render-only and never a fallback parser for state reconstruction.
- There is no explicit non-goal list for first release in the prompt body; while hints exist, this invites accidental work on out-of-scope legacy migration and compatibility paths.
- Several duplicated sections (capability map, architecture, testing, context packs, roadmap) repeat requirements without adding strict precedence, increasing the chance of conflicting interpretations.

## Refined Prompt
# Implement `ralph-burning` v1 Rewrite

## 1) Objective and Source of Truth
You are implementing a greenfield Rust rewrite of the orchestrator in `ralph-burning-rewrite/`. The existing code in `multibackend-orchestration/` is **read-only reference-only** for behavior inference; do not preserve or migrate legacy `.ralph` state, artifact formats, loop directories, or git checkpoint encoding.

## 2) Product Definition
Build a modular monolith called `ralph-burning` with one binary and these hard constraints:
- Rust implementation.
- New workspace root is `.ralph-burning/`.
- Built-in flow presets are exactly: `standard`, `quick_dev`, `docs_change`, `ci_improvement`.
- No arbitrary user-defined flow DSL in v1.
- Markdown is only a rendered, human-readable artifact, not workflow protocol.
- Canonical workflow state is structured stage payload JSON + run state + journal.
- Runtime logs are ephemeral and never part of durable project history.

## 3) Non-goals
- No compatibility layer for old `.ralph` workspaces or legacy reconstruction logic.
- No service-oriented microservice split in v1.
- No full implementation of every slice before completing core canonical state and shared engine.

## 4) Canonical Vocabulary (must use consistently)
- `workspace`: orchestration root `.ralph-burning/`
- `project`: durable work item with immutable `flow`
- `run`: execution instance for a project
- `stage`: named step in a flow preset
- `stage cursor`: current stage + cycle + attempt metadata
- `work cycle`: unit of iterative delivery work
- `completion round`: unit deciding complete vs continue
- `backend family`: e.g., Claude, Codex, OpenRouter
- `stage contract`: schema + required fields + semantic validation rules
- `stage payload`: validated structured output
- `history artifact`: rendered Markdown derived from payload
- `runtime log`: operational/debug output
- `journal`: append-only domain event log
- `rollback point`: durable logical checkpoint
- `task`: daemon unit of automated work

Decision in this prompt:
- Use canonical term `cycle` in public docs and CLI; accept `loop` as deprecated alias only for informational output, not as a primary API term.

## 5) Bounded Contexts (required)
Implement the following context packages with ports/adapters boundaries:

- `contexts/workspace_governance/*`
- `contexts/project_run_record/*`
- `contexts/workflow_composition/*`
- `contexts/agent_execution/*`
- `contexts/requirements_drafting/*`
- `contexts/automation_runtime/*`
- `contexts/conformance_spec/*`
- `adapters/*` for side effects

Primary dependencies:
- `workspace_governance` is upstream for config and flow discovery.
- `project_run_record` is source of durable truth.
- `workflow_composition` drives run progression.
- `agent_execution` provides invocation + normalized structured results.
- `automation_runtime` consumes routing and dispatches into workflow/requirements.

## 6) Folder and Storage Layout
Use this model in `ralph-burning-rewrite/`:

`src/`
- `main.rs`
- composition, shared, contexts, adapters, etc. as per provided package sketch.

`.ralph-burning/`
- `workspace.toml`
- `active-project`
- `projects/<project-id>/project.toml`
- `projects/<project-id>/prompt.md`
- `projects/<project-id>/run.json`
- `projects/<project-id>/journal.ndjson`
- `projects/<project-id>/sessions.json`
- `projects/<project-id>/history/payloads/*`
- `projects/<project-id>/history/artifacts/*`
- `projects/<project-id>/runtime/logs/*`
- `projects/<project-id>/runtime/backend/*`
- `projects/<project-id>/runtime/temp/*`
- `projects/<project-id>/amendments/*`
- `projects/<project-id>/rollback/*`
- `requirements/<requirements-run-id>/*`
- `daemon/tasks/*`
- `daemon/leases/*`

You must persist:
- canonical project metadata and active run state
- payload records and payload-derived artifacts
- journal and runtime-log metadata/indexes
- rollback points and amendment queue

## 7) Core Invariants (MUST pass)
- Each project has one immutable flow preset.
- Every active run has one stage cursor.
- Stage cursor stage must belong to the project’s selected flow.
- Cycle and completion round numbers are monotonic and independent.
- Structured payload validation must happen before any state mutation.
- Domain validation must happen after schema validation, before transition commit.
- Durable project history = journal + payloads + rendered artifacts only.
- Runtime logs are never promoted into durable history.
- Starting `run start` on a project with an active run must resume or fail with explicit message; it must not create duplicate active runs.
- Resume is allowed only from durable stage boundaries.
- Terminal state transitions persist before cleanup of external side effects when possible.
- Explicit model overrides trump role defaults.
- Session reuse only for roles/backends that explicitly allow it.
- Cancellation and timeout must immediately halt retries for the run/call.
- One writer lock per project.
- One active daemon task per issue.
- One worktree lease per task.
- Command-based routing overrides label-based routing; label overrides repo default.
- Hard rollback: logical rollback must happen before repository reset.
- Unsupported workspace versions fail fast with no implicit migration.

## 8) CLI Contract (v1 surface)
All commands exit non-zero on invalid state or policy failures.

Workspace/config:
- `ralph-burning init`
- `ralph-burning config show`
- `ralph-burning config get <key>`
- `ralph-burning config set <key> <value>`
- `ralph-burning config edit`

Flow discovery:
- `ralph-burning flow list`
- `ralph-burning flow show <flow-id>`

Projects:
- `ralph-burning project create --id <id> --name <name> --prompt <file> --flow <flow-id>`
- `ralph-burning project select <id>`
- `ralph-burning project list`
- `ralph-burning project show [<id>]`
- `ralph-burning project delete <id>`

Run lifecycle:
- `ralph-burning run start`
- `ralph-burning run resume`
- `ralph-burning run status`
- `ralph-burning run history`
- `ralph-burning run tail [--logs]`
- `ralph-burning run rollback --to <target> [--hard]`

Requirements:
- `ralph-burning requirements draft --idea "<text>"`
- `ralph-burning requirements quick --idea "<text>"`
- `ralph-burning requirements show <run-id>`
- `ralph-burning requirements answer <run-id>`

Daemon:
- `ralph-burning daemon start`
- `ralph-burning daemon status`
- `ralph-burning daemon abort <task-id>`
- `ralph-burning daemon retry <task-id>`
- `ralph-burning daemon reconcile`

Conformance:
- `ralph-burning conformance list`
- `ralph-burning conformance run [--filter <scenario-id>]`

Clarifications for CLI behavior:
- `run tail` shows durable history only by default.
- `run tail --logs` appends latest runtime logs.
- `loop` is accepted only as a user-facing alias; `cycle` is canonical in docs and output schema.

## 9) Built-in Flow Presets (v1)
Flows are fixed per project at creation and cannot change.
Flow stages are deterministic and configured in code:
- `standard`: `prompt_review` (optional by config) → `planning` → `implementation` → `qa` → `review` → `completion_panel` → `acceptance_qa` → `final_review` (preset-driven)
- `quick_dev`: `plan_and_implement` → `review` → `apply_fixes` → `final_review`
- `docs_change`: `docs_plan` → `docs_update` → `docs_validation` → `review`
- `ci_improvement`: `ci_plan` → `ci_update` → `ci_validation` → `review`

Validation profile and final review defaults:
- `standard`: final review enabled.
- `quick_dev`: final review enabled, lightweight panel.
- `docs_change`: final review disabled by default.
- `ci_improvement`: final review disabled by default.

Validation profiles include all required checks named in each preset’s default profile and may add policy-specific stricter checks when explicitly configured.

## 10) Structured Stage Contracts (mandatory)
For every supported stage:
- Define `StageContract` with JSON schema.
- Persist raw backend output for diagnostics.
- Validate payload schema first.
- Apply semantic domain validation second.
- Render Markdown deterministically from payload after successful validation.
- Do not parse Markdown for state transitions.
- Persist both payload and rendered artifact atomically under project history.

Retry and failure classes must be explicit:
- transport failure
- schema validation failure
- domain-validation failure
- timeout
- cancellation
- qa/review outcome failure
Different classes must map to distinct retry/terminal policies.

## 11) Project and Run Record Rules
`project` model stores:
- id, name, prompt reference/hash, fixed flow, active run pointer, run state, stage cursor, cycle history, completion rounds, sessions, amendment queue, rollback points, status summary, and journal pointer.

Run state transitions must never infer from artifacts.  
`journal.ndjson` is the authoritative event source.
State reads (`status`, `history`, `tail`) derive from canonical run/journal, not artifact scans.

## 12) Agent Execution
Every backend invocation must produce structured stage payload-compatible output.
Engine responsibilities:
- parse backend spec and model spec
- resolve per-role backend/model
- verify backend capability for target stage contract before run start
- check availability
- invoke with timeout and cancellation
- normalize to standard invocation envelope
- preserve raw output in runtime logs
Unsupported or mis-structured backend outputs must fail the stage with explicit class and no state transition.

## 13) Automation Routing
Implement daemon task routing with these precedence rules:
1. explicit routing command (e.g., `/rb flow <flow-id>`)
2. routing label (e.g., `rb:flow:<flow-id>`)
3. repository default

Label vocabulary baseline:
- `rb:flow:standard`
- `rb:flow:quick_dev`
- `rb:flow:docs_change`
- `rb:flow:ci_improvement`
- command and label values are validated against built-in presets only.

## 14) Delivery Roadmap (Minimum v1 order)
Execute in bounded slices with explicit handoff gates:

1) Charter + v1 domain model + scenario IDs + workspace init + flow discovery.  
2) Flow preset definitions + stage contracts + renderer contracts.  
3) Workspace governance effective config + active project resolution.  
4) Project/run canonical store + journal + query commands.  
5) Agent execution facade with structured-output guarantee.  
6) Standard preset vertical path (`run start`) end-to-end with schema/renderer persistence.  
7) Retry/remediation, prompt review, resume, and failure class semantics.  
8) Completion/acceptance/final-review behavior + amendment queue.  
9) docs and CI preset integration on same engine.  
10) Requirements drafting and project-seed handoff.  
11) Daemon routing, tasks, leases, watchers, rebase, interactive requirements path.  
12) Cutover: no legacy runtime paths in v1 entrypoint; conformance gate in CI.

## 15) Testability Requirements (hard constraints)
- All public behavior is defined in Gherkin under `tests/conformance/features/`.
- Every flow preset and routing policy has scenario coverage.
- Conformance must include scenario filtering and fail-fast semantics (non-zero exit if any selected scenario fails).
- For each context, implement:
  - pure domain/unit tests for state and transition rules
  - service-level tests with fake ports
  - adapter contract tests
  - targeted integration tests
- Core invariants must have property/invariant tests where feasible.
- Regression protection: any change to public flow behavior must update/extend corresponding scenario IDs.

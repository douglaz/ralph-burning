---
artifact: prompt-review
project: parity-plan
backend: codex
role: prompt_reviewer
created_at: 2026-03-18T19:09:33Z
---

Reviewing the prompt for execution ambiguity and missing contracts first, then I’ll rewrite it into an implementation-ready version with clearer scope, dependencies, acceptance rules, and testing expectations.
The rewrite is tightening three weak spots from the original: measurable parity definition, fixed contract/CLI decisions before coding, and explicit test gates per slice so loops can stop claiming “parity” based on partial behavior.
# Prompt Review

## Issues Found
- `Practical feature parity`, `production-trustworthy`, `parity-quality`, and `sufficiently for replacement` are subjective; without measurable success criteria, different implementation loops can stop at different quality bars.
- Several interfaces are only described as `recommended`, `optional`, or `if still required`; that leaves CLI shape and scope open to reinterpretation, which creates drift across slices.
- The prompt does not require a baseline comparison against old `ralph` code, tests, docs, or help output; agents lack an authoritative reference for edge cases and failure behavior.
- Shared contracts such as `ProjectSeed`, amendment metadata, JSON DTOs, backend diagnostics, and template resolution are called out but not enforced as phase gates; parallel work can start before boundaries are stable.
- Acceptance criteria mention conformance cases but not scenario naming, fixtures, expected outputs, or verification commands; this weakens testability and sign-off.
- Manual smoke tests list areas to exercise but not setup, supported environments, or expected outcomes; failures may be environmental rather than product regressions.
- The prompt does not define what to do when old `ralph` behavior is missing, conflicting, or incompatible with the new architecture; agents may silently invent product behavior.
- Several slices combine contract design, CLI design, runtime behavior, and UX in one unit; without a standard per-slice deliverable template, “done” can mean partially implemented.
- Docs updates are only required when CLI behavior changes, but a safe cutover also needs operator-facing behavior docs and migration/sign-off artifacts.

## Refined Prompt
**`ralph-burning` Feature Parity Implementation Prompt**

### Objective
Bring `ralph-burning` to cutover-ready feature parity with old `ralph` for the surfaces explicitly listed below.

Parity means equivalent user-visible behavior, operator workflow coverage, failure handling, and inspectability for the in-scope features in this prompt. Internal implementation may differ. The new architecture remains the source of truth.

### Fixed Invariants
- Product name is `ralph-burning`.
- Workspace root is `.ralph-burning/`.
- There is no legacy `.ralph` compatibility layer.
- Canonical state is structured state plus journal.
- Structured payloads are canonical.
- Markdown artifacts are rendered history, not machine protocol.
- The shared flow engine remains the orchestration core.
- Flow preset is fixed per project.
- Preserve durable history vs runtime log separation at all times.
- Do not reintroduce artifact scanning as source of truth, markdown parsing as canonical input, preset-specific orchestrators, or large workflow god-objects.

### Legacy Reference Rules
- Treat old `ralph` behavior as the parity reference for all in-scope surfaces.
- For each slice, consult and record the exact legacy references used from old `ralph` code, tests, docs, CLI help output, or sample artifacts.
- If legacy references conflict, prefer existing automated tests first, then user-facing docs/help text, then implementation behavior.
- If legacy behavior is unclear or conflicts with the new architecture, stop and raise a concrete question instead of guessing. Only proceed with a bounded extension or behavior change if it is documented explicitly in the slice notes.

### Execution Model
- Work in slice order unless a human explicitly reorders the work.
- Do not start a later slice until the current slice meets its acceptance criteria and required contracts are stable.
- Use slices as coherent delivery units: code, tests, conformance, docs, and verification land together.
- Apply contract-first changes before broad implementation. Stabilize DTOs, config schema, CLI contract, and conformance scenario IDs first.
- If a slice requires a bounded architecture extension, document the extension and why the existing architecture cannot express the behavior cleanly.
- Target Linux/POSIX behavior first. For external dependencies such as backend binaries, GitHub access, or `tmux`, provide deterministic readiness checks and clear failure messages when unavailable.

### Required Deliverables For Every Slice
- Production code for the slice.
- Targeted unit tests for core logic.
- CLI or integration tests for user-visible contract changes.
- Conformance scenarios for each acceptance criterion.
- Docs updates for user-visible behavior, operator workflow, and any new config or CLI contracts.
- A short slice report stating: legacy references consulted, contracts changed, tests run, results, and remaining known gaps.

### Conformance Requirements
- Use the repo’s existing conformance framework and naming conventions.
- If no scenario naming convention exists, use `parity_slice<N>_<short_name>`.
- Each conformance case must define deterministic inputs, expected outputs or error conditions, and the exact command or API surface being exercised.
- When a feature depends on an unavailable external system, prefer fakes, mocks, or fixtures for automated coverage and reserve live-system checks for manual smoke tests.

### Contracts To Freeze Before Parallel Work
- CLI contract for new commands and flags.
- Versioned `ProjectSeed` shape and ingestion rules.
- Amendment source metadata shape and dedup rules.
- JSON DTOs for `run status`, `run history`, payload/artifact inspection, and backend diagnostics.
- Execution config shape for direct versus `tmux` mode and streaming behavior.
- Template override catalog API and precedence rules.

### Slice Order
1. Slice 0: P0 hardening and sign-off.
2. Slice 1: full requirements and PRD parity.
3. Slice 2: bootstrap and auto parity.
4. Slice 3: manual amendment parity.
5. Slice 4: operator UX parity.
6. Slice 5: backend operations parity.
7. Slice 6: `tmux` and streaming parity.
8. Slice 7: prompt and template override parity.

### Slice 0: P0 Hardening and Sign-off
Purpose: close correctness and hardening gaps in the current P0 implementation before adding new product surface.

Likely code areas: `src/adapters/process_backend.rs`, `src/contexts/agent_execution/service.rs`, `src/contexts/workflow_composition/engine.rs`, `src/adapters/github.rs`.

Required changes:
- Backend availability checks must require executable permission, not just path existence.
- Cancel and timeout handling must use in-process signal APIs, not shelling out to `kill`.
- Panel preflight must validate the actual required panel members for prompt-review, completion, and final-review.
- Resolution snapshots and backend-drift detection must include the final-review planner.
- GitHub compare/ref link generation must safely encode refs with reserved characters.
- Close any remaining reproducible history/runtime cleanup gaps in the current P0 surface.

Acceptance:
- Permission-check failure surfaces cleanly.
- Cancel or timeout leaves no orphan child processes.
- Panel preflight fails when a required real panel member is unavailable.
- Final-review planner drift is reported on resume.
- Compare URL generation works for refs with reserved characters.
- Current P0 conformance remains green after the hardening changes.

Done when:
- All targeted hardening issues are closed or explicitly superseded with rationale.
- No public CLI contract regresses.
- P0 conformance suite passes.

### Slice 1: Full Requirements and PRD Parity
Purpose: make `requirements draft` a staged requirements pipeline with parity to old `prd` and `quick-prd` depth.

Likely code areas: `src/contexts/requirements_drafting/*`, `src/cli/requirements.rs`.

Target behavior:
- `requirements draft` is a staged pipeline with ideation, research, synthesis, implementation specification, gap analysis, validation, conditional question round, and final project seed generation.
- The question round is required only when missing information blocks synthesis or validation; otherwise the run continues without pausing.
- `requirements quick` retains a writer/reviewer loop, structured revision feedback, approval-based termination, and validated project seed output.

Required changes:
- Expand requirements run state to support staged execution depth.
- Every stage must produce a structured payload and rendered artifact.
- Cache reuse must be keyed by input and dependency hashes.
- Question rounds must support pause and resume.
- Approved requirements must produce a stable, versioned `ProjectSeed`.
- Full and quick modes must both produce the same handoff contract shape.

Acceptance:
- Staged requirements happy path.
- Cached resume skips reusable stages.
- Question generation and answer application work end to end.
- Quick requirements supports at least one revision cycle and approval.
- Both full and quick modes produce a valid `ProjectSeed`.

Done when:
- `requirements draft` covers old PRD depth closely enough to replace it.
- `requirements quick` is production-usable, not just a simplified approximation.
- The project handoff contract is stable and documented.

### Slice 2: Bootstrap and Auto Parity
Purpose: restore the convenience of old `auto` and `quick-dev-auto` without restoring the old command names.

Likely code areas: `src/cli/project.rs`, `src/contexts/requirements_drafting/*`, `src/contexts/project_run_record/*`, `src/contexts/workspace_governance/*`.

Target CLI:
- `ralph-burning project create --from-requirements <run-id>`
- `ralph-burning project bootstrap --idea "..." --flow <preset>`
- `ralph-burning project bootstrap --from-file <requirements-file> --flow <preset>`
- `ralph-burning project bootstrap --start`

Required changes:
- Provide a stable `ProjectSeed` ingestion path for project creation.
- Support explicit handoff from requirements output to project creation.
- Support convenience bootstrap that runs quick requirements, creates or selects the project, and optionally starts a run.
- Failure before project creation completes must leave no created project state.
- Failure after project creation but during `--start` must leave a valid project with a clear run failure or not-started state, not a half-created project.

Acceptance:
- Create project from requirements output.
- Bootstrap a standard project.
- Bootstrap a `quick_dev` project.
- Bootstrap with `--start`.
- Failure paths leave no partial project state or ambiguous active project selection.

Done when:
- Users do not need to manually stitch requirements and project creation unless they choose to.
- The new bootstrap flow matches old convenience value without reintroducing old command names.

### Slice 3: Manual Amendment Parity
Purpose: restore explicit operator amendment intake outside daemon and PR-review paths.

Likely code areas: `src/cli/project.rs` or `src/cli/amend.rs`, `src/contexts/project_run_record/*`, `src/contexts/workflow_composition/*`.

Target CLI:
- `ralph-burning project amend add --text "..."`
- `ralph-burning project amend add --file ./amendment.md`
- `ralph-burning project amend list`
- `ralph-burning project amend remove <id>`
- `ralph-burning project amend clear`

Required changes:
- Support manual amendment creation from inline text and file input.
- Support listing, removing, and clearing amendments.
- Store amendment source metadata such as `manual`, `pr_review`, or `issue_command`.
- Define and document deterministic dedup behavior. At minimum, identical pending manual amendments must not accumulate silently.
- Pending amendments must participate in completion and final-review gating.
- Adding a new manual amendment to a completed project must reopen the project in a defined, testable way.
- Manual amendment ingestion must appear in durable history.

Acceptance:
- Manual amendment add, list, and remove.
- Amendments persist across restart.
- Pending amendments block completion.
- Adding a manual amendment to a completed project reopens it correctly.

Done when:
- Operators can inject work without GitHub or PR-review flows.
- Manual and automated amendments behave consistently.

### Slice 4: Operator UX Parity
Purpose: restore daily operator inspection and rollback ergonomics.

Likely code areas: `src/cli/run.rs`, `src/contexts/project_run_record/queries.rs`, `src/contexts/project_run_record/service.rs`.

Target CLI:
- `ralph-burning run status --json`
- `ralph-burning run history --verbose`
- `ralph-burning run history --json`
- `ralph-burning run tail --last <n>`
- `ralph-burning run tail --follow`
- `ralph-burning run tail --logs`
- `ralph-burning run rollback --list`
- `ralph-burning run show-payload <payload-id>`
- `ralph-burning run show-artifact <artifact-id>`

Required changes:
- JSON outputs must be stable, documented, and script-friendly.
- History and tail views must be stage-aware.
- Payload and artifact inspection must resolve canonical stored objects, not inferred filesystem scans.
- Rollback listing must enumerate valid rollback targets clearly enough for operators to act without manual filesystem inspection.

Acceptance:
- `status --json`.
- `history --verbose`.
- `history --json`.
- `tail --last`.
- `tail --follow`.
- Payload and artifact inspection.
- Rollback target discovery.

Done when:
- Daily debugging and inspection do not require filesystem spelunking.
- All core run state is inspectable from the CLI.

### Slice 5: Backend Operations Parity
Purpose: expose operational backend diagnostics outside a run.

Likely code areas: new `src/cli/backend.rs`, `src/contexts/agent_execution/*`, `src/contexts/workspace_governance/config.rs`.

Target CLI:
- `ralph-burning backend list`
- `ralph-burning backend check`
- `ralph-burning backend show-effective`
- `ralph-burning backend probe --role <role> --flow <flow> [--cycle <n>]`

Required changes:
- `backend list` must show supported backends and enablement state.
- `backend check` must validate availability and readiness for required configured backends.
- `backend show-effective` must expose resolved role, backend, model, timeout, session policy, and config source precedence.
- `backend probe` must preview stage or panel resolution for a given role and flow.
- Output must make workspace, project, and CLI precedence explicit.

Acceptance:
- `backend list`.
- `backend check`.
- `backend show-effective`.
- `backend probe` for a completion panel.
- `backend probe` for a final-review panel.

Done when:
- Operators can understand backend resolution without reading config and source side by side.
- Broken backend setups are diagnosable before a run starts.

### Slice 6: `tmux` and Streaming Parity
Purpose: restore optional `tmux` execution mode and live streaming behavior without changing durable history semantics.

Likely code areas: new `src/adapters/tmux.rs`, `src/contexts/agent_execution/*`, `src/cli/run.rs`, config model.

Required config and CLI:
- `[execution] mode = "direct" | "tmux"`
- `[execution] stream_output = true | false`
- `ralph-burning run attach`
- `ralph-burning run tail --follow --logs`

Required changes:
- Provide a `tmux` adapter and configurable direct versus `tmux` execution mode.
- Live output may stream into runtime logs only; durable history must remain canonical and equivalent across modes.
- `run attach` must provide the operator path for active `tmux` sessions.
- Cancel and timeout must clean up `tmux` child work correctly.
- If `tmux` is unavailable, surface a clear readiness error and keep direct mode working.

Acceptance:
- Start a `tmux`-enabled run.
- Live tail sees runtime output.
- Cancel in `tmux` mode cleans up the session.
- Direct and `tmux` modes produce equivalent durable history, excluding explicitly ephemeral runtime-log metadata.

Done when:
- `tmux` users can operate `ralph-burning` with parity to old workflow habits.
- Streaming is usable without weakening durability guarantees.

### Slice 7: Prompt and Template Override Parity
Purpose: support workspace and project prompt template overrides for workflow and requirements paths.

Likely code areas: `src/contexts/workspace_governance/*`, prompt/template adapters, workflow prompt builders, requirements prompt builders.

Required precedence and layout:
- Project override takes precedence over workspace override.
- Workspace override takes precedence over built-in default.
- Workspace layout: `.ralph-burning/templates/<contract-or-stage>.md`
- Project layout: `.ralph-burning/projects/<project-id>/templates/<contract-or-stage>.md`

Required changes:
- Add a template catalog API that resolves overrides by precedence.
- Use the same override resolution for workflow prompts and requirements prompts.
- Reject malformed overrides clearly and safely, including the file path and validation reason.
- Overrides must not weaken stage-contract safety or allow canonical-state corruption.

Acceptance:
- Workspace template override.
- Project template override.
- Project override beats workspace override.
- Malformed override is rejected.

Done when:
- Prompt customization is possible without code changes.
- Override behavior is deterministic and safe.

### Parallelization and Conflict Rules
- If humans parallelize the work, land contract-first changes before downstream implementation changes.
- Assign a single clear owner for changes touching `src/shared/domain.rs`, `src/contexts/workspace_governance/config.rs`, `src/cli/project.rs`, `src/cli/run.rs`, `src/contexts/project_run_record/*`, and `src/contexts/agent_execution/*`.
- If work is being done by a single implementation loop, follow slice order and treat the same files as merge-risk hotspots that require extra review.

### Sign-off Requirements
- All slice acceptance criteria pass.
- All parity conformance cases exist and pass.
- Current P0 conformance is green after Slice 0 and remains green through final sign-off.
- No open correctness issues remain in backend, process, panel, or GitHub basics.
- Project seeds are stable.
- Manual and automated amendment paths behave consistently.
- Backend readiness is diagnosable outside a run.
- Workspace and project template overrides work.
- Repo docs point users only at `ralph-burning`.

### Manual Smoke Matrix
- Standard flow with Claude.
- Standard flow with Codex.
- Standard flow with OpenRouter.
- `quick_dev` flow.
- `docs_change` flow with configured docs validation.
- `ci_improvement` flow with configured CI validation.
- Full requirements draft.
- Quick requirements.
- Create project from requirements.
- Bootstrap and start run.
- Single-repo daemon routing by label.
- Single-repo daemon routing by explicit command.
- Multi-repo daemon polling.
- Draft PR creation.
- PR review ingestion into amendments.
- Rebase flow on changed default branch.

For each smoke item, record environment, command, pass or fail result, and any follow-up bug.

### Exit Criteria
- All slices are marked done.
- All required conformance cases pass.
- The manual smoke matrix is green.
- Old `ralph` provides no still-required feature missing from `ralph-burning`.
- The repository is ready to direct users only to `ralph-burning`.

# `ralph-burning` Feature Parity Plan

This document is the **implementation checklist and execution plan** for bringing `ralph-burning` to practical feature parity with old `ralph`.

It assumes the current `ralph-burning` architecture remains intact:

- product name: `ralph-burning`
- workspace root: `.ralph-burning/`
- no legacy `.ralph` compatibility
- canonical state + journal
- structured payloads are canonical
- markdown artifacts are rendered history
- shared flow engine remains the orchestration core
- flow preset is fixed per project

This plan is split into:

- **Track A — parity hardening/sign-off**
- **Track B — remaining feature parity**

It is written so an implementation agent can work slice by slice.

---

# 1. Goal

Retire old `ralph` safely by ensuring `ralph-burning` has:

1. production-trustworthy implementations for the current P0 surface
2. the remaining missing feature areas from old `ralph`
3. conformance coverage strong enough to support cutover

---

# 2. Current baseline

This plan assumes the repo already has:

- modern CLI nouns: `config`, `conformance`, `daemon`, `flow`, `init`, `project`, `requirements`, `run`
- shared workflow engine with built-in presets
- canonical project/run state
- structured stage contracts
- durable history vs runtime-log separation
- P0 foundations for backends, workflow panels, validation, checkpoints, and daemon/GitHub runtime

This plan does **not** re-open architecture decisions unless a slice explicitly requires a bounded extension.

---

# 3. Non-goals

These are out of scope unless explicitly added later:

- legacy `.ralph` compatibility
- user-defined workflow DSLs
- major architecture rewrites
- replacing the shared flow engine with preset-specific orchestrators
- making markdown the machine protocol again

---

# 4. Execution rules

## 4.1 Keep the new architecture

Do not reintroduce:

- separate quick-dev orchestrator
- artifact scanning as the source of truth
- giant workflow god-objects
- markdown parsing as canonical state input

## 4.2 Use slices as the unit of work

Each slice should land as a coherent increment with:

- code
- tests
- conformance scenarios
- docs updates if the CLI or product behavior changes

## 4.3 Prefer contract-first changes

Before broad implementation in a slice, stabilize:

- DTOs
- config schema
- CLI contract
- conformance scenario ids

## 4.4 Preserve durable history vs runtime log separation

Always keep:

- **durable history** = journal + payloads + rendered artifacts
- **runtime logs** = ephemeral debug/operational evidence

---

# 5. Track A — parity hardening / sign-off

This track does not add major new product surface. It makes the existing P0 implementation safe to trust.

## Slice 0 — P0 hardening and sign-off

### Purpose
Close the correctness and hardening gaps in the current implementation before adding more surface area.

### Issues to close

- executable-permission checks during backend availability
- replace shell-out `kill` with in-process signal handling
- panel preflight should validate real panel members, not a synthetic representative target
- include final-review reviewers and arbiter in resolution snapshots
- compare-ref URL encoding in GitHub compare links
- any remaining stray history/runtime cleanup gaps if still reproducible

### Expected code areas

- `src/adapters/process_backend.rs`
- `src/contexts/agent_execution/service.rs`
- `src/contexts/workflow_composition/engine.rs`
- `src/adapters/github.rs`
- related unit/conformance tests

### Checklist

- [ ] Backend availability checks require executability, not just file existence
- [ ] Cancel/timeout uses in-process signal APIs and preserves deterministic failure handling
- [ ] Prompt-review, completion, and final-review panel preflight validates actual required participants
- [ ] Final-review reviewers and arbiter are included in resolution snapshots and backend-drift detection
- [ ] GitHub compare/ref URL building uses safe path/URL encoding
- [ ] Current P0 conformance remains green after hardening changes

### Acceptance criteria

- [ ] New conformance case: permission check failure surfaces cleanly
- [ ] New conformance case: cancel/timeout leaves no orphan child processes
- [ ] New conformance case: panel preflight fails when a required real panel member is unavailable
- [ ] New conformance case: final-review arbiter drift is reported on resume
- [ ] New conformance case: compare URL generation works for refs with reserved characters

### Definition of done

- [ ] All targeted hardening issues are closed or explicitly superseded
- [ ] No public CLI contract regression
- [ ] P0 conformance suite passes

---

# 6. Track B — remaining feature parity

## Slice 1 — Full requirements / PRD parity

### Purpose
Bring `requirements` to parity with old `prd` + `quick-prd` depth while keeping the new CLI vocabulary.

### Target behavior

`requirements draft` should become a real staged pipeline, not just a simplified drafting pass.

### Required stages

- [ ] ideation / framing
- [ ] research / context gathering
- [ ] synthesis
- [ ] implementation specification
- [ ] gap analysis / missing-information detection
- [ ] validation
- [ ] optional question round
- [ ] final project seed generation

### Required quick-draft behavior

- [ ] writer/reviewer loop retained
- [ ] structured revision feedback
- [ ] approval terminates loop
- [ ] validated project seed output

### Expected code areas

- `src/contexts/requirements_drafting/*`
- `src/cli/requirements.rs`
- requirements-related tests and conformance

### Checklist

- [ ] Requirements run state expanded to support staged pipeline depth
- [ ] Each stage has structured payload + rendered artifact
- [ ] Stage cache reuse depends on input/dependency hashes
- [ ] Question rounds can pause and resume a requirements run
- [ ] Approved requirements produce a stable `ProjectSeed`
- [ ] Quick requirements reaches parity-quality revision/approval behavior

### Acceptance criteria

- [ ] Conformance: staged requirements draft happy path
- [ ] Conformance: cached resume skips reusable stages
- [ ] Conformance: question generation and answer application
- [ ] Conformance: quick requirements revision then approval
- [ ] Conformance: project seed produced from both full and quick draft modes

### Definition of done

- [ ] `requirements draft` covers old PRD depth sufficiently for replacement
- [ ] `requirements quick` is production-usable, not a reduced approximation
- [ ] Project handoff format is stable and documented

---

## Slice 2 — Bootstrap / auto parity

### Purpose
Restore the convenience of old `auto` / `quick-dev-auto` without resurrecting old CLI naming.

### Recommended CLI

- `ralph-burning project create --from-requirements <run-id>`
- `ralph-burning project bootstrap --idea "..." --flow <preset>`
- `ralph-burning project bootstrap --from-file <requirements-file> --flow <preset>`
- optional `--start`

### Behavior

Two supported modes:

1. **explicit handoff**
   - run requirements
   - create project from requirements

2. **convenience bootstrap**
   - run quick requirements
   - create/select project
   - optionally start run

### Expected code areas

- `src/cli/project.rs`
- `src/contexts/requirements_drafting/*`
- `src/contexts/project_run_record/*`
- `src/contexts/workspace_governance/*`

### Checklist

- [ ] Stable `ProjectSeed` ingestion path exists in project creation
- [ ] `project create --from-requirements` works end to end
- [ ] `project bootstrap` can run quick requirements + create project
- [ ] `project bootstrap --start` enters run execution
- [ ] Partial bootstrap failures do not leave half-created projects

### Acceptance criteria

- [ ] Conformance: create project from requirements output
- [ ] Conformance: bootstrap standard project
- [ ] Conformance: bootstrap quick_dev project
- [ ] Conformance: bootstrap with `--start`
- [ ] Conformance: bootstrap failure leaves no partial project state

### Definition of done

- [ ] Users no longer need to manually stitch requirements and project creation unless they want to
- [ ] New bootstrap flow matches old convenience value without reintroducing old command names

---

## Slice 3 — Manual amendment parity

### Purpose
Restore explicit operator amendment intake, not only daemon/PR-review-driven amendments.

### Recommended CLI

- `ralph-burning project amend add --text "..." [--priority high|normal|low]`
- `ralph-burning project amend add --file ./amendment.md`
- `ralph-burning project amend list`
- `ralph-burning project amend remove <id>`
- `ralph-burning project amend clear`

### Expected code areas

- `src/cli/project.rs` or new `src/cli/amend.rs`
- `src/contexts/project_run_record/*`
- `src/contexts/workflow_composition/*`

### Checklist

- [ ] Manual amendment creation exists
- [ ] Manual amendment file ingestion exists
- [ ] Amendments can be listed and removed
- [ ] Amendments carry source metadata (`manual`, `pr_review`, `issue_command`, etc.)
- [ ] Dedup rules are defined and enforced
- [ ] Pending amendments participate in completion/final-review gating
- [ ] Manual amendment ingestion is visible in durable history

### Acceptance criteria

- [ ] Conformance: manual amendment add/list/remove
- [ ] Conformance: amendment persists across restart
- [ ] Conformance: pending amendment blocks completion
- [ ] Conformance: completed project reopens when a manual amendment is added

### Definition of done

- [ ] Operators can inject new work without using GitHub/PR review flows
- [ ] Amendment behavior is consistent across manual and automated sources

---

## Slice 4 — Operator UX parity

### Purpose
Bring `status`, `history`, `tail`, and rollback ergonomics to parity with old daily operator needs.

### Recommended CLI additions

- `ralph-burning run status --json`
- `ralph-burning run history --verbose --json`
- `ralph-burning run tail --last 50 --follow --logs`
- `ralph-burning run rollback --list`
- `ralph-burning run show-payload <payload-id>`
- `ralph-burning run show-artifact <artifact-id>`

### Expected code areas

- `src/cli/run.rs`
- `src/contexts/project_run_record/queries.rs`
- `src/contexts/project_run_record/service.rs`

### Checklist

- [ ] `status --json` exists
- [ ] `history --verbose` exists
- [ ] `history --json` exists
- [ ] `tail --last <n>` exists
- [ ] `tail --follow` exists
- [ ] `tail --logs` exists
- [ ] `rollback --list` exists
- [ ] payload inspection exists
- [ ] artifact inspection exists
- [ ] history/tail filters are stage-aware

### Acceptance criteria

- [ ] Conformance: `status --json`
- [ ] Conformance: `history --verbose`
- [ ] Conformance: `tail --last`
- [ ] Conformance: `tail --follow`
- [ ] Conformance: payload/artifact inspection
- [ ] Conformance: rollback target discovery

### Definition of done

- [ ] Daily debugging and inspection do not require filesystem spelunking
- [ ] All core run state can be inspected from the CLI

---

## Slice 5 — Backend operations parity

### Purpose
Add the operational backend command surface that old `ralph` effectively had and `ralph-burning` still lacks.

### Recommended CLI

- `ralph-burning backend list`
- `ralph-burning backend check`
- `ralph-burning backend show-effective`
- `ralph-burning backend probe --role <role> --flow <flow> [--cycle <n>]`

### Expected code areas

- new `src/cli/backend.rs`
- `src/contexts/agent_execution/*`
- `src/contexts/workspace_governance/config.rs`

### Checklist

- [ ] `backend list` shows supported backends and enablement state
- [ ] `backend check` validates availability/readiness of required configured backends
- [ ] `backend show-effective` exposes resolved role/backend/model mapping
- [ ] `backend probe` can preview stage/panel resolution
- [ ] timeout and session policy are visible in backend diagnostics
- [ ] workspace/project/CLI precedence is reflected in output

### Acceptance criteria

- [ ] Conformance: `backend list`
- [ ] Conformance: `backend check`
- [ ] Conformance: `backend show-effective`
- [ ] Conformance: `backend probe` for completion panel
- [ ] Conformance: `backend probe` for final-review panel

### Definition of done

- [ ] Operators can understand backend resolution without reading config and source code together
- [ ] Broken backend setups are diagnosable before a run starts

---

## Slice 6 — Tmux and streaming parity

### Purpose
Restore optional tmux execution mode and live streaming behavior.

### Recommended config / CLI shape

Config:

```toml
[execution]
mode = "direct" | "tmux"
stream_output = true
```

Potential CLI:

- `ralph-burning run attach`
- `ralph-burning run tail --follow --logs`

### Expected code areas

- new `src/adapters/tmux.rs`
- `src/contexts/agent_execution/*`
- `src/cli/run.rs`
- config model

### Checklist

- [ ] Tmux adapter exists
- [ ] Direct vs tmux execution mode is configurable
- [ ] Live output can be streamed into runtime logs
- [ ] `run attach` or equivalent operator path exists
- [ ] Cancel/timeout cleans up tmux child work correctly
- [ ] Direct and tmux modes produce equivalent durable history

### Acceptance criteria

- [ ] Conformance: tmux-enabled run start
- [ ] Conformance: live tail sees runtime output
- [ ] Conformance: cancel in tmux mode cleans up session
- [ ] Conformance: direct vs tmux mode equivalent durable history

### Definition of done

- [ ] Tmux users can operate `ralph-burning` with parity to old workflow habits
- [ ] Streaming output is usable without damaging durability guarantees

---

## Slice 7 — Prompt/template override parity

### Purpose
Add workspace/project prompt template override support for workflow and requirements paths.

### Recommended override order

1. project template override
2. workspace template override
3. built-in default template

### Recommended layout

- `.ralph-burning/templates/<contract-or-stage>.md`
- `.ralph-burning/projects/<project-id>/templates/<contract-or-stage>.md`

### Expected code areas

- `src/contexts/workspace_governance/*`
- prompt/template adapters
- workflow prompt builders
- requirements prompt builders

### Checklist

- [ ] Template catalog API supports override resolution
- [ ] Workspace override directory is supported
- [ ] Project override directory is supported
- [ ] Project override beats workspace override
- [ ] Workflow prompt generation uses override resolution
- [ ] Requirements prompt generation uses override resolution
- [ ] Malformed overrides fail clearly and safely

### Acceptance criteria

- [ ] Conformance: workspace template override
- [ ] Conformance: project template override
- [ ] Conformance: project override beats workspace override
- [ ] Conformance: malformed override rejected

### Definition of done

- [ ] Prompt customization is possible without code changes
- [ ] Overrides do not weaken stage-contract safety

---

# 7. Recommended implementation order

Use this order:

1. **Slice 0** — P0 hardening/sign-off
2. **Slice 1** — full requirements / PRD parity
3. **Slice 2** — bootstrap / auto parity
4. **Slice 3** — manual amendments
5. **Slice 4** — operator UX parity
6. **Slice 5** — backend operations parity
7. **Slice 6** — tmux and streaming parity
8. **Slice 7** — prompt/template override parity

Rationale:

- hardening first because the current P0 surface should be trusted before expansion
- requirements before bootstrap because bootstrap depends on good seed quality
- amendments and operator UX before tmux because they matter more to daily parity
- template overrides last because they are valuable but do not unblock core product replacement

---

# 8. Suggested agent ownership

## Agent A — Parity hardening
Owns Slice 0.

**Primary files**
- `src/adapters/process_backend.rs`
- `src/contexts/agent_execution/service.rs`
- `src/contexts/workflow_composition/engine.rs`
- `src/adapters/github.rs`
- conformance additions

## Agent B — Requirements + bootstrap
Owns Slices 1 and 2.

**Primary files**
- `src/contexts/requirements_drafting/*`
- `src/cli/requirements.rs`
- `src/cli/project.rs`
- project-seed handoff

## Agent C — Amendments + operator UX
Owns Slices 3 and 4.

**Primary files**
- `src/contexts/project_run_record/*`
- `src/contexts/workflow_composition/*`
- `src/cli/project.rs`
- `src/cli/run.rs`

## Agent D — Backend ops + tmux/streaming
Owns Slices 5 and 6.

**Primary files**
- `src/contexts/agent_execution/*`
- new `src/cli/backend.rs`
- new `src/adapters/tmux.rs`
- runtime log streaming paths

## Agent E — Template overrides
Owns Slice 7.

**Primary files**
- `src/contexts/workspace_governance/*`
- prompt/template adapters
- workflow and requirements prompt builders

---

# 9. Stabilization points before parallel work

Before starting parallel implementation, stabilize these:

- [ ] CLI contract for new commands/options
- [ ] `ProjectSeed` format
- [ ] amendment source metadata shape
- [ ] operator JSON output DTOs
- [ ] backend diagnostics read-model shape
- [ ] execution mode config for tmux/streaming
- [ ] template override catalog API

---

# 10. Merge/conflict risk areas

These should have a single clear owner per slice:

- `src/shared/domain.rs`
- `src/contexts/workspace_governance/config.rs`
- `src/cli/project.rs`
- `src/cli/run.rs`
- `src/contexts/project_run_record/*`
- `src/contexts/agent_execution/*`

Rule:
- contract-first PRs land before downstream implementation PRs

---

# 11. Parity sign-off checklist

Use this before declaring old `ralph` replaceable.

## Hardening
- [ ] Slice 0 complete
- [ ] current P0 conformance is green
- [ ] no open correctness issues remain in backend/process/panel/GitHub basics

## Requirements
- [ ] full requirements draft has old-PRD-equivalent depth
- [ ] quick requirements is revision-capable and production-usable
- [ ] project seeds are stable

## Bootstrap
- [ ] project bootstrap exists
- [ ] project creation from requirements exists

## Amendments
- [ ] manual amendment CLI exists
- [ ] amendments from manual and PR-review sources behave consistently

## Operator UX
- [ ] status/history/tail JSON + verbose support exists
- [ ] rollback discovery is operator-friendly
- [ ] payload/artifact inspection exists

## Backend operations
- [ ] backend diagnostics CLI exists
- [ ] backend readiness can be checked outside a run

## Tmux / streaming
- [ ] tmux mode exists if still required for parity
- [ ] streaming/tail follow experience is usable

## Templates
- [ ] workspace/project template overrides exist

## Confidence
- [ ] conformance covers every above surface
- [ ] manual smoke test pass set exists for:
  - standard flow
  - quick_dev flow
  - docs_change flow
  - ci_improvement flow
  - requirements draft
  - bootstrap
  - daemon issue intake
  - PR draft/update/review flow

---

# 12. Manual smoke test matrix

These are recommended manual sign-off runs.

## Workflow matrix
- [ ] standard + Claude
- [ ] standard + Codex
- [ ] standard + OpenRouter
- [ ] quick_dev + Claude
- [ ] docs_change + configured docs validation
- [ ] ci_improvement + configured CI validation

## Requirements matrix
- [ ] full requirements draft
- [ ] quick requirements
- [ ] create project from requirements
- [ ] bootstrap and start run

## Daemon matrix
- [ ] single repo daemon routing by label
- [ ] single repo daemon routing by explicit command
- [ ] multi-repo daemon polling
- [ ] draft PR creation
- [ ] PR review ingestion into amendments
- [ ] rebase flow on changed default branch

---

# 13. Exit criteria

This parity plan is complete when:

- [ ] all slices marked done
- [ ] all required conformance cases exist and pass
- [ ] manual smoke matrix is green
- [ ] old `ralph` no longer provides any still-required feature missing in `ralph-burning`
- [ ] repo docs point users only at `ralph-burning`


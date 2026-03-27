#!/usr/bin/env bash
set -euo pipefail

# Bootstraps the Ralph milestone/bead integration backlog into `.beads/` using only `br`.
#
# Assumptions:
# - run from the root of the Ralph Burning repository (or another repo with `.beads/` already initialized)
# - `br` is installed and available on PATH
# - you want a fully linked, self-documenting phase-1 backlog plus a small deferred parallelism backlog
#
# This script intentionally uses only `br` commands for bead mutation:
# - br create
# - br update
# - br comments add
# - br dep add
# - br defer
# - br sync --flush-only
#
# Recommended usage:
#   BR_ACTOR=planner ./ralph_milestone_beads_bootstrap_v2.sh
#
# After running:
#   br ready
#   br epic status
#   br dep cycles
#   br sync --flush-only
#
# Then commit `.beads/` through git as normal.

ACTOR="${BR_ACTOR:-ralph-planner}"
BR=(br --actor "$ACTOR" --no-color)

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: required command '$1' is not on PATH" >&2
    exit 1
  }
}

capture() {
  local __var="$1"
  local __value
  __value="$(cat)"
  printf -v "$__var" '%s' "$__value"
}

create_issue() {
  local __out="$1"; shift
  local type="$1"; shift
  local priority="$1"; shift
  local parent="$1"; shift
  local labels="$1"; shift
  local title="$1"; shift
  local description="$1"; shift

  local id
  if [[ -n "$parent" ]]; then
    id=$("${BR[@]}" create --silent --title "$title" --type "$type" --priority "$priority" --parent "$parent" --description "$description")
  else
    id=$("${BR[@]}" create --silent --title "$title" --type "$type" --priority "$priority" --description "$description")
  fi

  if [[ -n "$labels" ]]; then
    "${BR[@]}" update "$id" --set-labels "$labels" >/dev/null
  fi

  printf -v "$__out" '%s' "$id"
}

add_comment() {
  local id="$1"; shift
  local body="$1"
  "${BR[@]}" comments add "$id" "$body" >/dev/null
}

depends_on() {
  local issue="$1"; shift
  local blocker="$1"
  "${BR[@]}" dep add "$issue" "$blocker" >/dev/null
}

defer_issue() {
  "${BR[@]}" defer "$@" >/dev/null
}

echo "==> validating br workspace"
require_cmd br
"${BR[@]}" where >/dev/null

echo "==> creating root epic, epics, tasks, and subtasks"
capture ROOT_DESC <<'EOF_ROOT_DESC'
Context:
Ralph already has a strong execution engine, a staged requirements system, durable project/run state,
and automation/worktree infrastructure. What it lacks is a larger planning-and-routing layer that can
reason about a whole body of work, decompose it into explicit beads, and then drive existing Ralph
execution one bead at a time.

Goal:
Adapt Ralph to a flywheel-style workflow while staying close to the current system. The milestone is
the new umbrella unit. One current Ralph project/run becomes the execution container for one bead
(user-facing term: task). The canonical graph lives in `.beads/` and is managed through br/bv.

Phase-1 intent:
- add a milestone layer above the current project/run substrate
- extend full requirements into a milestone planner
- export milestone work into beads/epics in `.beads/`
- select the next bead with bv/br
- execute each bead through a milestone-aware quick_dev loop
- reconcile review outcomes back into `.beads/`
- keep execution sequential for the first release

Guardrails:
- do not build a duplicate internal task graph for phase 1
- do not replace the quick_dev workflow shape
- do not make sprints the default execution abstraction
- do not force a global internal rename from project -> task before behavior is proven
- do not attempt parallel bead execution in the first release

Acceptance:
The end-to-end path should be: idea -> milestone plan -> beads -> next bead selection -> task run ->
milestone-aware review -> bead reconciliation -> next bead.
EOF_ROOT_DESC
create_issue ROOT epic 1 "" phase1,milestone,flywheel,ralph-burning,architecture 'Phase 1: milestone planning and bead-driven execution in Ralph Burning' "${ROOT_DESC}"
capture ROOT_COMMENT <<'EOF_ROOT_COMMENT'
Why this epic exists:
The point of this effort is not to bolt a new orchestration system beside Ralph. It is to give Ralph
a larger planning horizon while preserving the parts that already work: current requirements stages,
the quick_dev execution loop, durable run history, and the existing runtime/worktree substrate.

What future us should remember:
1. The milestone layer is the new abstraction; the current project/run is still the execution substrate.
2. br/bv are the source of truth for bead-space in phase 1.
3. Review must be whole-plan-aware so it does not keep asking for work that already belongs to later beads.
4. New bead creation must be intentionally conservative.
EOF_ROOT_COMMENT
add_comment "${ROOT}" "${ROOT_COMMENT}"

capture FND_DESC <<'EOF_FND_DESC'
Purpose:
Capture the shared language and architectural cut lines for the integration before implementation fans out.

This epic exists because the current Ralph codebase still uses `project` as its main durable execution unit,
while the new user-facing model introduces `milestone` as the umbrella body of work and `task` as the
execution of a single bead. Without explicit rules, teams will mix the terms and accidentally design the
wrong abstractions.

Deliverables:
- glossary for milestone / task / bead / epic / project
- phase-1 invariants and non-goals
- map of current Ralph modules to future responsibilities
- release acceptance checklist for the integrated behavior
EOF_FND_DESC
create_issue FND epic 1 "${ROOT}" phase1,architecture,naming,docs 'Define phase-1 vocabulary, invariants, and acceptance boundaries' "${FND_DESC}"
capture FND_COMMENT <<'EOF_FND_COMMENT'
This epic is intentionally meta and front-loaded. The goal is to prevent architectural drift while several
implementation tracks proceed in parallel.
EOF_FND_COMMENT
add_comment "${FND}" "${FND_COMMENT}"

capture FND1_DESC <<'EOF_FND1_DESC'
Goal:
Write the canonical terminology for the new system.

Must define:
- milestone = umbrella unit containing one plan and many beads
- bead = canonical work item in `.beads/`
- task = one Ralph-managed execution of one bead
- epic = grouped bead in `.beads/`
- project = legacy/internal execution substrate retained during transition

Acceptance:
- user-facing docs and CLI language prefer milestone/task terminology
- internal compatibility rule explains when `project` still appears
- docs make clear that one current Ralph project ~= one task execution container
EOF_FND1_DESC
create_issue FND1 meta 1 "${FND}" phase1,architecture,naming,docs 'Publish the glossary and naming rules for milestone, task, bead, epic, and project' "${FND1_DESC}"
capture FND1_COMMENT <<'EOF_FND1_COMMENT'
Naming is not cosmetic here. If we fail to separate milestone from task, we will either overgrow the old
project abstraction or accidentally create a second execution substrate.
EOF_FND1_COMMENT
add_comment "${FND1}" "${FND1_COMMENT}"

capture FND2_DESC <<'EOF_FND2_DESC'
Goal:
Record the rules that keep the first implementation small, coherent, and compatible with current Ralph.

Must include:
- use br/bv directly; do not build a duplicate internal task graph in phase 1
- reuse quick_dev shape instead of inventing a new bead workflow
- keep execution sequential initially
- treat review as milestone-aware, not sprint-oriented
- create new beads only when fix-now and planned-elsewhere are both wrong
- keep current project/run substrate internally until the new naming is proven

Acceptance:
- a short design note exists and is referenced by major implementation epics
- guardrails are reflected in milestone planner, review logic, and controller code
EOF_FND2_DESC
create_issue FND2 meta 1 "${FND}" phase1,architecture,guardrails 'Write the phase-1 architecture guardrails and explicit non-goals' "${FND2_DESC}"
capture FND2_COMMENT <<'EOF_FND2_COMMENT'
This issue is the brake pedal. When implementation gets exciting, this is the issue that reminds us what
*not* to build yet.
EOF_FND2_COMMENT
add_comment "${FND2}" "${FND2_COMMENT}"

capture FND3_DESC <<'EOF_FND3_DESC'
Goal:
Produce a code-aware responsibility map so implementation tasks stay grounded in the current repo.

Expected mappings:
- `requirements_drafting` -> milestone planner
- `project_run_record` -> task execution substrate with milestone/bead linkage
- `workflow_composition` -> milestone-aware quick_dev execution/review
- `automation_runtime` -> future home for parallel milestone dispatch, but not replaced in phase 1
- new `milestone_*` context -> milestone storage/state/orchestration
- `cli/*` -> milestone/task command surface and compatibility shims

Acceptance:
- the map points at concrete directories/files under `src/contexts/*`, `src/cli/*`, and docs
- every major implementation epic references the owning modules
EOF_FND3_DESC
create_issue FND3 meta 1 "${FND}" phase1,architecture,codebase-mapping 'Map current Ralph modules to their milestone-era responsibilities' "${FND3_DESC}"
capture FND3_COMMENT <<'EOF_FND3_COMMENT'
This is the issue that keeps the plan Ralph-native instead of drifting into an abstract greenfield design.
EOF_FND3_COMMENT
add_comment "${FND3}" "${FND3_COMMENT}"

capture FND4_DESC <<'EOF_FND4_DESC'
Goal:
Turn the integration goal into a concrete checklist that can drive conformance, manual smoke, and release decisions.

Must cover:
- milestone planning success
- bead graph creation and sync behavior
- next bead selection
- task creation from bead context
- milestone-aware review classifications
- conservative new bead creation
- successful bead close and advancement to the next bead

Acceptance:
- checklist is specific enough to drive tests and signoff
- checklist clearly distinguishes phase-1 done from later parallelism work
EOF_FND4_DESC
create_issue FND4 meta 2 "${FND}" phase1,acceptance,release 'Write the phase-1 acceptance criteria and cutover checklist' "${FND4_DESC}"

capture MS_DESC <<'EOF_MS_DESC'
Purpose:
Introduce the new umbrella unit above the existing project/run substrate.

The milestone is the durable holder of:
- the original request
- the refined plan
- milestone-level status
- bead/task lineage
- milestone journal/history

This epic should create the storage/service/query layer that lets the rest of the system talk about a
larger body of work without breaking the current project-run model.
EOF_MS_DESC
create_issue MS epic 1 "${ROOT}" phase1,milestone,storage,runtime 'Add milestone domain model, storage, and status tracking' "${MS_DESC}"
capture MS_COMMENT <<'EOF_MS_COMMENT'
Phase-1 note:
Keep the milestone store beside existing `.ralph-burning/projects/` data. Do not try to migrate the
project substrate out from under the current system.
EOF_MS_COMMENT
add_comment "${MS}" "${MS_COMMENT}"

capture MS1_DESC <<'EOF_MS1_DESC'
Goal:
Define the canonical on-disk shape for milestone state.

Expected artifacts:
- `milestone.toml`
- `plan.md`
- `plan.json`
- `status.json`
- `journal.ndjson`
- `task-runs/` or equivalent lineage mapping

Acceptance:
- schema is versioned and explicit about milestone identity, status, plan hash/version, active bead, and progress
- storage sits cleanly beside existing project storage
- layout is simple enough to inspect and debug manually
EOF_MS1_DESC
create_issue MS1 task 1 "${MS}" phase1,milestone,storage,design 'Define the milestone record schema and filesystem layout under .ralph-burning/milestones' "${MS1_DESC}"

capture MS2_DESC <<'EOF_MS2_DESC'
Goal:
Build the code path that persists and mutates milestone state.

Scope:
- load/save milestone records
- append milestone journal events
- read/write plan artifacts
- maintain task/bead linkage records
- keep writes atomic enough to match existing Ralph durability expectations

Likely target areas:
- new `src/contexts/milestone_*`
- adapters/fs integration
- CLI-facing service methods

Acceptance:
- milestone state can be created and updated without touching `.beads/` yet
- service layer exposes the minimum operations needed by planner, controller, and CLI
EOF_MS2_DESC
create_issue MS2 feature 1 "${MS}" phase1,milestone,storage,service 'Implement milestone store and service layer for create/load/update/journal operations' "${MS2_DESC}"
capture MS2_COMMENT <<'EOF_MS2_COMMENT'
Keep this domain boring and durable. The controller and planner should depend on it, not reimplement it.
EOF_MS2_COMMENT
add_comment "${MS2}" "${MS2_COMMENT}"

capture MS3_DESC <<'EOF_MS3_DESC'
Goal:
Compute milestone progress in a way that is useful for both CLI output and controller logic.

Must answer:
- how many beads exist
- how many are open/in progress/done/blocked
- which bead is active
- which Ralph task/run currently owns that bead
- whether the milestone is complete, blocked, or awaiting operator input

Acceptance:
- query surfaces exist for `milestone show` / `milestone status`
- progress is derived from milestone state plus `.beads/` reads, not chat memory
EOF_MS3_DESC
create_issue MS3 task 2 "${MS}" phase1,milestone,status,queries 'Add milestone status aggregation and progress queries' "${MS3_DESC}"

capture MS4_DESC <<'EOF_MS4_DESC'
Goal:
Record which Ralph task/project/run attempted which bead, with enough detail to audit retries and outcomes later.

Must capture:
- milestone_id
- bead_id
- current task/project id
- run id if available
- outcome summary
- prompt version or plan hash if relevant

Acceptance:
- milestone state can answer 'what happened to bead X?' without reconstructing from unrelated artifacts
- controller can update linkage after each run outcome
EOF_MS4_DESC
create_issue MS4 task 1 "${MS}" phase1,milestone,lineage,task-substrate 'Track bead-to-task run linkage inside milestone state' "${MS4_DESC}"

capture MS5_DESC <<'EOF_MS5_DESC'
Goal:
Define milestone lifecycle states and the events that move between them.

Suggested states:
- planning
- ready
- running
- blocked
- completed
- failed / needs_operator

Acceptance:
- state transitions are explicit and validated
- journal events provide enough detail for debugging and operator recovery
- controller logic can rely on these states instead of inferring status indirectly
EOF_MS5_DESC
create_issue MS5 task 2 "${MS}" phase1,milestone,journal,lifecycle 'Add milestone lifecycle transitions and journal events' "${MS5_DESC}"

capture PLN_DESC <<'EOF_PLN_DESC'
Purpose:
Turn the current full requirements pipeline into the source of truth for milestone-scale planning.

Instead of ending only in a single project seed, the full pipeline should produce a versioned milestone bundle
containing the umbrella plan, acceptance framing, workstreams, and bead-generation instructions.

This is where the large-context reasoning belongs. The implementation loop should consume the output, not re-do it.
EOF_PLN_DESC
create_issue PLN epic 1 "${ROOT}" phase1,requirements,planning,milestone 'Extend requirements_drafting into a milestone planner' "${PLN_DESC}"
capture PLN_COMMENT <<'EOF_PLN_COMMENT'
Planner principle:
do the hard reasoning once in plan space while the whole milestone still fits in context. The rest of the system
should operate on the resulting explicit structure.
EOF_PLN_COMMENT
add_comment "${PLN}" "${PLN_COMMENT}"

capture PLN1_DESC <<'EOF_PLN1_DESC'
Goal:
Define the machine-readable and human-readable output of milestone planning.

Bundle should include:
- milestone identity/name
- executive summary
- goals and non-goals
- constraints and assumptions
- acceptance map
- workstreams/epics
- bead proposals and dependency hints
- recommended default flow for bead execution
- generated AGENTS guidance seed material

Acceptance:
- schema/versioning strategy is clear
- renderers can emit deterministic `plan.md` and `plan.json`
- downstream bead export has a stable input contract
EOF_PLN1_DESC
create_issue PLN1 task 1 "${PLN}" phase1,requirements,planning,contracts 'Define a versioned MilestoneBundle contract and renderer set' "${PLN1_DESC}"

capture PLN2_DESC <<'EOF_PLN2_DESC'
Goal:
Wire milestone planning into the existing full-mode stage machine.

Scope:
- decide whether milestone output extends `project_seed` or introduces a new final stage
- persist milestone-oriented committed stage output
- preserve current caching, rollback, and atomic stage commit behavior
- ensure question rounds still invalidate only the correct downstream artifacts

Likely target areas:
- `src/contexts/requirements_drafting/model.rs`
- `src/contexts/requirements_drafting/service.rs`
- `src/contexts/requirements_drafting/contracts.rs`
- `src/contexts/requirements_drafting/renderers.rs`

Acceptance:
- a full requirements run can end in a milestone bundle
- stage reuse and invalidation semantics remain coherent
- the change does not quietly break existing quick mode behavior
EOF_PLN2_DESC
create_issue PLN2 feature 1 "${PLN}" phase1,requirements,planning,pipeline 'Extend the full requirements pipeline so it can emit a MilestoneBundle' "${PLN2_DESC}"
capture PLN2_COMMENT <<'EOF_PLN2_COMMENT'
This is the beating heart of the integration. If this output is weak or unstable, the rest of the system
will compensate badly and drift back into ad-hoc chat planning.
EOF_PLN2_COMMENT
add_comment "${PLN2}" "${PLN2_COMMENT}"

capture PLN2A_DESC <<'EOF_PLN2A_DESC'
Goal:
Add the new output type to the requirements run state and committed stage bookkeeping.

Acceptance:
- run state can identify milestone-oriented completion
- payload/artifact ids for milestone output are stored consistently
- backward compatibility for older seed-oriented runs is preserved where required
EOF_PLN2A_DESC
create_issue PLN2A task 1 "${PLN2}" phase1,requirements,planning,pipeline,subtask 'Wire MilestoneBundle through the full-mode stage machine and run state' "${PLN2A_DESC}"

capture PLN2B_DESC <<'EOF_PLN2B_DESC'
Goal:
Ensure that answer-driven invalidation still removes the correct downstream stages and nothing more.

Acceptance:
- changing answers invalidates milestone outputs that depend on synthesis onward
- ideation/research reuse remains valid where intended
- tests prove this behavior
EOF_PLN2B_DESC
create_issue PLN2B task 1 "${PLN2}" phase1,requirements,planning,invalidation,subtask 'Preserve question-round invalidation semantics when milestone outputs change' "${PLN2B_DESC}"

capture PLN2C_DESC <<'EOF_PLN2C_DESC'
Goal:
Make sure operators can tell what kind of run they are looking at and what the final output is.

Acceptance:
- requirements show indicates milestone-oriented completion clearly
- outputs remain readable for both old and new flows
EOF_PLN2C_DESC
create_issue PLN2C task 2 "${PLN2}" phase1,requirements,cli,subtask 'Keep requirements show/status surfaces intelligible with milestone-oriented runs' "${PLN2C_DESC}"

capture PLN3_DESC <<'EOF_PLN3_DESC'
Goal:
Produce durable plan artifacts that humans and downstream code can both consume.

Deliverables:
- `plan.md` for human review
- `plan.json` for machine use
- explicit acceptance map linked to workstreams/beads

Acceptance:
- artifacts are deterministic for the same inputs
- they carry enough structure to drive bead creation and review scope boundaries
EOF_PLN3_DESC
create_issue PLN3 task 1 "${PLN}" phase1,requirements,planning,artifacts 'Render plan.md, plan.json, and an explicit acceptance map from the milestone planner' "${PLN3_DESC}"

capture PLN4_DESC <<'EOF_PLN4_DESC'
Goal:
Make the planner output explicit enough that bead creation is mostly translation, not another free-form planning pass.

Must include:
- milestone root epic proposal
- child epic/workstream proposals
- bead specs with rationale, scope, and acceptance criteria
- dependency hints suitable for br dep edges
- notes about what is intentionally deferred to later beads

Acceptance:
- the output is strong enough for a deterministic first export into `.beads/`
- review can later recognize 'planned elsewhere' because the planner named that work explicitly
EOF_PLN4_DESC
create_issue PLN4 task 1 "${PLN}" phase1,requirements,planning,decomposition 'Generate workstreams, epics, bead specs, and dependency hints from the planning output' "${PLN4_DESC}"

capture PLN5_DESC <<'EOF_PLN5_DESC'
Goal:
Once a milestone plan is approved/generated, create the milestone record without manual copy/paste.

Acceptance:
- completed planner output can materialize a milestone entry
- milestone state stores references to the plan artifacts and bundle version
- operators do not have to hand-wire milestone metadata after planning
EOF_PLN5_DESC
create_issue PLN5 task 1 "${PLN}" phase1,requirements,milestone,handoff 'Create a milestone record automatically from completed planner output' "${PLN5_DESC}"

capture PLN6_DESC <<'EOF_PLN6_DESC'
Goal:
Introduce milestone planning without breaking today's single-project flows.

Must preserve:
- quick requirements path
- project create from requirements seed
- project bootstrap behavior
- existing docs and tests where the legacy path remains valid

Acceptance:
- the new milestone planner is additive or carefully gated
- legacy project-seed paths still work until explicitly replaced
EOF_PLN6_DESC
create_issue PLN6 task 1 "${PLN}" phase1,requirements,compatibility,bootstrap 'Preserve current quick requirements and project-seed bootstrap behavior during rollout' "${PLN6_DESC}"

capture PLN7_DESC <<'EOF_PLN7_DESC'
Goal:
Prove that the planner remains safe under the same operational conditions as the current requirements system.

Must test:
- cache reuse
- downstream invalidation
- question round resume
- milestone bundle creation
- legacy compatibility where still supported

Acceptance:
- test coverage exists at model/service level and at least one CLI-visible path
EOF_PLN7_DESC
create_issue PLN7 task 1 "${PLN}" phase1,requirements,tests 'Add requirements tests for cache reuse, invalidation, and milestone bundle handoff' "${PLN7_DESC}"

capture BRV_DESC <<'EOF_BRV_DESC'
Purpose:
Make `.beads/` the canonical work graph for the new milestone workflow.

Ralph should not invent a second internal graph in phase 1. Instead it needs reliable adapters around:
- br for create/update/comment/dependencies/claim/close/sync
- bv for next-bead recommendations and graph-aware analysis

This epic owns that integration boundary.
EOF_BRV_DESC
create_issue BRV epic 1 "${ROOT}" phase1,beads,br-bv,graph 'Add br/bv integration for the canonical bead graph' "${BRV_DESC}"
capture BRV_COMMENT <<'EOF_BRV_COMMENT'
Rule of thumb:
if the state belongs to bead-space, prefer putting it in `.beads/` and talking to it through br/bv instead of
creating a competing Ralph-only representation.
EOF_BRV_COMMENT
add_comment "${BRV}" "${BRV_COMMENT}"

capture BRV1_DESC <<'EOF_BRV1_DESC'
Goal:
Wrap br command execution behind a typed boundary Ralph can trust.

Needed capabilities:
- create epic/task issues
- update fields and labels
- add comments
- add/remove dependencies
- claim and close beads
- sync/export/import status checks
- parse machine-readable outputs where available

Acceptance:
- adapter surfaces typed success/error results
- command failures are translated into actionable app errors
- controller and planner code do not manually shell out all over the codebase
EOF_BRV1_DESC
create_issue BRV1 feature 1 "${BRV}" phase1,beads,br,adapter 'Implement a br adapter for create/update/comments/dependencies/claim/close/sync operations' "${BRV1_DESC}"
capture BRV1_COMMENT <<'EOF_BRV1_COMMENT'
The main thing to remember is that br is intentionally non-invasive. Export/import/sync behavior is part of the
functional contract here, not an afterthought.
EOF_BRV1_COMMENT
add_comment "${BRV1}" "${BRV1_COMMENT}"

capture BRV1A_DESC <<'EOF_BRV1A_DESC'
Goal:
Centralize shell execution, exit-code handling, stdout/stderr capture, and error classification for br commands.

Acceptance:
- missing binary, command failure, malformed output, and workspace misuse are distinct error classes
- callers do not have to parse raw process failures themselves
EOF_BRV1A_DESC
create_issue BRV1A task 1 "${BRV1}" phase1,beads,br,adapter,subtask 'Add process execution wrapper and structured command errors for br' "${BRV1A_DESC}"

capture BRV1B_DESC <<'EOF_BRV1B_DESC'
Goal:
Convert br output into typed structures Ralph can use for planner/controller decisions.

Acceptance:
- ready list, show detail, dependency tree/list, and status surfaces are normalized
- parsing code is isolated and testable
EOF_BRV1B_DESC
create_issue BRV1B task 1 "${BRV1}" phase1,beads,br,adapter,parsing,subtask 'Parse and normalize br JSON output for ready/show/list/dep operations' "${BRV1B_DESC}"

capture BRV1C_DESC <<'EOF_BRV1C_DESC'
Goal:
Handle create/update/comment/close/reopen/dependency mutations and follow them with the correct sync discipline.

Acceptance:
- mutation helpers can optionally force `br sync --flush-only` at the right moments
- controller and planner can trace what changed and why
EOF_BRV1C_DESC
create_issue BRV1C task 1 "${BRV1}" phase1,beads,br,adapter,sync,subtask 'Support br mutation flows with audit-friendly logging and explicit sync calls' "${BRV1C_DESC}"

capture BRV2_DESC <<'EOF_BRV2_DESC'
Goal:
Give Ralph a typed way to ask bv for graph-aware recommendations.

Needed capabilities:
- next bead selection
- triage summaries
- related-work lookups
- impact/file-overlap checks where useful
- future support for capacity/parallelization analysis

Acceptance:
- adapter can read the few bv commands phase 1 actually needs
- output is normalized enough for controller and prompt generator use
EOF_BRV2_DESC
create_issue BRV2 feature 1 "${BRV}" phase1,beads,bv,adapter 'Implement a bv adapter for next-bead, triage, related-work, and impact analysis' "${BRV2_DESC}"

capture BRV3_DESC <<'EOF_BRV3_DESC'
Goal:
Translate milestone planning output into actual beads managed by br.

Scope:
- create a milestone root epic
- create child epics/workstreams
- create task beads
- add dependency edges
- attach planning rationale/comments where useful
- sync the resulting `.beads/` state

Acceptance:
- a milestone plan can be exported deterministically into `.beads/`
- the initial graph is rich enough for bv/br routing without hand-editing
EOF_BRV3_DESC
create_issue BRV3 feature 1 "${BRV}" phase1,beads,planning,export 'Materialize a MilestoneBundle into a root epic, epics, beads, and dependencies in .beads' "${BRV3_DESC}"

capture BRV4_DESC <<'EOF_BRV4_DESC'
Goal:
Make sure Ralph handles br's non-invasive model safely.

Must cover:
- when to `sync --flush-only`
- when to `sync --import-only`
- how to react to stale DB / malformed JSONL / conflict markers
- what operator recovery looks like after git pull / merge / failed run

Acceptance:
- the rules are encoded in code paths where possible
- docs and recovery guidance exist for the rest
EOF_BRV4_DESC
create_issue BRV4 task 1 "${BRV}" phase1,beads,br,sync,ops 'Codify sync/import safety rules for non-invasive br operation' "${BRV4_DESC}"

capture BRV5_DESC <<'EOF_BRV5_DESC'
Goal:
Give prompt generation, controller logic, and CLI read commands a common view of bead-space.

Should answer:
- bead detail
- parent epic
- dependency/blocker status
- ready list
- related/planned-elsewhere candidate beads

Acceptance:
- callers can ask for read models instead of shelling out ad hoc
- data is rich enough to support milestone-aware prompt construction
EOF_BRV5_DESC
create_issue BRV5 task 2 "${BRV}" phase1,beads,queries,graph 'Add read/query models for bead details, blocker state, and ready work' "${BRV5_DESC}"

capture BRV6_DESC <<'EOF_BRV6_DESC'
Goal:
Make the br/bv integration operationally trustworthy before the controller depends on it.

Acceptance:
- missing binary paths are handled cleanly
- malformed JSON / unexpected output is surfaced with actionable context
- sync failure paths are covered in tests
EOF_BRV6_DESC
create_issue BRV6 task 1 "${BRV}" phase1,beads,tests,adapter 'Add adapter tests and error handling for missing tools, malformed output, and sync failures' "${BRV6_DESC}"

capture TSK_DESC <<'EOF_TSK_DESC'
Purpose:
Reuse the current Ralph project/run model as the substrate for one bead execution, while introducing task and
milestone concepts above it.

This epic is where current `project` semantics get linked to:
- milestone_id
- bead_id
- plan/prompt versioning
- task-facing UX wrappers
EOF_TSK_DESC
create_issue TSK epic 1 "${ROOT}" phase1,task-substrate,project-run,migration 'Bridge the current project/run substrate into milestone-scoped task execution' "${TSK_DESC}"
capture TSK_COMMENT <<'EOF_TSK_COMMENT'
This is the compatibility epic. We want the new behavior without forcing a risky global rename first.
EOF_TSK_COMMENT
add_comment "${TSK}" "${TSK_COMMENT}"

capture TSK1_DESC <<'EOF_TSK1_DESC'
Goal:
Extend the current project/run metadata model so a run can be understood as the implementation of a bead.

Expected metadata:
- milestone_id
- bead_id
- parent epic or root epic when useful
- source = milestone
- task mode / execution mode
- prompt or plan version info where needed

Acceptance:
- lineage can be recovered from durable state
- existing project/run semantics remain valid for non-milestone tasks
EOF_TSK1_DESC
create_issue TSK1 task 1 "${TSK}" phase1,task-substrate,metadata,milestone 'Define task-mode metadata linking current project records to milestone_id and bead_id' "${TSK1_DESC}"

capture TSK2_DESC <<'EOF_TSK2_DESC'
Goal:
Create one Ralph execution unit directly from selected bead context.

Scope:
- take milestone + bead input
- generate or accept a bead-backed prompt
- create the current project substrate
- set initial task/milestone metadata
- keep the result compatible with normal `run start`

Acceptance:
- controller can create a task/project for a selected bead without manual setup
- created task is durable and inspectable through existing run machinery
EOF_TSK2_DESC
create_issue TSK2 feature 1 "${TSK}" phase1,task-substrate,creation,prompt 'Add a task creation path that bootstraps a project/run from a bead-backed prompt' "${TSK2_DESC}"

capture TSK3_DESC <<'EOF_TSK3_DESC'
Goal:
Persist enough provenance to tell whether a task was run against the latest milestone plan/prompt.

Acceptance:
- run/task state stores prompt hash or plan hash
- controller and CLI can detect obvious drift between current milestone plan and historical task context
EOF_TSK3_DESC
create_issue TSK3 task 2 "${TSK}" phase1,task-substrate,metadata,drift 'Track prompt version, plan hash, and source metadata on bead-backed task runs' "${TSK3_DESC}"

capture TSK4_DESC <<'EOF_TSK4_DESC'
Goal:
Make it easy to inspect which task/run belongs to which bead.

Acceptance:
- run/task detail can display milestone/bead linkage
- history views are useful enough for debugging retries and review outcomes
EOF_TSK4_DESC
create_issue TSK4 task 2 "${TSK}" phase1,task-substrate,queries,history 'Expose milestone and bead lineage in run history and task detail queries' "${TSK4_DESC}"

capture TSK5_DESC <<'EOF_TSK5_DESC'
Goal:
Introduce task terminology to users without ripping out existing project code.

Scope:
- add CLI aliases/wrappers where appropriate
- keep internal storage/services on the project substrate in phase 1
- document the mapping clearly in output/help

Acceptance:
- user-facing task commands exist or the pathway to them is explicit
- backward compatibility for current project commands is preserved
EOF_TSK5_DESC
create_issue TSK5 feature 2 "${TSK}" phase1,task-substrate,cli,naming 'Add task-facing aliases while keeping the internal project substrate intact' "${TSK5_DESC}"

capture PRT_DESC <<'EOF_PRT_DESC'
Purpose:
Feed the current workflow engine better context instead of replacing it.

Each bead-backed task prompt needs:
- milestone summary
- current bead scope
- dependencies and parent epic
- acceptance criteria
- already-planned-elsewhere context
- repo operating guidance / AGENTS rules

This is the main seam that makes quick_dev whole-plan-aware.
EOF_PRT_DESC
create_issue PRT epic 1 "${ROOT}" phase1,prompts,agents,workflow 'Generate milestone-aware prompts and AGENTS guidance for each bead task' "${PRT_DESC}"
capture PRT_COMMENT <<'EOF_PRT_COMMENT'
Important constraint:
include enough whole-plan context to prevent false review feedback, but not so much that the active task prompt
becomes bloated and unfocused.
EOF_PRT_COMMENT
add_comment "${PRT}" "${PRT_COMMENT}"

capture PRT1_DESC <<'EOF_PRT1_DESC'
Goal:
Define the canonical input shape for a milestone-aware bead execution prompt.

Should include sections for:
- milestone summary
- current bead details
- must-do scope
- explicit non-goals
- acceptance criteria
- already planned elsewhere
- review policy
- AGENTS/repo guidance

Acceptance:
- prompt generator and workflow stages share one clear contract
- contract is stable enough to hash/version for drift detection
EOF_PRT1_DESC
create_issue PRT1 task 1 "${PRT}" phase1,prompts,contracts 'Define the task prompt contract for bead execution' "${PRT1_DESC}"

capture PRT2_DESC <<'EOF_PRT2_DESC'
Goal:
Build the actual task prompt generator for bead-backed execution.

Acceptance:
- prompt.md is generated deterministically
- the current bead is clearly scoped
- nearby/future bead references explain what should *not* be absorbed into the active task
- output works with the existing workflow engine's prompt consumption model
EOF_PRT2_DESC
create_issue PRT2 feature 1 "${PRT}" phase1,prompts,generation,beads 'Generate prompt.md from milestone summary, bead scope, dependencies, and planned-elsewhere context' "${PRT2_DESC}"

capture PRT3_DESC <<'EOF_PRT3_DESC'
Goal:
Produce durable operating guidance that survives long runs and resumptions.

Must cover:
- repo conventions
- milestone priorities
- bead workflow rules
- review policy for fix-now vs planned-elsewhere vs new-bead
- validation expectations

Acceptance:
- milestone planning can generate AGENTS guidance
- Ralph has a clear strategy for inserting/updating a managed section in repo AGENTS.md
EOF_PRT3_DESC
create_issue PRT3 feature 2 "${PRT}" phase1,agents,docs,prompts 'Generate milestone-scoped AGENTS guidance and define how Ralph updates repo AGENTS.md' "${PRT3_DESC}"

capture PRT4_DESC <<'EOF_PRT4_DESC'
Goal:
Decide how much graph context belongs in the active task prompt and how it should be summarized.

Acceptance:
- prompt includes enough neighboring context to explain 'planned elsewhere'
- prompt does not dump the entire milestone graph or swamp the active implementation task
- heuristics are deterministic and testable
EOF_PRT4_DESC
create_issue PRT4 task 2 "${PRT}" phase1,prompts,context,scope-control 'Inject nearby and future bead context without overwhelming the working prompt' "${PRT4_DESC}"

capture PRT5_DESC <<'EOF_PRT5_DESC'
Goal:
Prove that prompt generation is stable and suitable for drift detection and reproducible runs.

Acceptance:
- prompts hash deterministically for identical inputs
- rendered content includes the required milestone/bead sections
- tests cover the planned-elsewhere section and AGENTS inclusion
EOF_PRT5_DESC
create_issue PRT5 task 1 "${PRT}" phase1,prompts,tests 'Add tests for prompt determinism, hashing, and milestone-aware rendering' "${PRT5_DESC}"

capture QDV_DESC <<'EOF_QDV_DESC'
Purpose:
Keep the current quick_dev sequence:
1. plan_and_implement
2. review
3. apply_fixes
4. final_review

But make each stage understand the whole milestone well enough to avoid repeatedly suggesting work that is
already planned for later beads.

This epic owns the behavioral change, not a new workflow shape.
EOF_QDV_DESC
create_issue QDV epic 1 "${ROOT}" phase1,workflow,quick_dev,review 'Make quick_dev milestone-aware without replacing its shape' "${QDV_DESC}"
capture QDV_COMMENT <<'EOF_QDV_COMMENT'
The core review distinction we need is:
- fix now
- already planned elsewhere
- genuinely missing work

That distinction is what makes bead-by-bead execution viable without collapsing back into 'do the whole project now'.
EOF_QDV_COMMENT
add_comment "${QDV}" "${QDV_COMMENT}"

capture QDV1_DESC <<'EOF_QDV1_DESC'
Goal:
Make the first stage of quick_dev operate as the implementation of one bead, not the whole milestone.

Acceptance:
- stage prompt sees milestone summary, current bead, and out-of-scope notes
- implementation output is shaped by bead acceptance criteria, not global ambition
EOF_QDV1_DESC
create_issue QDV1 task 1 "${QDV}" phase1,workflow,quick_dev,planning 'Teach plan_and_implement to honor bead scope, non-goals, and milestone context' "${QDV1_DESC}"

capture QDV2_DESC <<'EOF_QDV2_DESC'
Goal:
Enrich the structured outputs so review findings can be routed correctly.

Needed classes:
- fix_current_bead
- covered_by_existing_bead
- propose_new_bead
- informational_only

Acceptance:
- schemas/contracts support the classification
- domain validation enforces meaningful payloads
- downstream stages can consume the result without guesswork
EOF_QDV2_DESC
create_issue QDV2 feature 1 "${QDV}" phase1,workflow,review,contracts 'Extend review and final-review contracts with fix-now / planned-elsewhere / new-bead classifications' "${QDV2_DESC}"
capture QDV2_COMMENT <<'EOF_QDV2_COMMENT'
This issue is central because it bridges the old task-local review world and the new milestone-aware routing world.
EOF_QDV2_COMMENT
add_comment "${QDV2}" "${QDV2_COMMENT}"

capture QDV2A_DESC <<'EOF_QDV2A_DESC'
Goal:
Put the new classification categories into the structured contracts Ralph validates.

Acceptance:
- schemas are explicit
- domain validation prevents nonsensical combinations
- tests cover common and edge cases
EOF_QDV2A_DESC
create_issue QDV2A task 1 "${QDV2}" phase1,workflow,review,contracts,subtask 'Define the new review classification schema and domain validation rules' "${QDV2A_DESC}"

capture QDV2B_DESC <<'EOF_QDV2B_DESC'
Goal:
Teach the reviewers what the new categories mean and when to use them.

Acceptance:
- prompts define the routing choices clearly
- reviewers are told to be conservative about proposing new beads
EOF_QDV2B_DESC
create_issue QDV2B task 1 "${QDV2}" phase1,workflow,review,prompts,subtask 'Render classification guidance into review and final_review prompts' "${QDV2B_DESC}"

capture QDV3_DESC <<'EOF_QDV3_DESC'
Goal:
Use milestone context to keep the review loop scoped to the current bead.

Acceptance:
- review can explicitly mark issues as already planned elsewhere
- such findings do not bounce the current task back into fix loops
- milestone/bead identifiers can be attached when mapping to existing work
EOF_QDV3_DESC
create_issue QDV3 feature 1 "${QDV}" phase1,workflow,review,planned-elsewhere 'Update the review stage to avoid demanding work already assigned to later beads' "${QDV3_DESC}"

capture QDV4_DESC <<'EOF_QDV4_DESC'
Goal:
Prevent apply_fixes from becoming a backdoor for expanding task scope.

Acceptance:
- apply_fixes only receives fix-now amendments for the active bead
- planned-elsewhere and propose-new-bead findings are routed elsewhere
EOF_QDV4_DESC
create_issue QDV4 task 1 "${QDV}" phase1,workflow,apply-fixes,scope 'Update apply_fixes so it consumes only in-scope remediation for the active bead' "${QDV4_DESC}"

capture QDV5_DESC <<'EOF_QDV5_DESC'
Goal:
Keep final_review valuable at the end of a bead, but teach it to operate in milestone context.

Acceptance:
- final review understands the umbrella plan and nearby bead coverage
- it can approve a bead without demanding all future work now
- it proposes a new bead only when the missing work is real and not already represented
EOF_QDV5_DESC
create_issue QDV5 feature 1 "${QDV}" phase1,workflow,final-review,milestone 'Make final_review whole-plan-aware and conservative about creating new beads' "${QDV5_DESC}"
capture QDV5_COMMENT <<'EOF_QDV5_COMMENT'
Final review still matters. The change is not to remove it, but to give it the context needed to stop acting like
every bead is the entire project.
EOF_QDV5_COMMENT
add_comment "${QDV5}" "${QDV5_COMMENT}"

capture QDV6_DESC <<'EOF_QDV6_DESC'
Goal:
Prove the new review behavior with structured tests.

Acceptance:
- tests cover all three routing cases
- quick_dev can complete a bead cleanly when later work already exists
- tests verify that new bead creation is conservative
EOF_QDV6_DESC
create_issue QDV6 task 1 "${QDV}" phase1,workflow,tests,review 'Add workflow tests for planned-elsewhere behavior, conservative new-bead creation, and bead approval' "${QDV6_DESC}"

capture MCR_DESC <<'EOF_MCR_DESC'
Purpose:
Add the new component that selects beads and turns them into Ralph task runs.

Responsibilities:
- inspect milestone state
- inspect `.beads/`
- choose the next bead
- claim it
- create a bead-backed Ralph task
- run it through quick_dev
- reconcile the outcome back into milestone state and `.beads/`

Phase-1 scope is intentionally sequential: one active task per milestone.
EOF_MCR_DESC
create_issue MCR epic 1 "${ROOT}" phase1,milestone,controller,runtime 'Build the milestone controller for sequential bead execution' "${MCR_DESC}"
capture MCR_COMMENT <<'EOF_MCR_COMMENT'
This is the new meta-project component. It should be designed so future parallel execution can grow on top of it,
but the first implementation should stay single-bead-at-a-time.
EOF_MCR_COMMENT
add_comment "${MCR}" "${MCR_COMMENT}"

capture MCR1_DESC <<'EOF_MCR1_DESC'
Goal:
Make the controller an explicit state machine instead of a loose script.

Must define:
- idle
- selecting
- claimed
- running
- reconciling
- blocked / needs_operator
- completed

Acceptance:
- controller persistence is explicit
- operator can stop/resume without losing track of the active bead/task
EOF_MCR1_DESC
create_issue MCR1 task 1 "${MCR}" phase1,milestone,controller,state 'Define the controller state model, journal events, and stop/resume semantics' "${MCR1_DESC}"

capture MCR2_DESC <<'EOF_MCR2_DESC'
Goal:
Ask bv what to do next, then verify it against br's actual ready state before acting.

Acceptance:
- controller can request a next-bead recommendation
- selection is validated against blockers/readiness
- chosen bead is recorded in milestone state
EOF_MCR2_DESC
create_issue MCR2 feature 1 "${MCR}" phase1,milestone,controller,selection 'Select the next bead with bv guidance and br readiness validation' "${MCR2_DESC}"

capture MCR3_DESC <<'EOF_MCR3_DESC'
Goal:
Turn the selected bead into an owned task run.

Acceptance:
- br claim/update happens explicitly
- a Ralph task/project is created from the bead-backed prompt
- milestone state records the claim and linked task id
EOF_MCR3_DESC
create_issue MCR3 feature 1 "${MCR}" phase1,milestone,controller,claiming 'Claim a bead and create a corresponding Ralph task' "${MCR3_DESC}"

capture MCR4_DESC <<'EOF_MCR4_DESC'
Goal:
Orchestrate the active task run without replacing the workflow engine.

Acceptance:
- controller can start or resume the task
- controller tracks success/failure/pause state
- milestone status reflects the active run accurately
EOF_MCR4_DESC
create_issue MCR4 feature 1 "${MCR}" phase1,milestone,controller,execution 'Run the selected bead through the current quick_dev engine and monitor progress' "${MCR4_DESC}"

capture MCR5_DESC <<'EOF_MCR5_DESC'
Goal:
Close the loop from task execution back into bead-space.

Must handle:
- successful bead close
- notes/comments tied to covered-by-existing-bead findings
- mapping to existing beads
- true failure / retry states
- proposed new beads when the threshold is met

Acceptance:
- task outcomes result in explicit bead mutations
- milestone state and `.beads/` remain consistent enough to continue safely
EOF_MCR5_DESC
create_issue MCR5 feature 1 "${MCR}" phase1,milestone,controller,reconciliation 'Reconcile success, failure, planned-elsewhere notes, and missing-work proposals back into .beads' "${MCR5_DESC}"
capture MCR5_COMMENT <<'EOF_MCR5_COMMENT'
Reconciliation is where this whole design either compounds or falls apart. Be explicit, durable, and conservative.
EOF_MCR5_COMMENT
add_comment "${MCR5}" "${MCR5_COMMENT}"

capture MCR5A_DESC <<'EOF_MCR5A_DESC'
Goal:
Implement the happy path cleanly.

Acceptance:
- completed bead is closed in br
- sync occurs
- milestone progress updates
- next-step hints can be recorded from br/bv output
EOF_MCR5A_DESC
create_issue MCR5A task 1 "${MCR5}" phase1,milestone,controller,reconciliation,subtask 'Handle successful bead completion and close/suggest-next reconciliation' "${MCR5A_DESC}"

capture MCR5B_DESC <<'EOF_MCR5B_DESC'
Goal:
Persist the fact that a review concern was valid but already covered by another bead.

Acceptance:
- mappings to existing bead ids can be recorded
- active bead can still complete without unnecessary reopen/fix loops
EOF_MCR5B_DESC
create_issue MCR5B task 1 "${MCR5}" phase1,milestone,controller,reconciliation,subtask 'Handle planned-elsewhere findings and existing-bead mappings during reconciliation' "${MCR5B_DESC}"

capture MCR5C_DESC <<'EOF_MCR5C_DESC'
Goal:
Create a new bead only when truly necessary.

Acceptance:
- controller checks for an existing bead first
- new bead creation includes rationale and dependency placement
- sync runs after mutation
- operator-visible evidence explains why a new bead was created
EOF_MCR5C_DESC
create_issue MCR5C task 1 "${MCR5}" phase1,milestone,controller,reconciliation,subtask 'Handle propose-new-bead outcomes with conservative thresholds and dependency injection' "${MCR5C_DESC}"

capture MCR6_DESC <<'EOF_MCR6_DESC'
Goal:
Codify the policy for when new bead creation is allowed.

Acceptance:
- default choice is fix now if in scope
- second choice is map to existing planned work
- only then may controller create a new bead
- policy is shared by review/final-review/controller logic
EOF_MCR6_DESC
create_issue MCR6 task 1 "${MCR}" phase1,milestone,controller,new-work-policy 'Create new beads parsimoniously when review uncovers genuinely missing work' "${MCR6_DESC}"

capture MCR7_DESC <<'EOF_MCR7_DESC'
Goal:
Validate the controller under realistic phase-1 conditions.

Acceptance:
- tests cover success, blocked dependency, task failure, resume after interruption, and tool/adaptor failure
- sequential one-bead-at-a-time execution is proven before we consider parallelism
EOF_MCR7_DESC
create_issue MCR7 task 1 "${MCR}" phase1,milestone,controller,tests 'Add sequential runtime tests for happy path, blocked path, restart, and tool failure' "${MCR7_DESC}"

capture CLI_DESC <<'EOF_CLI_DESC'
Purpose:
Surface the new milestone/task model to users without forcing a disruptive rewrite underneath.

This epic owns:
- milestone commands
- task aliases/wrappers
- lineage/status output
- compatibility messaging during the transition
EOF_CLI_DESC
create_issue CLI epic 2 "${ROOT}" phase1,cli,ux,milestone,task 'Deliver milestone and task CLI/UX on top of the current system' "${CLI_DESC}"
capture CLI_COMMENT <<'EOF_CLI_COMMENT'
A lot of confusion can be prevented just by making the CLI say the right thing consistently.
EOF_CLI_COMMENT
add_comment "${CLI}" "${CLI_COMMENT}"

capture CLI1_DESC <<'EOF_CLI1_DESC'
Goal:
Provide a first-class CLI surface for the new umbrella abstraction.

Acceptance:
- users can create/plan/show/run milestones without dropping into internal tooling
- command output is grounded in milestone state and bead graph state
EOF_CLI1_DESC
create_issue CLI1 feature 1 "${CLI}" phase1,cli,milestone 'Add milestone create, plan, show, status, next, and run commands' "${CLI1_DESC}"
capture CLI1_COMMENT <<'EOF_CLI1_COMMENT'
The milestone CLI is where the new model becomes real for operators.
EOF_CLI1_COMMENT
add_comment "${CLI1}" "${CLI1_COMMENT}"

capture CLI1A_DESC <<'EOF_CLI1A_DESC'
Goal:
Implement the read-heavy command set first.

Acceptance:
- create/plan/show/status are functional
- output reflects milestone artifacts and progress
EOF_CLI1A_DESC
create_issue CLI1A task 1 "${CLI1}" phase1,cli,milestone,subtask 'Add milestone create/plan/show/status command handlers' "${CLI1A_DESC}"

capture CLI1B_DESC <<'EOF_CLI1B_DESC'
Goal:
Add the execution-facing commands that talk to the controller.

Acceptance:
- next shows the recommended bead
- run starts/resumes sequential execution
- active milestone selection rules are explicit
EOF_CLI1B_DESC
create_issue CLI1B task 1 "${CLI1}" phase1,cli,milestone,subtask 'Add milestone next/run commands and active milestone selection behavior' "${CLI1B_DESC}"

capture CLI2_DESC <<'EOF_CLI2_DESC'
Goal:
Let users talk about tasks while the implementation still uses project services underneath.

Acceptance:
- task commands exist or are clearly aliased
- output explains mapping where useful
EOF_CLI2_DESC
create_issue CLI2 feature 2 "${CLI}" phase1,cli,task,naming 'Add task aliases/wrappers around the current project commands' "${CLI2_DESC}"

capture CLI3_DESC <<'EOF_CLI3_DESC'
Goal:
Make normal inspection commands milestone-aware.

Acceptance:
- show/status/history output can reveal milestone_id and bead_id
- active task/run display is useful in milestone mode
EOF_CLI3_DESC
create_issue CLI3 task 2 "${CLI}" phase1,cli,lineage,status 'Show milestone and bead lineage in task/project/run output' "${CLI3_DESC}"

capture CLI4_DESC <<'EOF_CLI4_DESC'
Goal:
Avoid breaking existing operators and scripts while new terminology lands.

Acceptance:
- current project commands still work
- messaging makes it clear when project == internal task substrate
EOF_CLI4_DESC
create_issue CLI4 task 2 "${CLI}" phase1,cli,compatibility 'Keep project commands as compatibility aliases during the transition' "${CLI4_DESC}"

capture CLI5_DESC <<'EOF_CLI5_DESC'
Goal:
Align the docs/help output with the new mental model.

Acceptance:
- docs and help mention milestone/task terminology consistently
- compatibility notes explain how legacy project commands map to the new model
EOF_CLI5_DESC
create_issue CLI5 task 2 "${CLI}" phase1,cli,docs,help 'Update CLI reference and help text for milestone/task vocabulary' "${CLI5_DESC}"

capture QA_DESC <<'EOF_QA_DESC'
Purpose:
Turn the new design into something operable and releasable.

This epic covers:
- unit and integration tests
- conformance scenarios
- operator docs for milestone + br workflows
- explicit documentation of deferred parallelism
EOF_QA_DESC
create_issue QA epic 1 "${ROOT}" phase1,tests,docs,conformance,release 'Conformance, integration tests, docs, and operator hardening for phase 1' "${QA_DESC}"
capture QA_COMMENT <<'EOF_QA_COMMENT'
The controller, planner, and review changes cross many modules. This epic exists so the final system is not only
implemented, but also testable, operable, and explainable.
EOF_QA_COMMENT
add_comment "${QA}" "${QA_COMMENT}"

capture QA1_DESC <<'EOF_QA1_DESC'
Goal:
Cover the new model/service boundaries with focused tests.

Acceptance:
- milestone model/storage/service tests exist
- planner contract/rendering tests exist
- br/bv adapter tests exist
- prompt generation tests exist
EOF_QA1_DESC
create_issue QA1 task 1 "${QA}" phase1,tests,unit 'Add unit tests for milestone models, planners, prompt generation, and adapters' "${QA1_DESC}"

capture QA2_DESC <<'EOF_QA2_DESC'
Goal:
Prove the full integrated flow once, end to end.

Acceptance:
- the scenario covers milestone planning, bead export, next-bead selection, task creation, quick_dev execution,
  review classification, and bead reconciliation
- the scenario is automated enough to rerun during development
EOF_QA2_DESC
create_issue QA2 feature 1 "${QA}" phase1,tests,e2e,acceptance 'Add an end-to-end scenario: idea -> milestone plan -> bead graph -> task run -> bead close' "${QA2_DESC}"
capture QA2_COMMENT <<'EOF_QA2_COMMENT'
This is the closest thing to a reality check for phase 1. If this scenario is weak, the architecture probably is too.
EOF_QA2_COMMENT
add_comment "${QA2}" "${QA2_COMMENT}"

capture QA2A_DESC <<'EOF_QA2A_DESC'
Goal:
Create a reproducible fixture repo/workspace that can exercise the integrated flow safely.

Acceptance:
- fixture includes milestone artifacts and a beads workspace
- tests can run without depending on a developer's real repo state
EOF_QA2A_DESC
create_issue QA2A task 1 "${QA2}" phase1,tests,e2e,fixtures,subtask 'Build a temp-workspace fixture with .beads and milestone artifacts for integration testing' "${QA2A_DESC}"

capture QA2B_DESC <<'EOF_QA2B_DESC'
Goal:
Turn the fixture into a meaningful automated scenario.

Acceptance:
- assertions cover milestone creation, bead export, selection, task run, reconciliation, and status updates
EOF_QA2B_DESC
create_issue QA2B task 1 "${QA2}" phase1,tests,e2e,scenario,subtask 'Script the end-to-end acceptance scenario and assertions' "${QA2B_DESC}"

capture QA3_DESC <<'EOF_QA3_DESC'
Goal:
Lock in the whole-plan-aware review behavior so it does not regress.

Acceptance:
- conformance scenarios cover fix-now, planned-elsewhere, and propose-new-bead outcomes
- approval behavior is tested for a bead whose later work already exists in the graph
EOF_QA3_DESC
create_issue QA3 task 1 "${QA}" phase1,tests,conformance,workflow 'Add conformance coverage for milestone-aware quick_dev review behavior' "${QA3_DESC}"

capture QA4_DESC <<'EOF_QA4_DESC'
Goal:
Write the runbook future operators will actually need.

Must cover:
- normal milestone planning/export/run flow
- br sync expectations
- recovery after git pull / sync conflicts / failed task run
- meaning of review classifications
- when a new bead is expected vs suspicious

Acceptance:
- docs are specific enough to unblock an operator who did not build the system
EOF_QA4_DESC
create_issue QA4 task 2 "${QA}" phase1,docs,ops,br-bv 'Document operator workflow for br sync, milestone runs, recovery, and review outcomes' "${QA4_DESC}"

capture QA5_DESC <<'EOF_QA5_DESC'
Goal:
Preserve the shape of later parallel milestone execution without pulling it into phase 1.

Must cover:
- one worktree lease per active bead task
- controller concurrency limits
- interaction with br claims and bead readiness
- how bv might help with parallel track selection later

Acceptance:
- a compact design note exists
- phase-1 code is left ready enough to grow in this direction later
EOF_QA5_DESC
create_issue QA5 meta 3 "${QA}" backlog,parallelism,design,worktrees 'Capture the deferred phase-2 parallel execution design on top of existing worktree leases' "${QA5_DESC}"
capture QA5_COMMENT <<'EOF_QA5_COMMENT'
This is intentionally a follow-on design capture, not part of the phase-1 critical path.
EOF_QA5_COMMENT
add_comment "${QA5}" "${QA5_COMMENT}"

capture PAR_DESC <<'EOF_PAR_DESC'
Purpose:
Track the explicitly deferred work needed to run multiple ready beads in parallel later.

This epic is here so future-us does not forget the intended direction, but it should stay out of the phase-1
execution path.
EOF_PAR_DESC
create_issue PAR epic 4 "${ROOT}" backlog,parallelism,milestone,runtime 'Backlog: parallel milestone execution on top of existing worktree leases' "${PAR_DESC}"
capture PAR_COMMENT <<'EOF_PAR_COMMENT'
Keep this epic deferred until sequential milestone execution is proven.
EOF_PAR_COMMENT
add_comment "${PAR}" "${PAR_COMMENT}"

capture PAR1_DESC <<'EOF_PAR1_DESC'
Goal:
Define how the milestone controller will scale to multiple active bead tasks without shared-worktree chaos.

Acceptance:
- design fits the current automation_runtime lease model
- ownership and cleanup semantics are explicit
EOF_PAR1_DESC
create_issue PAR1 spike 4 "${PAR}" backlog,parallelism,worktrees,design 'Design controller concurrency around one worktree lease per active bead task' "${PAR1_DESC}"

capture PAR2_DESC <<'EOF_PAR2_DESC'
Goal:
Define how future parallel execution chooses multiple independent beads safely.

Acceptance:
- design explains how bv planning/triage/capacity outputs inform parallel choice
- deferred until sequential controller behavior is stable
EOF_PAR2_DESC
create_issue PAR2 spike 4 "${PAR}" backlog,parallelism,bv,design 'Design multi-bead selection and capacity rules using bv guidance' "${PAR2_DESC}"

echo "==> wiring dependencies"
depends_on "${FND4}" "${FND1}"
depends_on "${FND4}" "${FND2}"
depends_on "${FND4}" "${FND3}"
depends_on "${MS1}" "${FND1}"
depends_on "${MS1}" "${FND3}"
depends_on "${MS2}" "${MS1}"
depends_on "${MS3}" "${MS2}"
depends_on "${MS4}" "${MS2}"
depends_on "${MS5}" "${MS2}"
depends_on "${PLN1}" "${FND2}"
depends_on "${PLN1}" "${FND3}"
depends_on "${PLN2}" "${PLN1}"
depends_on "${PLN2B}" "${PLN2A}"
depends_on "${PLN2C}" "${PLN2A}"
depends_on "${PLN3}" "${PLN1}"
depends_on "${PLN3}" "${PLN2A}"
depends_on "${PLN4}" "${PLN2A}"
depends_on "${PLN4}" "${PLN3}"
depends_on "${PLN5}" "${PLN2A}"
depends_on "${PLN5}" "${PLN3}"
depends_on "${PLN5}" "${MS2}"
depends_on "${PLN6}" "${PLN2A}"
depends_on "${PLN7}" "${PLN2B}"
depends_on "${PLN7}" "${PLN3}"
depends_on "${PLN7}" "${PLN6}"
depends_on "${BRV1}" "${FND2}"
depends_on "${BRV1}" "${FND3}"
depends_on "${BRV1B}" "${BRV1A}"
depends_on "${BRV1C}" "${BRV1A}"
depends_on "${BRV2}" "${FND2}"
depends_on "${BRV2}" "${FND3}"
depends_on "${BRV3}" "${PLN4}"
depends_on "${BRV3}" "${BRV1C}"
depends_on "${BRV3}" "${MS2}"
depends_on "${BRV4}" "${BRV1C}"
depends_on "${BRV5}" "${BRV1B}"
depends_on "${BRV5}" "${BRV2}"
depends_on "${BRV6}" "${BRV1B}"
depends_on "${BRV6}" "${BRV1C}"
depends_on "${BRV6}" "${BRV2}"
depends_on "${BRV6}" "${BRV4}"
depends_on "${TSK1}" "${FND1}"
depends_on "${TSK1}" "${MS2}"
depends_on "${TSK2}" "${TSK1}"
depends_on "${TSK2}" "${BRV5}"
depends_on "${TSK3}" "${TSK2}"
depends_on "${TSK3}" "${PLN3}"
depends_on "${TSK4}" "${TSK1}"
depends_on "${TSK5}" "${FND1}"
depends_on "${TSK5}" "${TSK2}"
depends_on "${PRT1}" "${FND2}"
depends_on "${PRT1}" "${PLN3}"
depends_on "${PRT1}" "${BRV5}"
depends_on "${PRT2}" "${PRT1}"
depends_on "${PRT2}" "${TSK2}"
depends_on "${PRT3}" "${PRT1}"
depends_on "${PRT3}" "${MS2}"
depends_on "${PRT4}" "${PRT1}"
depends_on "${PRT4}" "${BRV5}"
depends_on "${PRT5}" "${PRT2}"
depends_on "${PRT5}" "${PRT3}"
depends_on "${PRT5}" "${PRT4}"
depends_on "${QDV1}" "${PRT2}"
depends_on "${QDV2}" "${FND2}"
depends_on "${QDV2}" "${PRT1}"
depends_on "${QDV2B}" "${QDV2A}"
depends_on "${QDV2B}" "${PRT2}"
depends_on "${QDV3}" "${QDV2B}"
depends_on "${QDV3}" "${PRT2}"
depends_on "${QDV4}" "${QDV3}"
depends_on "${QDV5}" "${QDV2B}"
depends_on "${QDV5}" "${PRT2}"
depends_on "${QDV6}" "${QDV1}"
depends_on "${QDV6}" "${QDV3}"
depends_on "${QDV6}" "${QDV4}"
depends_on "${QDV6}" "${QDV5}"
depends_on "${MCR1}" "${MS5}"
depends_on "${MCR1}" "${BRV5}"
depends_on "${MCR1}" "${TSK1}"
depends_on "${MCR2}" "${MCR1}"
depends_on "${MCR2}" "${BRV2}"
depends_on "${MCR2}" "${BRV5}"
depends_on "${MCR3}" "${MCR2}"
depends_on "${MCR3}" "${TSK2}"
depends_on "${MCR4}" "${MCR3}"
depends_on "${MCR4}" "${QDV5}"
depends_on "${MCR5}" "${MCR4}"
depends_on "${MCR5}" "${BRV1C}"
depends_on "${MCR5}" "${QDV2A}"
depends_on "${MCR6}" "${QDV5}"
depends_on "${MCR6}" "${MCR5B}"
depends_on "${MCR6}" "${MCR5C}"
depends_on "${MCR7}" "${MCR2}"
depends_on "${MCR7}" "${MCR3}"
depends_on "${MCR7}" "${MCR4}"
depends_on "${MCR7}" "${MCR5A}"
depends_on "${MCR7}" "${MCR5B}"
depends_on "${MCR7}" "${MCR5C}"
depends_on "${CLI1}" "${MS3}"
depends_on "${CLI1}" "${PLN5}"
depends_on "${CLI1}" "${MCR6}"
depends_on "${CLI1B}" "${CLI1A}"
depends_on "${CLI1B}" "${MCR6}"
depends_on "${CLI2}" "${TSK5}"
depends_on "${CLI3}" "${CLI1A}"
depends_on "${CLI3}" "${TSK4}"
depends_on "${CLI4}" "${CLI2}"
depends_on "${CLI5}" "${CLI1B}"
depends_on "${CLI5}" "${CLI2}"
depends_on "${CLI5}" "${CLI3}"
depends_on "${CLI5}" "${CLI4}"
depends_on "${QA1}" "${MS2}"
depends_on "${QA1}" "${PLN7}"
depends_on "${QA1}" "${BRV6}"
depends_on "${QA1}" "${PRT5}"
depends_on "${QA2}" "${CLI1B}"
depends_on "${QA2}" "${MCR6}"
depends_on "${QA2}" "${BRV3}"
depends_on "${QA2B}" "${QA2A}"
depends_on "${QA2B}" "${CLI1B}"
depends_on "${QA2B}" "${MCR6}"
depends_on "${QA3}" "${QDV6}"
depends_on "${QA3}" "${CLI1A}"
depends_on "${QA4}" "${BRV4}"
depends_on "${QA4}" "${CLI5}"
depends_on "${QA4}" "${MCR6}"
depends_on "${QA5}" "${MCR7}"
depends_on "${PAR1}" "${MCR7}"
depends_on "${PAR2}" "${MCR7}"
depends_on "${PAR2}" "${BRV2}"

echo "==> deferring explicitly deferred backlog items"
defer_issue "${PAR}" "${PAR1}" "${PAR2}"

echo "==> final sync"
"${BR[@]}" sync --flush-only >/dev/null

echo "==> done"
echo "Root epic: ${ROOT}"
echo "Foundations epic: ${FND}"
echo "Milestone domain epic: ${MS}"
echo "Planner epic: ${PLN}"
echo "br/bv integration epic: ${BRV}"
echo "Task substrate epic: ${TSK}"
echo "Prompt/AGENTS epic: ${PRT}"
echo "quick_dev integration epic: ${QDV}"
echo "Milestone controller epic: ${MCR}"
echo "CLI epic: ${CLI}"
echo "QA/docs epic: ${QA}"
echo "Deferred parallelism epic: ${PAR}"
echo "Next steps:"
echo "  br epic status"
echo "  br ready --json"
echo "  br dep cycles"
echo "  git add .beads/ && git commit -m 'seed milestone integration beads'"

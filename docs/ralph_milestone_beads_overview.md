# Ralph milestone integration bead map

This is the bead breakdown encoded by `ralph_milestone_beads_bootstrap_v2.sh`.

## Intent
- add a milestone layer above Ralph’s current project/run substrate
- keep `.beads/` canonical for task graph state via br/bv
- reuse `quick_dev` for one-bead execution with milestone-aware review
- build a sequential milestone controller first
- keep future parallel execution documented but deferred

## Top-level epics
- **FND** — Define phase-1 vocabulary, invariants, and acceptance boundaries
- **MS** — Add milestone domain model, storage, and status tracking
- **PLN** — Extend requirements_drafting into a milestone planner
- **BRV** — Add br/bv integration for the canonical bead graph
- **TSK** — Bridge the current project/run substrate into milestone-scoped task execution
- **PRT** — Generate milestone-aware prompts and AGENTS guidance for each bead task
- **QDV** — Make quick_dev milestone-aware without replacing its shape
- **MCR** — Build the milestone controller for sequential bead execution
- **CLI** — Deliver milestone and task CLI/UX on top of the current system
- **QA** — Conformance, integration tests, docs, and operator hardening for phase 1
- **PAR** — Backlog: parallel milestone execution on top of existing worktree leases

## Critical path
1. FND -> MS / PLN / BRV
2. BRV + PLN + MS -> TSK / PRT
3. PRT -> QDV
4. MS + BRV + TSK + QDV -> MCR
5. MCR + MS + TSK -> CLI
6. Everything above -> QA

## Explicitly deferred
- `PAR` and its subtasks are created and then deferred.
- They document future worktree-backed parallel milestone execution.

## Script behavior
- creates 1 root epic, 10 active epics, 1 deferred backlog epic, and all tasks/subtasks
- adds labels, rich descriptions, issue comments, and dependency edges
- defers the explicit parallelism backlog
- ends with `br sync --flush-only`

## Usage
```bash
chmod +x ./ralph_milestone_beads_bootstrap_v2.sh
BR_ACTOR=planner ./ralph_milestone_beads_bootstrap_v2.sh
br epic status
br dep cycles
br ready --json
git add .beads/
git commit -m "seed milestone integration beads"
```

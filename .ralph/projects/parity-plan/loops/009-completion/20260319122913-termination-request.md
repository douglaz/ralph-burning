---
artifact: termination-request
loop: 9
project: parity-plan
backend: claude(opus)
role: planner
created_at: 2026-03-19T12:29:13Z
---

# Project Completion Request

## Rationale
All seven slices defined in the master prompt have been implemented and approved:

- **Slice 0** (Loop 1): P0 Hardening and Sign-off — completed and approved
- **Slice 1** (Loop 2): Full Requirements and PRD Parity — completed and approved
- **Slice 2** (Loop 3): Bootstrap and Auto Parity — completed and approved
- **Slice 3** (Loop 4): Manual Amendment Parity — completed and approved
- **Slice 4** (Loop 5): Operator UX Parity — completed and approved
- **Slice 5** (Loop 6): Backend Operations Parity — completed and approved
- **Slice 6** (Loop 7): Tmux and Streaming Parity — completed and approved
- **Slice 7** (Loop 8): Prompt and Template Override Parity — completed and approved

Every slice in the defined slice order (0–7) has a corresponding completed loop with an approved verdict. No further features remain in the master prompt's scope.

## Summary of Work
1. **P0 Hardening**: Backend permission checks, cancel/timeout signal handling, panel preflight validation, resolution snapshot drift detection, GitHub URL encoding.
2. **Requirements/PRD Parity**: Staged requirements pipeline with ideation, research, synthesis, gap analysis, validation, conditional question rounds, and ProjectSeed generation. Quick mode with writer/reviewer loop.
3. **Bootstrap/Auto Parity**: `project create --from-requirements`, `project bootstrap` with `--idea`, `--from-file`, and `--start` flags. Atomic failure semantics.
4. **Manual Amendment Parity**: `project amend add/list/remove/clear` with source metadata, dedup, completion gating, and project reopen on post-completion amendment.
5. **Operator UX Parity**: `run status --json`, `run history --verbose/--json`, `run tail --last/--follow/--logs`, `run rollback --list`, `run show-payload`, `run show-artifact`.
6. **Backend Operations Parity**: `backend list`, `backend check`, `backend show-effective`, `backend probe --role --flow`.
7. **Tmux/Streaming Parity**: `tmux` adapter, `direct`/`tmux` execution modes, `run attach`, cancel/timeout cleanup, equivalent durable history across modes.
8. **Template Override Parity**: Template catalog API with project > workspace > built-in precedence, shared resolution for workflow and requirements prompts, malformed override rejection.

## Remaining Items
- Run the manual smoke matrix (all flow/backend combinations, daemon routing, PR workflows, rebase flow) and record results.
- Final sign-off: confirm P0 conformance remains green across all slices, all parity conformance cases pass, and no open correctness issues remain.
- Update repository docs to point users exclusively to `ralph-burning`.

---

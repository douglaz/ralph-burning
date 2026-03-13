---
artifact: termination-request
loop: 15
project: ralph-burning-rewrite
backend: claude(opus)
role: planner
created_at: 2026-03-13T03:00:46Z
---

All requirements from the master prompt are satisfied. Every delivery roadmap slice (1-12) has a corresponding completed and approved loop, all 7 bounded contexts are implemented, all CLI commands work, all 4 flow presets execute end-to-end, and the conformance suite provides 201 scenarios across 18 feature files with a runner that supports filtering and fail-fast semantics.

# Project Completion Request

## Rationale
All 12 delivery roadmap slices from Section 14 have been implemented through approved loops (5–14), with loops 1–4 providing the baseline. Every hard requirement in the master prompt is satisfied:

- **CLI Contract (Section 8):** All commands implemented — `init`, `config {show,get,set,edit}`, `flow {list,show}`, `project {create,select,list,show,delete}`, `run {start,resume,status,history,tail,rollback}`, `requirements {draft,quick,show,answer}`, `daemon {start,status,abort,retry,reconcile}`, `conformance {list,run}`.
- **Bounded Contexts (Section 5):** All 7 contexts with ports/adapters boundaries — `workspace_governance`, `project_run_record`, `workflow_composition`, `agent_execution`, `requirements_drafting`, `automation_runtime`, `conformance_spec`.
- **Flow Presets (Section 9):** All 4 presets (`standard`, `quick_dev`, `docs_change`, `ci_improvement`) with correct stage sequences, final-review policies, and validation profiles.
- **Core Invariants (Section 7):** Immutable flow per project, single stage cursor per run, monotonic cycle/completion-round numbers, schema-then-domain validation before mutation, journal as authoritative event source, writer lock per project, one active daemon task per issue, worktree lease exclusivity, command > label > repo-default routing precedence, and fail-fast on unsupported workspace versions.
- **Stage Contracts (Section 10):** JSON schema + domain validation + deterministic Markdown rendering for all 16 stage types across all presets. Failure classes (transport, schema, domain, timeout, cancellation, QA/review outcome) map to distinct retry/terminal policies.
- **Testability (Section 15):** 201 Gherkin scenarios across 18 feature files; unit, service-level, adapter-contract, and integration tests for each context; conformance runner with `--filter` scenario selection and non-zero exit on failure.
- **Cutover (Roadmap Slice 12):** No legacy runtime paths in the v1 entrypoint; conformance gate ready for CI.

## Summary of Work
| Loop | Feature | Roadmap Slice |
|------|---------|--------------|
| 1–4 | Baseline: domain model, workspace init, flow discovery, project/run store, stage contracts | Slices 1–4 |
| 5 | Agent Execution Facade with structured-output guarantee | Slice 5 |
| 6 | Standard Preset Run Start end-to-end orchestration | Slice 6 |
| 7 | Retry, Remediation, Resume, and Failure Class Semantics | Slice 7 |
| 8 | Completion Rounds, Late-Stage Acceptance, Durable Amendments | Slice 8 |
| 9 | Docs Change and CI Improvement Flow Execution | Slice 9 |
| 10 | Requirements Drafting and Project Seed Handoff | Slice 10 |
| 11 | Daemon Routing, Tasks, Leases, and Worktree Management | Slice 11 |
| 12 | Quick Dev Flow Public Execution | Slice 9 (completes all 4 presets) |
| 13 | Run Rollback (logical + hard with VCS reset) | Section 8 CLI |
| 14 | Conformance Runner and CI Gate | Slice 12 |

## Remaining Items
- **Property/invariant tests:** Section 15 calls for property tests "where feasible." Adding `proptest` or `quickcheck` coverage for monotonicity invariants and stage-cursor transition rules would strengthen the test suite but is not blocking v1 launch.
- **GitHub adapter for daemon watchers:** The daemon routing framework and task lifecycle are complete; wiring a live GitHub issue/PR watcher adapter is an infrastructure integration task beyond the core domain.
- **Interactive requirements via GitHub issues:** The requirements CLI pipeline (draft → answer → review → seed) is fully functional; the GitHub-issue-based interactive path is an adapter extension.
- **Backend timeout configuration:** Currently hardcoded defaults; exposing per-backend timeout tuning via `workspace.toml` is a non-blocking enhancement.

---

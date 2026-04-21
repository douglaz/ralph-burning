# Bead 9ni.5.7: Harden `create-from-bead` against duplicate active-bead task creation

## Problem description

`project create-from-bead` / `task create` can mint a new project for a bead
that already has a non-terminal lineage attempt. At run-start time,
`record_task_run_start` in `src/adapters/fs.rs` rejects *different*-bead active
attempts but does not prevent creating *another* project for the *same* active
bead. The final round of 9ni.5.2 surfaced this as a policy gap: the current
behavior is ambiguous (operator might double-claim a bead, leaving competing
projects with no explicit handoff semantics).

Goal: make the policy explicit at create time and keep create/start semantics
aligned.

### Expected policy

- If the same bead has **no active task run** (all attempts terminal or no
  prior attempts): allow creation.
- If the same bead has **one active task run** whose project is still
  non-terminal (not-started, running, paused): reject creation with a clear
  error naming the existing project/run, unless `--force` or equivalent is
  passed. Operators should resolve the existing run first.
- If the same bead has an **active task run that failed or was stopped** (a
  "retry" situation): behave consistently with run-start's existing
  retry/reopen logic — prefer `run resume` on the existing project, and only
  allow creation if the operator passes an explicit retry flag that we route
  to the existing project rather than spinning up a new one.
- If the prior active record is clearly orphaned (e.g. project directory
  missing but task_run entry says `running`): surface the inconsistency with
  a remediation hint rather than silently creating a parallel project.

### Required changes

1. `src/cli/project.rs::execute_create_from_bead` — before the
   `FsProjectStore.project_exists` check, add a duplicate-active-bead check
   that reads existing task-run entries (same mechanism
   `record_task_run_start` uses) and rejects if a non-terminal same-bead
   record exists. Fail with a specific `AppError` variant (reuse
   `RunStartFailed` or add a new targeted one — prefer a new variant
   `DuplicateActiveBead { bead_id, existing_project_id, existing_run_id }`
   so callers can special-case it).
2. `src/adapters/fs.rs` — if a helper like `running_task_runs_for_bead`
   already exists, reuse it; expose a thin read-only function
   (`FsMilestoneTaskRunStore::active_task_runs_for_bead` or similar) for the
   cli layer rather than re-implementing the scan inline.
3. `src/cli/run.rs` — double-check that `run start` lineage logic still
   agrees. If run-start had an implicit "allow duplicate project for same
   bead if its own project record is different" path, surface the same
   explicit error.
4. Tests — add deterministic coverage in the appropriate test module:
   - creating a second project for the same actively-running bead is rejected
   - creating a project for a bead whose prior attempt is terminal succeeds
   - creating a project for a different bead when another bead is active is
     still handled by the existing different-bead guard and does not regress
   - an orphaned task-run entry (project dir missing) produces a helpful
     error, not a silent shadow project

## Implementation hints

Relevant code paths:
- `src/cli/project.rs::execute_create_from_bead` — insert the duplicate
  check between project-id resolution and `claim_bead_in_br`.
- `src/adapters/fs.rs` — `record_task_run_start`,
  `running_task_runs_for_bead`, `collapse_task_run_attempts`,
  `matching_finalized_task_runs` already do the scanning — factor out a
  read-only helper rather than re-implementing.
- `src/shared/domain.rs` — add a new `AppError` variant if needed.
- Tests: prefer `tests/unit/project_run_record_test.rs` or a `task_create`
  test module — follow the existing fixture style (use real fs + a
  throwaway `tempdir`).

Existing utilities to reuse:
- `milestone_bead_refs_match` — to compare bead identities consistently.
- `TaskRunOutcome::is_terminal` — to classify active vs terminal attempts.

Do **not** change the external bead-claim (`br`) call order. The claim still
happens after validation succeeds, never before.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files. The same applies to `.beads/` — that is durable bead state,
not code.

## Acceptance criteria

- `project create-from-bead` / `task create` rejects a second project for
  the same bead while the prior project is non-terminal, with an actionable
  error that names the existing project and run.
- Retry-on-failed attempts still work via `run resume` on the existing
  project (no regression).
- Different-bead active conflict behavior (from `record_task_run_start`) is
  unchanged.
- Orphaned task-run entries produce a clear remediation hint.
- Deterministic tests cover same-bead duplicate creation, retry, and the
  orphaned case.
- `nix build` passes on the final tree (authoritative gate).
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.

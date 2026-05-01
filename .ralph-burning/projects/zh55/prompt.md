# Work Item: ralph-burning-zh55 - Stabilize check_available_times_out_when_version_probe_hangs (50ms timeout race)

## Goal

This test in src/adapters/br_process.rs:2127 spawns a fake-br binary that
hangs on --version, then asserts check_available_with_timeout(50ms) returns
BrTimeout. On loaded CI runners the spawn itself can exceed 50ms, producing
a different error class (likely BrExitError or pipe-related) and the
matches!(error, BrError::BrTimeout {..}) assertion fails.

Failure mode observed in https://github.com/douglaz/ralph-burning/actions/runs/25098402297 (PR #200 amended push). Test passes locally and on most CI runs; rerun typically succeeds.

Fix options:
- Bump the timeout to 200-500ms (more headroom for spawn under load)
- Or assert on either BrTimeout OR a transient error class
- Or pre-warm the fake-br binary (e.g., a no-op invocation) before timing the hung one

## Acceptance Criteria

## Nearby Graph Context

- **Parent:**
  - None.
- **Closed blockers:**
  - None.
- **Direct dependents:**
  - None.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Repository Norms

- `AGENTS.md`
- `CLAUDE.md`

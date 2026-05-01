# Work Item: ralph-burning-qc7z - drain harness: mock at the right level to catch missing pr_open/pr_watch invocations (xrwl follow-up)

## Goal

The xrwl drain harness mocks the drain at a level that doesn't exercise pr_open/pr_watch invocations — that's why it didn't catch the bug filed as part of the first real-world drain test (vl8z fails to invoke the PR machinery and tries to push to master directly).

Tighten the harness so that scenarios where drain SHOULD call pr_open/pr_watch verify those calls happened on the mock ports.

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

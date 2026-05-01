# Work Item: ralph-burning-yy7y - ralph pr open: fetch remote refs before resolving base branch

## Goal

Bot finding on PR #214 (sr2x): the new BaseBranch resolver in pr_open.rs only inspects local refs (origin/HEAD, origin/main, origin/master). In repos where origin exists but no refs/remotes/origin/* have been materialized locally (e.g. fresh clone with origin added but never fetched), resolution fails before any git fetch happens. Previously master-based repos could recover via git fetch origin master.

Fix: do a git fetch origin (or git fetch origin --no-tags --filter=blob:none with a default-ref query) before base_branch_ref's local-ref resolution. The fetch is idempotent and cheap if refs are up to date.

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

# Work Item: ralph-burning-run8 - ralph pr open: validate real-code diff before destructive soft-reset

## Goal

PR #208 (2qlo) performs \`git reset --soft origin/master\` and *then* checks whether any review-scope diff remains, returning NoRealCodeChanges if the staged diff is metadata-only. The branch HEAD has already been rewritten by the time the error fires, so the user has to recover via reflog instead of getting a clean no-op failure.

Fix: peek at \`git diff origin/master..HEAD\` (filtered to review-scope paths) BEFORE the reset. Only reset when real-code is present. Document the flow in a comment.

Bot's review on PR #208: https://github.com/douglaz/ralph-burning/pull/208#discussion_r2522714

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

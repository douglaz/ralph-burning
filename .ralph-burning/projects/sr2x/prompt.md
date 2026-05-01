# Work Item: ralph-burning-sr2x - ralph pr open: resolve base branch instead of hardcoding master

## Goal

PR #208 (2qlo) hardcodes \`master\`/\`origin/master\` in src/contexts/bead_workflow/pr_open.rs at multiple call sites (fetch, ancestry check, log walk, soft-reset, gh pr base). This breaks in repos whose default branch is main or any non-master name.

Fix: introduce a base-branch resolver on GitPort that runs \`git symbolic-ref --quiet --short refs/remotes/origin/HEAD\` and falls back to checking \`origin/main\` then \`origin/master\` (the same logic src/contexts/workflow_composition/final_review.rs::review_base_ref already uses). Cache the result on the GitPort instance; thread the resolved ref through the pr_open methods that currently take literal "master".

Bot's review on PR #208: https://github.com/douglaz/ralph-burning/pull/208#discussion_r2522715

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

# Bead 2qlo — ralph pr open: post-run squash, push, PR creation (drain loop)

## Problem

Today, after a project run reports `completed`, the operator manually:
1. Verifies gates: `cargo fmt --check`, `cargo clippy --locked -- -D warnings`,
   `cargo test`, `nix build`.
2. Soft-resets the `rb: checkpoint …` commits to `origin/master`.
3. Hand-writes a commit message that summarizes the bead, the convergence
   pattern, and the file-stat summary.
4. Commits and pushes the branch.
5. Hand-writes a PR title and body and runs `gh pr create`.

This is the part of the drain loop that runs on every successful bead. It
is currently fully manual and costs ~2–5 minutes of human attention per
bead. This bead automates it.

The 2qlo command is the success-path exit. It does NOT watch the PR or
merge — that's bead `itn1`. It does NOT decide whether to create a PR
based on bead success/failure — that's bead `vl8z` (the drain loop). It
just runs the end-of-cycle automation that's appropriate when the run
succeeded.

## Required behavior

Add a CLI subcommand. Natural shape: `ralph pr open [--bead-id <id>]
[--skip-gates]`. The bead-id is needed for rendering the PR body;
default it to the active project's bead binding if the project has one.

The command:

1. **Verify the run is in `completed` status.** If not, fail with a
   clear error pointing at `ralph run status` / `run resume`.

2. **Run gates** unless `--skip-gates` is passed:
   - `cargo fmt --check`
   - `cargo clippy --locked -- -D warnings`
   - `cargo test`
   - `nix build`
   On any failure: surface a structured error naming the failing gate
   AND its stderr excerpt. Do NOT push. Do NOT file a bead — that's
   the drain loop's call (bead `gj74`'s policy decision).

3. **Soft-reset checkpoint commits** to `origin/master`:
   - `git fetch origin master`
   - `git reset --soft origin/master`
   - Verify only `rb: checkpoint …` commits were squashed (sanity:
     run history shouldn't have hand-written commits this command
     would silently subsume).

4. **Render the commit message** from:
   - Bead title and id (line 1: `<bead-id>: <bead-title>` or similar).
   - Convergence pattern (extracted from the run's journal — count of
     amendments per completion round, e.g. `1, 2, 0`).
   - Real-code diff stats (excluding `.ralph-burning/` paths).
   - The standard `Co-Authored-By` trailer used in this repo.

5. **Commit + push** the squashed work. Branch name: derive from the
   currently-checked-out branch (likely `feat/<bead-id>-…` from
   d31l) — do not guess; if it's not a feature branch, fail.

6. **Open the PR via `gh pr create`** with:
   - Title: bead-id + bead-title.
   - Body sections rendered by the command:
     - **Summary**: bead description first paragraph + a "closes
       <bead-id>" link.
     - **Run history**: convergence pattern.
     - **Diff stats**: real-code-only file list.
     - **Reviewer-attention block**: the orchestration-state exclusion
       boilerplate, listing the actual files reviewers should look at
       (derived from the diff).
     - **Test plan**: derived from the bead's structured acceptance
       criteria field; render each as a checkbox. Always include
       `nix build` as the last item.

7. **Print the PR URL** to stdout so callers (operator or
   `ralph drain`) can use it.

## Failure modes (each tested where feasible)

- Run not in `completed` status → error with status hint.
- Gate failure (any of fmt/clippy/test/nix-build) → structured
  error naming the gate.
- No checkpoint commits to squash → no-op; either fail or warn,
  pick the safer behavior (probably warn-and-continue if the diff
  is otherwise clean).
- Branch name doesn't look like a feature branch → fail with
  pointer to creating one.
- `gh pr create` returns an error (e.g., branch already has a PR
  open) → surface verbatim.
- `git push` fails (e.g., conflict with origin) → surface verbatim.

## Reuse vs duplicate

Reuse the existing bead-prompt renderer's br port (4wdb / d31l) for
bead lookup. The PR-body rendering is a separate template — co-locate
it with the bead-workflow context (`src/contexts/bead_workflow/
pr_open.rs` is a natural home) but do not couple it to the project-
prompt renderer. Different output shape, different audience.

For shelling out to git and `gh`, factor a small port (
`PrToolPort` / `GitPort` or similar) so the tests can mock without
real binaries.

## Tests

- Happy path: project in `completed` state, gates clean, mock
  git/gh ports → command produces the expected commit message,
  pushes, and opens a PR with the rendered body.
- Run not completed → error.
- One of each gate fails → command exits without pushing; verify
  the structured error mentions the failing gate.
- PR-body rendering is a pure-function test on its own — assert
  on substring/structure (the orchestration-state exclusion
  paragraph, the convergence-pattern section, etc.).

## Where to look

- 4wdb's renderer module and its br port:
  `src/contexts/bead_workflow/project_prompt.rs`.
- d31l's command: `src/cli/bead.rs` and its create_project module
  for the existing CLI shape.
- Existing project create/run subcommands for CLI plumbing
  conventions: `src/cli/project.rs`, `src/cli/run.rs`.
- Tests: `tests/unit/bead_*_test.rs` for the test patterns this
  bead's tests should follow.

## Out of scope

- Watching the PR for CI/bot reactions or merging. That's bead
  `itn1` (`ralph pr watch`).
- Filing a bead on gate failure. That's bead `gj74` (failure-mode
  policies).
- Loop orchestration. That's bead `vl8z` (`ralph drain`).
- The orchestration-state exclusion boilerplate is owned by 4wdb;
  reuse the same constant rather than duplicating.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy --locked -- -D warnings`,
  `cargo fmt --check` all pass.
- Each failure mode listed above has at least one test.
- Git/gh interactions go through a mockable port; tests do not
  shell out to real binaries.
- The rendered PR body includes the orchestration-state exclusion
  block sourced from the same constant 4wdb uses.

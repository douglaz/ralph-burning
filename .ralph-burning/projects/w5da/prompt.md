# Work Item: ralph-burning-w5da - vl8z: drain doesn't invoke pr open/pr watch — tries to push beads to master directly

## Goal

The ralph drain command (vl8z) was supposed to compose pr_open (2qlo) + pr_watch (itn1) into the success-path completion of a drain cycle. In practice it doesn't.

Observed in the first real-world end-to-end test on bead sr2x:

1. drain picked sr2x, marked it in_progress.
2. drain created the feature branch feat/ralph-burning-sr2x-ralph-pr-open-resolve-base-branch.
3. drain ran iterative_minimal — converged round 1, 0 amendments.
4. The run produced 352 lines of real code on the feature branch (including the actual sr2x implementation: base-branch resolver in pr_open.rs).
5. drain then switched back to master.
6. drain tried git push origin HEAD:master with only beads/ changes — blocked by master branch protection.
7. drain failed with the "GH013: repository rule violations" error and filed a follow-up bead.

The actual code changes from the run never got pushed or opened as a PR. The feature branch sits there with checkpoint commits but no PR.

Looking at src/cli/drain.rs:455, drain just does git push origin HEAD:master after a "sync beads after drain" commit. There's no integration with src/contexts/bead_workflow/pr_open.rs (2qlo) or pr_watch.rs (itn1).

The drain harness (xrwl) tests pass because they mock at a level above pr_open/pr_watch — they don't catch this gap.

Fix: replace the direct git push with calls to:
- bead_workflow::pr_open::run_pr_open (squash + push + create PR via gh)
- bead_workflow::pr_watch::watch_pr (poll CI + bot, merge on green + +1)

These already exist as library functions; vl8z just needs to invoke them.

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

# Work Item: ralph-burning-v8dm - Drain harness: recompute ready beads from persisted graph each cycle

## Goal

The drain harness in src/test_support/drain_harness.rs (added by xrwl/PR #212)
walks a fixed bead_order captured at construction time, so beads filed via
file_follow_up_bead during the harness run are never picked up by subsequent
cycles. This diverges from production br ready behavior and means
recovery-path scenarios (e.g. BackendExhaustedSkip) can pass for the wrong
reason: the follow-up bead is invisible, so the test asserts "drain
continued" without actually exercising the case where a freshly-filed
follow-up gets queued.

Fix: have the harness re-read .beads/issues.jsonl each cycle (or wrap a
mock BrAdapter whose ready output reflects writes back to the file).
The production br adapter does this naturally; the harness should mirror
that.

Bot's review on PR #212: https://github.com/douglaz/ralph-burning/pull/212#discussion_r2522932

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

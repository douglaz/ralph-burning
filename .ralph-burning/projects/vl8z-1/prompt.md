# Bead vl8z — ralph drain: top-level bead-queue drain loop

## Problem

This is the headline command for the drain-loop bead chain. Today, every
bead in the queue requires the operator to manually:

1. Sync master (`git fetch && git reset --hard origin/master`).
2. Pick the top of `br ready`.
3. Hand-write a project prompt and run `ralph project create`.
4. Mark the bead `in_progress` via `br update`.
5. Run `ralph run start`.
6. Poll the run periodically (~25 min/iter, multiple iters).
7. After completion: run gates, squash checkpoints, push, open PR.
8. Poll the PR for CI + bot reactions.
9. Merge on green + bot `+1`.
10. Sync master again, close the bead, queue follow-up beads if any.
11. Loop.

Every prior bead in this chain (4wdb, d31l, 2qlo, itn1, gj74) automated
one piece of that. This bead glues them together into a continuous drain
that runs unattended over `br ready` until the queue is empty or a stop
condition fires.

## Required behavior

Add a CLI subcommand. Natural shape: `ralph drain
[--max-cycles <n>] [--stop-on-p0]`. Defaults: no cycle cap, do stop on
P0 bead arrival.

Each cycle:

1. **Sync master.** `git fetch origin master` + `git reset --hard
   origin/master` + `nix build` sanity check. If any of these fail,
   abort the drain (something is wrong with the workspace).

2. **Pick the next bead.** Run `br ready` and take the top entry whose
   status is still `open` (a bead can have changed status between
   `br ready` and the time we look at it). If queue is empty, exit
   with `Drained{cycles}`. If a P0 bead is at the top and `--stop-on-p0`
   is set, exit with `P0Encountered{bead_id}`.

3. **Create the project from the bead.** Use the d31l command machinery
   directly — `ralph bead create-project <bead-id>` — through its
   library entry point (don't shell out).

4. **Start the run.** Same as `ralph run start`. Direct call into the
   engine, not a shell-out.

5. **Poll the run** until completed or failed. Use the existing
   `run status` machinery; no need to invent new polling.

6. **Classify the run outcome** using gj74's `classify_drain_failure`
   when the run did NOT complete cleanly. The classifier returns a
   `RecoveryAction` — match on it:
   - `Rerun` (CI flake-class) → ignore here; it's a PR-level concern,
     covered by itn1 inside the PR-watch step
   - `ResumeOnNextCycle` → call `ralph run resume` and poll again on
     the next iteration
   - `FileBead` → file the follow-up bead via `br create`, then
     decide based on the secondary action (Skip vs Abort)
   - `Abort` → exit drain with `Failed{bead_id, reason}`
   - `Skip` → close the current bead with a "skipped: {reason}" note,
     continue to the next cycle
   - `ForceComplete` → engine already handles via
     `max_completion_rounds`; no-op here

7. **On clean completion**, hand off to the success-path chain:
   - `ralph pr open` (2qlo) — runs gates, squashes, pushes, opens PR
   - `ralph pr watch` (itn1) — polls + merges

8. **After merge**, close the bead with a "Landed via PR #N" reason
   and continue to the next cycle.

## Stop conditions (configurable)

- Queue empty (br ready returns nothing) → `Drained{cycles}`
- N consecutive bead failures (default 3) → `TooManyFailures{count}`
- A P0 bead appears in the queue (default behavior; opt out with
  `--no-stop-on-p0`) → `P0Encountered{bead_id}`
- User interrupt (SIGINT) → `Interrupted{cycles, last_bead}`

## Output

- Per-cycle: a one-line summary printed to stdout. Format:
  `[cycle N] bead-id: convergence-pattern -> PR #N -> outcome (duration)`
- End-of-loop: a multi-line summary. Beads landed, failed, skipped,
  total wall time.
- Returns a structured `DrainOutcome` enum the operator (or future
  callers) can inspect.

## Reuse vs duplicate

- 4wdb / d31l / 2qlo / itn1 / gj74 are all libraries with public
  entry functions. Call them directly. **Do not shell out** to ralph
  subcommands; that would lose error fidelity and add latency.
- The drain orchestration itself is a thin glue function calling
  the existing pieces in sequence.
- The known-flake list is itn1's KNOWN_CI_FLAKES.
- The classifier is gj74's `classify_drain_failure`.
- Polling intervals follow itn1's defaults.

## Tests

- Happy path: queue with 2 mock beads, both run cleanly → drain exits
  with both merged, closing each bead.
- Empty queue → exits cleanly with `Drained{cycles=0}`.
- P0 bead in queue → exits with `P0Encountered`.
- N consecutive failures → exits with `TooManyFailures`.
- Run failure classified as `ResumeOnNextCycle` → resume invoked, drain
  continues.
- Run failure classified as `Skip` → bead marked closed-with-reason,
  drain continues.

Tests should mock the underlying ports (br, run engine, pr tools) so
the drain logic runs in isolation. No real shell-outs.

## Where to look

- All five preceding drain modules:
  - `src/contexts/bead_workflow/{project_prompt,create_project,
    pr_open,pr_watch,drain_failure}.rs`
- Existing CLI dispatch: `src/cli/{bead,pr,run}.rs`
- The orchestrating-with-ralph-burning skill (operator's environment,
  not this repo) documents the manual flow this bead automates. The
  policies it documents map to gj74's classifier; reference both in
  inline comments.

## Out of scope

- Running multiple beads concurrently. The loop is sequential — one
  bead at a time. Concurrency is a future enhancement, not this bead.
- Auto-amending on bot findings. Per gj74's policy, bot findings
  abort the drain. Future work could add an `amend` action.
- Driving the planner / requirements pipeline. The drain operates on
  pre-existing beads in the graph.
- A daemon mode (`ralph drain --daemon`). One-shot for now; daemonize
  later if there's demand.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy --locked -- -D warnings`,
  `cargo fmt --check` all pass.
- Each stop condition has at least one test exercising the exit
  path.
- Each `RecoveryAction` variant has at least one test exercising
  the drain's response.
- The drain command does NOT shell out to ralph subcommands; it
  calls the library functions directly.
- The known-flake list and classifier are sourced from itn1 and
  gj74 respectively (no duplicates).

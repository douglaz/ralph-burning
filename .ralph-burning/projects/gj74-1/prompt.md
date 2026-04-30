# Bead gj74 — failure-mode policies + recovery for ralph drain

## Problem

The drain loop's pieces (4wdb, d31l, 2qlo, itn1) cover the happy path
end-to-end. But each piece has failure modes — gates fail, bot rejects,
CI flakes, runs get interrupted — and today nothing decides what to do
when those happen. This bead fills the gap with **deterministic
failure-mode policies** so `ralph drain` (vl8z) is decidable, not
heuristic.

The output of this bead is twofold:

1. A small piece of code that takes a failure observation and returns
   a structured `RecoveryAction` — `{rerun, file_bead, abort, skip,
   resume}`. Callers (drain loop, future operators) match on the action.
2. A documented policy in `docs/` mapping each failure mode to its
   recovery action with rationale.

## Required policies

For each failure mode below, decide the action and document the
reasoning. The policy code is the source of truth; the docs link to it.

| Failure mode | Action | Reasoning |
|---|---|---|
| CI flake (test in known-flake list) | rerun once per PR | flakes resolve on retry; cap at one rerun to avoid masking real regressions |
| CI permanent failure (real test/build error) | file bead + abort drain | failure needs human attention; drain shouldn't paper over it |
| Bot line-comments after latest push | abort drain (no auto-amend) | findings need human judgment; drain doesn't try to outsmart the bot |
| Bot `-1` reaction | abort drain | explicit rejection signal |
| Amendment oscillation (counts not converging for N rounds) | force-complete the run; ship | the engine already supports `max_completion_rounds`; drain treats this as a normal completion |
| Same bead failed twice in a row (e.g., backend exhaustion two cycles running) | abort drain + file bead | persistent failure on one bead suggests a structural issue worth inspecting |
| Run interrupted (orchestrator process died) | run stop + run resume on next cycle | resume preserves checkpoint state and is the documented recovery path |
| Backend exhausted (BackendExhausted, non-retryable per existing classifier) | file bead + skip this bead, continue drain | one bead's backend exhaustion shouldn't kill the whole drain; capture context for later |

## Required behavior

1. **Add a classifier function** with the contract:
   ```
   pub fn classify_drain_failure(observation: &FailureObservation) -> RecoveryAction
   ```
   `FailureObservation` carries enough context to discriminate (failing
   test names, bot line-comments, run outcome, retry count, etc.).
   `RecoveryAction` is the structured enum above.

2. **Reuse, don't duplicate.** itn1's known-flake list and 2qlo's gate
   classifications are the authoritative sources. Do NOT redefine them.
   The classifier composes those facts.

3. **The classifier is a pure function** of the observation. Tests can
   build observations directly.

4. **Document the policy** in a single markdown file under `docs/` (or
   wherever the project keeps similar references). The doc should:
   - State each policy in the table above
   - Link to the test cases that exercise it
   - Note that the engine's existing `max_completion_rounds` cap is
     the mechanism for amendment oscillation force-complete

## Tests

- One test per failure mode in the policy table — assert the
  classifier returns the right action for a representative
  observation.
- Tests should also verify the failure-mode coverage is **exhaustive**:
  add a compile-time check (e.g., `match observation.kind` over all
  variants of an enum) so the next failure mode added requires a
  policy decision.

## Where to look

- Drain loop chain modules:
  - `src/contexts/bead_workflow/project_prompt.rs` (4wdb)
  - `src/contexts/bead_workflow/create_project.rs` (d31l)
  - `src/contexts/bead_workflow/pr_open.rs` (2qlo)
  - `src/contexts/bead_workflow/pr_watch.rs` (itn1) — `WatchOutcome`
    enum lives here, useful as a partial input shape for the
    classifier.
- Engine's existing failure handling:
  - `src/contexts/workflow_composition/engine.rs` for
    `max_completion_rounds` semantics
  - `src/adapters/process_backend.rs` for `BackendExhausted`
    classification
- The orchestrating-with-ralph-burning skill (in the operator's
  environment, not this repo) documents the manual policies in
  prose. Replicate the relevant decisions in inline comments and
  the new `docs/` file so the code is self-documenting.

## Out of scope

- The drain loop itself. That's `vl8z`. This bead supplies the
  classifier `vl8z` will call.
- Auto-amending. The classifier returns `abort` for bot findings;
  the operator decides whether to amend. A future bead could add
  `amend` as an action with a sub-policy.
- Per-class retry budgets beyond "one rerun for known flakes."
  Keep it simple; expand later if observations show stalling.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy --locked -- -D warnings`,
  `cargo fmt --check` all pass.
- The classifier is a pure function with the contract above.
- Each failure mode has a test exercising it; the test set is
  exhaustive (a compile-time check forces a policy decision when
  a new failure-mode variant is added).
- The docs/ policy file links to the classifier and to the test
  cases.

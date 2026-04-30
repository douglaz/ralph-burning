# Bead 4wdb — render project prompt from bead id (drain loop foundation)

## Problem

Today, every time the operator drives a bead through ralph-burning,
they hand-write a `prompt.md` for the project: the goal, the acceptance
criteria, what files to look at, the orchestration-state exclusion
boilerplate, etc. This is the part of the orchestration glue that runs
on every single bead, and it's currently 100% manual.

This bead lays the foundation for `ralph drain` by replacing the hand-
written prompt with a deterministic renderer that takes a bead id
(plus the live `br` graph) and produces a complete project prompt.

The renderer is the foundation — `ralph bead create-project` (next
bead, d31l) and `ralph drain` (vl8z) both depend on it.

## Required behavior

Add a pure function — call it `render_project_prompt_from_bead` or
similar — with this contract:

**Input:**
- A bead id (e.g., `ralph-burning-4wdb`).
- A handle on the `br` adapter (the existing port in
  `src/adapters/br_process.rs` that knows how to call `br show`,
  `br dep tree`, etc.). The renderer must be testable without
  invoking real `br` — accept the port as a trait so a mock can be
  injected.
- The repository root (`PathBuf`) so the renderer can include
  pointers to `AGENTS.md` / `CLAUDE.md`.

**Output:**
- A complete prompt string ready to feed to `project create
  --prompt`. The exact structure should follow the template the
  operator has been hand-writing in this codebase. The prompt
  must include:

  1. A header naming the bead and stating it as the work item.
  2. The bead's goal/description (from `br show <id>`'s
     description body) — verbatim, since beads carry the canonical
     statement of intent.
  3. The bead's structured acceptance criteria field if non-empty
     (the field after `## Acceptance Criteria`).
  4. **Nearby-graph context.** From `br dep tree` or equivalent:
     - Parent (parent-child relationship) — id and title only.
     - Direct blockers (the beads this one depends on), separated
       by closed/open status — closed blockers are factual context
       ("the milestone-bundle path was wired by closed bead X"),
       open blockers should fail the renderer with a clear error
       (a bead with open blockers shouldn't be picked up by drain).
     - Direct dependents (beads that depend on this one) — id and
       title only, useful for the implementer to understand
       downstream impact.
  5. A short "where to look" section with file/path pointers if
     the bead description contains them. If the bead description
     does not have explicit pointers, omit this section rather
     than guessing.
  6. The orchestration-state exclusion boilerplate verbatim — the
     paragraph asking reviewers not to flag `.ralph-burning/` files
     as findings. This text is the same on every prompt; centralize
     it as a constant in the renderer.
  7. A standard footer pointing to `AGENTS.md` and `CLAUDE.md` as
     repo norms references (only if these files exist in the repo
     root).

**Failure modes (return `Err` with the listed variant):**
- Bead not found.
- Bead status is closed → renderer refuses to render (drain shouldn't
  reopen closed work).
- Bead has open blockers → renderer refuses, lists them in the error
  for the caller's diagnostic.

## Tests

The renderer must be unit-testable without `br` actually running.
That means:

- Define a small trait or struct alias representing the subset of
  `br` operations the renderer needs (probably `bead_show`,
  `bead_dep_tree`). If the existing `BrAdapter` already implements
  this surface, use it; if it requires a port refactor for testability,
  do the minimum extraction needed.
- Add tests that exercise:
  - Happy path: bead with goal, acceptance criteria, parent,
    closed blockers, dependents → prompt contains all sections in
    expected order.
  - Bead with no acceptance criteria → that section is omitted, not
    rendered as empty.
  - Bead with no nearby graph (no parent, no blockers, no
    dependents) → prompt still includes the orchestration-state
    boilerplate and is well-formed.
  - Bead with open blockers → renderer returns an error listing the
    open blockers.
  - Closed bead → renderer returns an error.
- Snapshot or substring assertions are both acceptable; prefer
  substring assertions on the boilerplate (since it's stable) and
  structural assertions on the variable parts.

## Where to look

- `src/adapters/br_process.rs` — the `br` port. Look at what's
  already exposed (probably `BrAdapter::show`, `BrAdapter::ready`,
  etc.). Add a method for the dep-tree call if missing.
- `src/contexts/` — pick the right home for the renderer. It's
  workflow-adjacent. Likely candidates: a new module under
  `src/contexts/workflow_composition/` or a new sibling
  `src/contexts/bead_workflow/`. Use whatever fits the project's
  hexagonal layering.
- `tests/unit/` — co-locate the renderer tests with whichever
  context module hosts the renderer.
- The hand-written prompts the operator has been using are
  illustrative: see the recent merged PRs (#201, #202, #203, #204)
  for the structural pattern.

## Out of scope

- The CLI command (`ralph bead create-project`) that calls this
  renderer. That's bead `d31l` and depends on this one.
- Hooking the renderer into `project create`. The renderer is a
  pure function; wiring it up to a CLI entry point is a separate
  bead.
- Caching, idempotency, or any state. The renderer is a function,
  not a service.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`
  all pass.
- The renderer is a pure function with the contract above.
- Renderer tests cover happy path + each failure mode + each
  edge case listed under "Tests".
- The orchestration-state exclusion boilerplate is centralized as
  a single constant in the renderer module — not duplicated in
  callers.

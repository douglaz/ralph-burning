# ralph-burning-9po — Write a quality README.md for the project

## Problem description

There is no `README.md` at the repo root. New users (operators, contributors,
packagers) have no entry point — they have to grep `AGENTS.md`, the `src/`
tree, or hunt through closed PRs to figure out what ralph-burning even is.
Fix this by creating a clear, accurate, well-scoped `README.md`.

## Required content (in order)

1. **Tagline + one-paragraph elevator pitch.** What ralph-burning does in
   plain English, e.g. "ralph-burning is an AI-assisted, multi-stage
   code-execution orchestrator. It takes a problem prompt, plans an
   implementation, has agents iteratively code + review the change, and
   tracks every cycle in durable state so a run can resume after a crash."
   Tune to what's actually true.

2. **Why it exists.** What problem class it solves vs. running an LLM CLI
   ad hoc: durable state, rollback points, multi-reviewer panels with
   arbiter, `.beads` integration as the canonical work graph. Mention the
   `iterative_minimal` flow (plan_and_implement + final_review) as the
   most-used flow.

3. **Quick start.** Minimum required commands:
   - install: `nix run github:douglaz/ralph-burning -- --help` (or local cargo build)
   - init: `ralph-burning init` then `backend check`
   - first run: `ralph-burning project bootstrap --idea ...` followed by
     `run start`
   - monitor: `run status`, `run tail --follow`
   - resume: `run resume` after any failure

4. **Concept cheat sheet.** One-liner each for: project, run, cycle,
   completion round, stage, bead, milestone, flow preset, panel,
   amendment, classification, rollback point. Link to deeper docs/AGENTS
   files where they exist.

5. **Flows.** Quick table: name → stages → when to pick. The 6 presets
   (`minimal`, `iterative_minimal`, `quick_dev`, `standard`, `docs_change`,
   `ci_improvement`) — `docs_change` is now a Minimal alias; keep it
   listed for UX recognizability.

6. **Backend support.** Codex, Claude, OpenRouter, stub (test-only).
   Mention `BackendExhausted` graceful degradation.

7. **Bead-driven workflow.** How beads/`.beads`/`br` integrate: project
   create-from-bead, milestone bundles, classification routing in
   `final_review`, automatic propose_new_bead creation. Reference 9ni.4.x
   and 9ni.7.2/9ni.8.5/9ni.8.6 as the implementation lineage.

8. **Configuration.** Where `workspace.toml` lives, common knobs
   (`workflow.max_completion_rounds`, `workflow.stable_rounds_required`,
   `workflow.parsimonious_bead_creation`), per-project overrides at
   `.ralph-burning/projects/<id>/config.toml`.

9. **Development.** `nix develop -c cargo test --features test-stub`;
   `cargo clippy --locked -- -D warnings`; `cargo fmt --check`; `nix build`
   is the authoritative gate.

10. **License + contributing pointer.** Link to `AGENTS.md`, `CLAUDE.md`,
    and the beads-rust project where relevant.

## Style / voice

- Concise. Top section under one screen.
- Use short code blocks, not screenshots.
- Sentence case headers, no emojis.
- Link to source files (with line refs where useful) rather than duplicating.
- Don't oversell — describe what ships today, not aspirations.

## Implementation hints

- Read `AGENTS.md` first for vocabulary and project layout.
- Read `src/cli/run.rs` and `src/cli/project.rs` for the actual CLI surface.
- Read `src/contexts/workflow_composition/contracts.rs` for the family
  taxonomy (Planning / Execution / Validation).
- Read `src/contexts/workflow_composition/mod.rs` for the flow preset
  definitions.
- Read `src/contexts/workspace_governance/config.rs` for the config
  surface (parsimonious_bead_creation, max_completion_rounds, etc.).
- Verify quick-start commands work end-to-end before merging — the
  README's value comes from accuracy.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged. Only review the new `README.md` plus any small
adjustments to `AGENTS.md`, `CLAUDE.md`, or repo-root files.

## Acceptance criteria

- `README.md` exists at repo root.
- Covers all 10 sections above (sections may be combined for brevity, but
  every concept must appear).
- Quick start commands work as written (verify by running them).
- `nix build` passes on the final tree (authoritative gate).
- `cargo test --features test-stub --locked && cargo clippy --locked -- -D warnings && cargo fmt --check` pass.

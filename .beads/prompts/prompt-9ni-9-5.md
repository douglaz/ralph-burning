# Bead 9ni.9.5: Update CLI help text for milestone/task vocabulary

## Problem description

`milestone` and `task` subcommands currently have minimal `--help` text
(e.g. bare `Usage: ralph-burning milestone <COMMAND>` with no per-command
descriptions). Operators invoking `ralph-burning milestone --help` or
`ralph-burning task --help` get near-empty output instead of short,
actionable help.

Goal: every milestone and task subcommand has a short description (clap
`#[command(about = "...")]`) and, where useful, a brief example in the
`long_about`. Keep it concise — no walls of text.

## Required changes

Edit the clap definitions in `src/cli/*.rs` (typically
`src/cli/milestone.rs` and `src/cli/task.rs`, possibly `src/cli/mod.rs`
for the top-level dispatch).

For each subcommand, add:
- `about = "<one-line description>"` (1 sentence, imperative)
- `long_about = "<short explanation + example>"` where the one-line
  description is insufficient. Keep `long_about` to ≤ 5 lines total.

Commands to cover (names may differ slightly — check the actual
`Subcommand` enum):
- `milestone create` — "Create a milestone record from a name or idea."
- `milestone plan` — "Plan a milestone's bead graph via the requirements pipeline."
- `milestone show` — "Show milestone details and bead summary."
- `milestone status` — "Show milestone progress (completed / in-progress / blocked / remaining)."
- `milestone next` — "Report the next actionable bead for the active milestone."
- `milestone run` — "Start the next actionable bead as a task run."
- `milestone export-beads` — "Export a milestone's bead graph to the beads store."
- `milestone bead-history` — "Show history of bead-backed runs for a milestone."
- `milestone tasks` — "List tasks (bead-backed projects) for a milestone."
- `task create` — "Create a task (project) from a bead."
- `task show` — "Show task details including milestone and bead lineage."
- `task select` — "Select a task as the active project."
- `task list` — "List tasks (bead-backed projects) in the workspace."

Vocabulary rules:
- **milestone** — a scoped unit of work (bundle of beads + plan).
- **bead** — a single actionable issue tracked in the beads graph.
- **task** — a project that was created from a bead (i.e., a Ralph run
  executing a bead).
- **project** — internal substrate backing a task; avoid in help text
  unless a command truly targets non-bead projects (e.g., legacy
  `project bootstrap`).

Use these consistently. Do NOT refer to a task as "project" or vice
versa in the new help text.

Examples (use realistic stand-ins, not foo/bar):
- milestone: `ms-alpha-plan`, `ms-dogfood`
- bead: `ralph-burning-9ni.4.1`, `ralph-burning-xyz`
- flow: `minimal`, `iterative_minimal`

## Scope guard

- Do NOT rename commands.
- Do NOT change subcommand structure.
- Do NOT touch `project` subcommands' help (they already print a
  deprecation notice).
- Do NOT add new commands.
- Do NOT refactor unrelated help text (e.g., `run`, `backend`,
  `config`) unless the refactor is a 1-line terminology fix to use
  "task" instead of "project" in a help string.
- README updates: one section for milestone+task workflow is fine, but
  do not rewrite unrelated README sections.

## Tests

Clap `about`/`long_about` changes do not need unit tests. If an
existing test checks help output (e.g. `tests/cli.rs` snapshot), update
the snapshot to match. Otherwise add no new tests.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged.

## Acceptance criteria

- Every milestone subcommand has `about = ...` with a concise one-line
  description.
- Every task subcommand has `about = ...`.
- Vocabulary (milestone / bead / task) is consistent across the help
  text.
- `ralph-burning milestone --help` and `ralph-burning task --help`
  surface the new descriptions.
- `nix build` passes.
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.

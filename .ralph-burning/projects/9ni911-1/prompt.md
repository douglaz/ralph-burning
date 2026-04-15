# Add milestone create/plan/show/status command handlers

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Goal

Implement CLI command handlers for milestone management:

1. `milestone create <name> [--from-idea <text>]` — Create a new milestone record
2. `milestone plan <milestone_id>` — Trigger requirements pipeline in milestone mode
3. `milestone show <milestone_id>` — Display milestone metadata, status, bead count, progress
4. `milestone status [milestone_id]` — List all milestones or show detailed status for one

## Implementation hints

- Check existing CLI structure in `src/cli/` — follow the pattern of `run.rs` and `project.rs`
- The `milestone` subcommand may already be partially wired up — search for `milestone` in `src/cli/`
- Use `clap` for argument parsing, matching the existing patterns
- Milestone storage is in `src/contexts/milestone_record/` — use the existing service/store ports
- Support `--json` flag for structured output on show/status commands
- All output should reflect real milestone state, not cached data
- Error messages should reference milestone_id and explain what went wrong

## Acceptance Criteria
- All 4 commands work: create, plan, show, status
- --json flag supported on show and status
- Error handling with clear messages
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

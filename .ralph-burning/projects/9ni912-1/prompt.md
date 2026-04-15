# Add milestone next/run commands and active milestone selection

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## Goal

Add execution-facing CLI commands for milestone-driven bead work:

1. `milestone next [milestone_id]` — Query for recommended next bead, show title/priority/readiness
2. `milestone run [milestone_id]` — Start/resume sequential bead execution via controller

Plus active milestone selection: if milestone_id is omitted, use most recently active milestone.

## Implementation hints

- Build on the milestone CLI infrastructure from 9ni.9.1.1 (just merged in `src/cli/milestone.rs`)
- Follow the same patterns: clap subcommands, --json flag, error handling
- `milestone next` needs to query the bead graph for ready work — check how `br ready` works or use the bead adapter
- `milestone run` needs the milestone controller — check `src/contexts/milestone_record/` and `src/contexts/automation_runtime/` for controller logic
- Active milestone selection: store last-used milestone in workspace state or infer from project metadata

## Acceptance Criteria
- Both commands work: next and run
- --json flag supported
- Active milestone selection when id omitted
- Clear status codes and messages for success/blocked/needs_operator
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

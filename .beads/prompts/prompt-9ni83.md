## Bead ID: ralph-burning-9ni.8.3

## Goal

Claim a bead and create a corresponding Ralph task. Turn a selected bead into an owned task run by claiming it in br and creating the corresponding Ralph project/task.

## Context

The controller state model (9ni.8.1) and bead selection (9ni.8.2) are already implemented. After `select_next_milestone_bead` transitions the controller to `claimed` with a `bead_id`, this bead implements the logic that actually claims the bead in `br` and creates the Ralph project/task.

The claiming flow should:
1. Transition controller from `claimed` to `running` (the selection step already set it to `claimed`)
2. Claim the bead via `br update <bead_id> --status=in_progress`
3. Create a Ralph project from the bead context using `create_project_from_bead_context` (already exists in `src/contexts/project_run_record/service.rs`)
4. Record the linked task/project ID in the controller via `sync_controller_task_claimed`
5. If br claim fails (bead already claimed, tool error), do not create the Ralph task — transition to `needs_operator`

The `prepare_milestone_controller_for_execution` function in `src/cli/run.rs` already handles the controller claimed→running transition and project adoption. The main gap is wiring the `br update --status=in_progress` claim step and handling its failure before project creation proceeds.

## Acceptance Criteria

- `br update <bead_id> --status=in_progress` is called explicitly before task creation
- A Ralph project is created from the bead-backed prompt with correct milestone and bead metadata in `task_source`
- Milestone controller state records the claim and linked task_id
- Claim failure (br tool error) prevents task creation and transitions to `needs_operator`
- The claimed bead no longer appears as ready in `bv` triage
- Deterministic tests cover successful claim, claim failure, and integration with the existing controller flow
- Existing tests pass

## Non-Goals

- Bead selection (covered by 9ni.8.2)
- Task execution / running the engine (covered by 9ni.8.4)
- Prompt generation internals (covered by 9ni.6.1/6.2)

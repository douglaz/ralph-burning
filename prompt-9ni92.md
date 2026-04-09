## Bead ID: ralph-burning-9ni.9.2

## Goal

Add task aliases/wrappers around the current project commands while keeping the internal project substrate intact. Let users talk about "tasks" while the implementation still uses project services underneath.

## Context

Ralph-burning's internal model uses "projects" as the unit of work. But in the milestone/bead execution context, users think in terms of "tasks" — a task is a project that was created from a bead context. The CLI should support `task` as a command namespace that maps to the relevant project operations.

The existing project commands that matter for task workflows:
- `project create-from-bead` → should be accessible as `task create`
- `project show` → should be accessible as `task show`
- `project select` → should be accessible as `task select`
- `project list` → should be accessible as `task list`

## Acceptance Criteria

- A `task` top-level command exists in the CLI with subcommands that wrap the relevant project operations
- `task create` wraps `project create-from-bead` (the bead-backed task creation path)
- `task show`, `task select`, `task list` wrap their project equivalents
- Output from task commands explains the task↔project mapping where useful (e.g., "Task 'foo' (project 'foo')")
- All existing `project` commands continue to work unchanged (backward compatibility)
- Help text for task commands is clear about what they do
- Deterministic tests verify task aliases route to the same behavior as their project counterparts
- Existing tests pass

## Non-Goals

- Changing the internal storage model from projects to tasks
- Renaming internal service/domain types
- Adding new task-specific functionality beyond aliasing

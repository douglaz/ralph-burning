# Keep project commands as compatibility aliases during the transition

## IMPORTANT: Exclude orchestration state from review scope
Files under `.ralph-burning/` are live orchestration state and MUST NOT be reviewed or flagged.

## IMPORTANT: This is an ADDITIVE change
Add deprecation notices to existing project commands. Do NOT remove or rename any existing commands. Do NOT change behavior.

## Background — what already exists

### CLI structure (`src/cli/mod.rs`):
- Top-level commands: Init, Flow, Config, Milestone, Project, Run, Requirements, Daemon, Backend, Conformance, Task
- `Task` already exists as an alias layer over `Project` (in `src/cli/task.rs`)
- Dispatch is via `match cli.command { Commands::Project(cmd) => project::handle(cmd).await, ... }`

### Project subcommands (`src/cli/project.rs` lines 66-75):
- `Create`, `CreateFromBead`, `Bootstrap`, `Select`, `List`, `Show`, `Delete`, `Amend`

### Milestone subcommands (`src/cli/milestone.rs` lines 56-93):
- `Create`, `Plan`, `Next`, `Run`, `Show`, `BeadHistory`, `Status`, `Tasks`

### Task subcommands (`src/cli/task.rs` lines 25-41):
- `Create` (wraps project create-from-bead), `Show`, `Select`, `List`

### No deprecation infrastructure exists yet.

## What to implement

### 1. Add deprecation notice helper

In `src/cli/project.rs` or a shared location:
- Add a function `print_deprecation_notice(old_command: &str, new_command: &str)` that prints to stderr:
  ```
  Note: `ralph-burning project <old>` is deprecated. Use `ralph-burning <new>` instead.
  ```
- Print to stderr so piped stdout is unaffected

### 2. Add deprecation notices to project command handlers

In `src/cli/project.rs`, at the top of each handler function, call the deprecation notice with the appropriate mapping:

| Project Command | Suggested Replacement |
|---|---|
| `project create` | `task create` or `milestone create` (depending on context) |
| `project create-from-bead` | `task create` |
| `project bootstrap` | `task create --from-idea` (or keep as-is if no equivalent) |
| `project select` | `task select` |
| `project list` | `task list` |
| `project show` | `task show` |
| `project delete` | (no equivalent yet — note this in the deprecation message) |

Only add notices for commands that have a clear equivalent. For commands without a replacement (like `delete`), skip the notice or add a softer message like "This command will move to `task delete` in a future release."

### 3. Update --help text

In the clap derive annotations for `ProjectCommand`:
- Add `#[command(about = "Project commands (deprecated — use 'milestone' and 'task' instead)")]`
- Add `(alias)` notes in the help text for individual subcommands where equivalents exist

### 4. Add tests

Add tests that verify:
- Deprecation notice is printed to stderr (not stdout)
- Project commands still work (behavior unchanged)
- Help text mentions the deprecation

## Implementation guidance

- The deprecation notice should be a simple `eprintln!` — no need for a complex framework
- Keep it minimal: one line per command, printed at the start of the handler
- Don't add deprecation notices to internal/programmatic uses — only to the CLI entry points
- The `Task` command should NOT print deprecation notices (it's the new vocabulary)

## Files to modify

- `src/cli/project.rs` — add deprecation notices to handlers, update help text
- Possibly `src/cli/mod.rs` — update Project command description

## Acceptance Criteria
- All existing project commands continue to work with identical behavior
- Deprecation notices print to stderr (not stdout)
- Help text indicates project commands are deprecated
- All existing tests still pass
- cargo test && cargo clippy -- -D warnings && cargo fmt --check pass
- nix build passes on the final tree

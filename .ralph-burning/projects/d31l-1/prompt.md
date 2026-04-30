# Bead d31l — ralph bead create-project <bead-id> (drain loop)

## Problem

The bead-prompt renderer landed in 4wdb (PR #205). What's missing is a CLI
surface that composes:
1. Looking up the bead by id (br adapter call).
2. Rendering the project prompt (using `bead_workflow::project_prompt`).
3. Calling project create with the rendered prompt and auto-derived id/name.
4. Marking the bead `in_progress` so the rest of the drain loop knows it's
   being worked on.

This is the second piece of the drain-loop chain. After this lands,
operators (and `ralph drain`) can go from a bead-id to a ready-to-execute
project in one command instead of the manual hand-write-prompt sequence.

## Required behavior

Add a new CLI subcommand. The natural shape is `ralph bead create-project
<bead-id> [--branch <name>] [--flow <preset>]`. Use `ralph task create
--from-bead <id>` if it fits the existing CLI vocabulary better
(`project create` is being deprecated in favor of `task create`/
`milestone create` per the deprecation warnings already in the code).

The command:
1. Calls the bead-prompt renderer from 4wdb. The renderer already returns
   a structured error for closed/blocked beads — surface that error
   verbatim (do not silently overwrite or skip).
2. Derives a project id from the bead id. Ralph project ids have a
   particular shape (alphanumeric + hyphens, see `ProjectId::new`); take
   the bead suffix (e.g., `d31l`) and use it directly as the id, or
   suffix `-1` if a project with that id already exists. Do not silently
   overwrite an existing project — fail with a pointer to
   `ralph run resume` instead.
3. Optionally creates a feature branch (`feat/<bead-id>-<short-slug>`).
   Only create the branch if `--branch` is passed (or a config flag
   says so). Default is "no branch creation" so callers can choose.
   The slug should be derived from the bead title using the existing
   slugification helper if one exists, or a simple
   lowercase-and-hyphen-collapse if not.
4. Calls the existing project-create code path (`service::create_project`
   or whatever `ralph project create` uses today). Reuse — do not
   duplicate the project-create logic.
5. Marks the bead `in_progress` via the br adapter. Use the same
   adapter used by 4wdb's renderer.

The command does NOT start the run. That's a separate command
(`ralph run start` or `ralph drain`). Keeping create and start separate
preserves the composable surface and matches the existing project flow.

## Failure modes

Pass each of these through with a clear error message:
- Bead not found.
- Bead status is closed (renderer-returned error).
- Bead has open blockers (renderer-returned error; list them in the
  user-facing message).
- A project with the derived id already exists — point to
  `ralph run resume`.
- The br update to `in_progress` fails — the project is already
  created at this point; surface the partial-success state explicitly
  ("project created but bead status update failed; run br update
  manually").

## Tests

- Happy path: existing open bead with no blockers → project is created,
  prompt content matches the renderer output, bead is now in_progress.
- Closed bead → command fails, no project created.
- Bead with open blockers → command fails, error lists the blockers.
- Project id collision → command fails with a resume pointer.
- (If feasible) br update failure mid-flight → command surfaces partial
  state.

Tests should use a mocked br port (the same trait surface 4wdb's tests
use) so they don't shell out to a real `br` binary.

## Where to look

- 4wdb's renderer module: `src/contexts/bead_workflow/project_prompt.rs`
  (and the trait it uses to abstract br).
- Existing project-create CLI handler: `src/cli/project.rs`
  (`handle_create` / `handle_create_from_requirements`). The new command
  should mirror its shape but take a bead id instead of a prompt path.
- Existing CLI dispatch: wherever `Subcommand::Project` /
  `Subcommand::Task` lives. Add the new subcommand parallel to those.
- Tests: pick a location that matches existing CLI test patterns.
  `tests/cli.rs` is the integration-test home; lib-level tests are also
  fine if a function-shape API is exposed.

## Out of scope

- The branch lifecycle automation (squash + push + PR open) is bead
  2qlo, not this one. This bead stops at "project created and bead
  marked in_progress."
- The `ralph drain` loop top-level orchestration is bead vl8z; that
  one composes this command with the rest.
- Any policy decisions about which bead to pick — this command takes
  a bead id from the user. `br ready` selection logic is in vl8z.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Acceptance criteria

- `nix build` passes on the final tree (authoritative gate).
- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check`
  all pass.
- The new command exercises the 4wdb renderer (does not re-implement
  prompt construction inline).
- Each failure mode listed above has at least one test.
- The command's CLI help output includes a clear usage example.

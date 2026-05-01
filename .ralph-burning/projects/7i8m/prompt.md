# Work Item: ralph-burning-7i8m - Add --br-path flag with preflight check; abort with informative error if br is missing

## Goal

ralph drain (and the bead-aware commands: bead create-project, pr open,
pr watch, drain) all shell out to br for bead operations. Today they
look up br via $PATH only, so running from outside `nix develop` fails
with a generic "br binary not found" message and no recovery path.

Add a top-level optional --br-path <PATH> flag that takes precedence
over $PATH, and run a preflight check at the start of any command that
needs br. The preflight either:
- finds br at the explicit --br-path if provided, OR
- finds br on $PATH (current behavior), OR
- aborts with an informative error that:
  - says which paths were tried (the explicit one if given, plus a
    summary of $PATH)
  - suggests `nix develop` for development workflows
  - suggests setting --br-path to a known location
  - mentions the github.com/Dicklesworthstone/beads_rust install URL

The preflight runs ONCE at command start, not per-call. Cache the
resolved binary path on the BrAdapter (or wherever the existing port
construction lives).

## Scope

- Add `--br-path` as a global flag on the relevant subcommand groups:
  bead, pr, drain. (Not all subcommands need it — only the ones that
  invoke br. Plumb where used.)
- Add a small preflight function in src/adapters/br_process.rs (or a
  new src/preflight/br.rs if cleaner) that returns the resolved path
  or a structured error.
- Update existing BrAdapter construction sites to use the resolved
  path.
- Tests:
  - --br-path with a valid path → adapter uses it.
  - --br-path with a non-existent path → preflight aborts with the
    explicit-path error.
  - No --br-path, br on PATH → adapter finds it.
  - No --br-path, br not on PATH → preflight aborts with the
    informative error.

## Where to look

- src/adapters/br_process.rs — existing br port construction
- src/cli/{bead,pr,drain}.rs — where the BrAdapter is built today
- src/cli/mod.rs — top-level Cli struct (clap derive)
- The error type src/shared/error.rs — add a structured BrUnavailable
  variant if not already present

## Out of scope

- Auto-installing br. The preflight reports where to find it, doesn't
  fetch it.
- Validating br version compatibility. Future bead.
- Doing the same for bv, codex, claude, gh. Future beads if those
  hit the same friction.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files.

## Acceptance Criteria

## Nearby Graph Context

- **Parent:**
  - None.
- **Closed blockers:**
  - None.
- **Direct dependents:**
  - None.

## Where to Look

- `src/`
- `tests/`
- `docs/`

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`,
`docs/`, and config files (`Cargo.toml`, `flake.nix`, etc.).

## Repository Norms

- `AGENTS.md`
- `CLAUDE.md`

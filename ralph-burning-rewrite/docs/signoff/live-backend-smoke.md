# Live Backend Smoke Runbook

This runbook describes how to validate the three live backend smoke items
(Claude, Codex, OpenRouter) required by the manual smoke matrix.

## Prerequisites

| Requirement | Check | Notes |
|-------------|-------|-------|
| `ralph-burning` binary | `cargo build` or `nix build` | Must be current branch build |
| Claude CLI | `command -v claude` | Required for Claude smoke |
| Codex CLI | `command -v codex` | Required for Codex smoke |
| `OPENROUTER_API_KEY` | `test -n "$OPENROUTER_API_KEY"` | Required for OpenRouter smoke |
| Network access | curl to backend endpoints | Backends need outbound HTTPS |

## Workspace Isolation

Each smoke run creates a scratch directory (`/tmp/rb-smoke-$PID`) and **`cd`s
into it** before executing any CLI commands.  The CLI resolves workspace root
from `current_dir()` (`src/cli/project.rs:217`, `src/cli/run.rs:130`,
`src/cli/backend.rs:310`), so running inside the scratch directory guarantees:

- A fresh `.ralph-burning/workspace.toml` is written inside the scratch dir
- `project bootstrap` persists the active project inside `$SMOKE_DIR/.ralph-burning/`
- No existing workspace config, active-project selection, or checked-in state
  in the real repo is read or mutated

The script initialises the scratch workspace with a `workspace.toml` that sets
`settings.default_backend` to the backend under test (e.g. `"claude"`, `"codex"`,
or `"openrouter"`).  This ensures that `project bootstrap`, which runs quick
requirements internally, resolves its backend from `default_backend()` in
`service.rs:23` → `config.rs:376` using the correct backend — not the ambient
fallback (`DEFAULT_BASE_BACKEND = Claude` at `config.rs:37`).

### Single-Backend Role Overrides

The standard flow normally requires multiple backend families (e.g. Claude for
planner/reviewer, Codex for implementer/qa/completer).  For smoke testing a
single backend end-to-end, the scratch `workspace.toml` overrides ALL roles
and panels to use only the backend under test:

- `workflow.implementer_backend` / `workflow.qa_backend` (or `planner`/`reviewer` for non-Claude)
- `completion.backends` — set to `["<backend>", "<backend>"]`
- `final_review.backends` and `final_review.arbiter_backend`
- `prompt_review.refiner_backend` and `prompt_review.validator_backends`

This prevents `backend check` from failing on unavailable opposite-family
backends and ensures every invocation in the run exercises the backend under test.

## Backend Binding

The script explicitly binds the backend under test at every CLI phase:

- **`backend check --backend <name>`** — validates the specific backend
- **`backend probe --backend <name>`** — resolves against the specific backend
- **`run start --backend <name>`** — executes using the specific backend

For **OpenRouter**, the script additionally:
- Writes `[backends.openrouter] enabled = true` in the scratch workspace config
- Sets `execution.mode = "direct"` (OpenRouter does not support tmux transport)
- Exports `RALPH_BURNING_BACKEND=openrouter` so that `agent_execution_builder`
  selects the `OpenRouterBackendAdapter` instead of the default `ProcessBackendAdapter`
  (which rejects OpenRouter targets at `process_backend.rs:468`)

## Running the Smoke Script

```bash
# Claude backend
./scripts/live-backend-smoke.sh claude

# Codex backend
./scripts/live-backend-smoke.sh codex

# OpenRouter backend (requires OPENROUTER_API_KEY)
OPENROUTER_API_KEY=sk-or-... ./scripts/live-backend-smoke.sh openrouter
```

### Environment Overrides

| Variable | Default | Purpose |
|----------|---------|---------|
| `RALPH_BURNING` | `cargo run --manifest-path .../Cargo.toml --` | Path/command for ralph-burning binary |
| `SMOKE_DIR` | `/tmp/rb-smoke-$$` | Scratch directory (becomes CWD for CLI) |
| `OPENROUTER_API_KEY` | (none) | API key for OpenRouter smoke |

## Backend-Specific Commands

### Claude

1. **Preflight**: `command -v claude` + `backend check --backend claude`
2. **Probe**: `backend probe --role planner --flow standard --backend claude`
3. **Bootstrap**: `project bootstrap --idea "..." --flow standard` (from scratch CWD; `settings.default_backend = "claude"` in scratch `workspace.toml`)
4. **Run**: `run start --backend claude`

### Codex

1. **Preflight**: `command -v codex` + `backend check --backend codex`
2. **Probe**: `backend probe --role planner --flow standard --backend codex`
3. **Bootstrap**: `project bootstrap --idea "..." --flow standard` (from scratch CWD; `settings.default_backend = "codex"` in scratch `workspace.toml`)
4. **Run**: `run start --backend codex`

### OpenRouter

OpenRouter has additional constraints:

1. **Preflight**: `test -n "$OPENROUTER_API_KEY"` + `backend check --backend openrouter`
2. **Config**: Scratch `workspace.toml` with `settings.default_backend = "openrouter"`,
   `[backends.openrouter] enabled = true`, and `[execution] mode = "direct"`;
   `RALPH_BURNING_BACKEND=openrouter` exported
3. **Probe**: `backend probe --role planner --flow standard --backend openrouter`
4. **Bootstrap**: `project bootstrap --idea "..." --flow standard` (from scratch CWD; `settings.default_backend = "openrouter"` in scratch `workspace.toml`)
5. **Run**: `run start --backend openrouter`

**Important**: OpenRouter must run in `execution.mode = "direct"`.  The process
adapter rejects OpenRouter targets (`process_backend.rs:468`).  The
`RALPH_BURNING_BACKEND=openrouter` env var selects the direct OpenRouter adapter
via `agent_execution_builder.rs:85`.

## Failure Recording Rules

### Preflight Failure (exit code 2)

- No project directory, active-project selection, or workspace config is mutated
- The scratch directory is removed on preflight failure, but the evidence file
  is first copied to the parent directory (e.g. `/tmp/<smoke-id>-preflight-evidence.txt`)
  so the operator can inspect the exact readiness error
- The smoke matrix row records `FAIL` with the exact preflight error from the
  preserved evidence file

### Run Failure (exit code 1)

- The created project remains valid and inspectable inside the scratch workspace
- Run state shows `failed` or `not_started` (never half-written)
- No backend override or active project selection is left in the real repo
- Durable run history remains canonical via `run history --json`
- Runtime logs are attached to that specific run only
- The smoke matrix records the exact failure, not a generic "not exercised"

### Cancellation

- If a smoke run is cancelled (Ctrl-C / SIGINT), the script propagates the
  signal.  The ralph-burning process handles cleanup (no orphan processes).
- Durable history up to the cancellation point remains inspectable.
- The evidence file captures partial results.

## Cleanup

After successful smoke:
```bash
# Evidence files are in the smoke directory
ls /tmp/rb-smoke-*/

# Remove after recording evidence in manual-smoke-matrix.md
rm -rf /tmp/rb-smoke-*
```

After failed smoke: leave the smoke directory for inspection.  The created
project (if any) is inside the scratch directory and does not affect the
real workspace.

## Recording Evidence in Sign-off Docs

After each smoke run, update `docs/signoff/manual-smoke-matrix.md`:

1. From the evidence file, extract: **project_id**, **run_id** (from the
   `run_started` journal event in `run history --json`, see `journal.rs:107`),
   **run_status** (from `run status --json`), **smoke_id**, and **smoke_dir**.
   The harness extracts these fields using `jq` when available, falling back to
   whitespace-tolerant `sed` patterns that handle the pretty-printed JSON output
   from `serde_json::to_string_pretty()` (`run.rs:764`).
2. Replace the Result column with `PASS` or `FAIL`
3. Record the project_id, run_id, run_status (must be `completed` for PASS),
   and smoke_id in the Follow-up Bug column
4. If `FAIL`, record the exact error and leave the scratch dir for inspection
5. If preflight `FAIL` (exit code 2), the evidence is preserved at
   `/tmp/<smoke-id>-preflight-evidence.txt` after scratch-dir cleanup

Once all three backend rows are `PASS` with complete evidence, update
`docs/signoff/final-validation.md` to change `Cutover status` from
`Not Ready` to `Ready`.

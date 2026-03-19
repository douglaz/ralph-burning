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

## Isolated Config Setup

Each smoke run uses an isolated scratch directory so that no workspace config,
active-project selection, or checked-in state is mutated.

```bash
# The script creates /tmp/rb-smoke-$PID as the scratch root.
# For OpenRouter, it also writes an isolated workspace.toml that:
#   - enables [backends.openrouter]
#   - forces execution.mode = "direct"
# No existing workspace.toml or project directory is touched.
```

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
| `RALPH_BURNING` | `cargo run --` | Path to ralph-burning binary |
| `SMOKE_DIR` | `/tmp/rb-smoke-$$` | Scratch directory for smoke state |
| `OPENROUTER_API_KEY` | (none) | API key for OpenRouter smoke |
| `RALPH_BURNING_WORKSPACE` | (none) | Set automatically for OpenRouter |

## Backend-Specific Commands

### Claude

1. **Preflight**: `command -v claude` + `ralph-burning backend check`
2. **Probe**: `ralph-burning backend probe --role planner --flow standard`
3. **Bootstrap**: `ralph-burning project bootstrap --idea "..." --flow standard`
4. **Run**: `ralph-burning run start`

### Codex

1. **Preflight**: `command -v codex` + `ralph-burning backend check`
2. **Probe**: `ralph-burning backend probe --role planner --flow standard`
3. **Bootstrap**: `ralph-burning project bootstrap --idea "..." --flow standard`
4. **Run**: `ralph-burning run start`

### OpenRouter

OpenRouter has additional constraints:

1. **Preflight**: `test -n "$OPENROUTER_API_KEY"` + `ralph-burning backend check`
2. **Config**: Isolated `workspace.toml` with `[backends.openrouter] enabled = true`
   and `[execution] mode = "direct"` (OpenRouter does not support tmux mode)
3. **Probe**: `ralph-burning backend probe --role planner --flow standard`
4. **Bootstrap**: `ralph-burning project bootstrap --idea "..." --flow standard`
5. **Run**: `ralph-burning run start`

**Important**: OpenRouter must run in `execution.mode = "direct"`. The tmux
adapter rejects OpenRouter targets. The smoke script enforces this via the
isolated workspace config.

## Failure Recording Rules

### Preflight Failure (exit code 2)

- No project directory, active-project selection, or workspace config is mutated
- Only the evidence file records the readiness error
- The smoke matrix row records `FAIL` with the exact preflight error

### Run Failure (exit code 1)

- The created project remains valid and inspectable
- Run state shows `failed` or `not_started` (never half-written)
- No backend override or active project selection is left in ambiguous state
- Durable run history remains canonical via `ralph-burning run history --json`
- Runtime logs are attached to that specific run only
- The smoke matrix records the exact failure, not a generic "not exercised"

### Cancellation

- If a smoke run is cancelled (Ctrl-C / SIGINT), the script propagates the
  signal. The ralph-burning process handles cleanup (no orphan processes).
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

After failed smoke: leave the smoke directory for inspection. The created
project (if any) is inside the scratch directory and does not affect the
real workspace.

## Recording Evidence in Sign-off Docs

After each smoke run, update `docs/signoff/manual-smoke-matrix.md`:

1. Replace the row's Command column with the exact command used
2. Replace the Result column with `PASS` or `FAIL`
3. Record the smoke ID and timestamp
4. If `FAIL`, record the exact error in the Follow-up Bug column

Once all three backend rows are `PASS`, update
`docs/signoff/final-validation.md` to change `Cutover status` from
`Not Ready` to `Ready`.

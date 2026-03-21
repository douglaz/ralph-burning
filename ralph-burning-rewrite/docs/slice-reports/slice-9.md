# Slice 9: OpenRouter Live Smoke Closure and Cutover Readiness

## Purpose

Close the final exit-criteria gap by rerunning the OpenRouter live backend smoke
with funded API credits, upgrading manual smoke matrix row 3 from `DEFERRED` to
`PASS`, and promoting cutover status to `Ready`.

This slice adds no new product surface. It is a sign-off closure slice.

## Legacy References Consulted

- `docs/signoff/manual-smoke-matrix.md` — current row 3 `DEFERRED` evidence
- `docs/signoff/final-validation.md` — cutover status and OpenRouter narrative
- `docs/signoff/live-backend-smoke.md` — qualifying deferred policy
- `scripts/live-backend-smoke.sh` — smoke harness (unmodified)
- `scripts/smoke-seed.json` — seed fixture (unmodified)
- `.ralph-burning/workspace.toml` — checked-in config with `[backends.openrouter] enabled = false`

## Contracts Changed

None. This slice modifies only sign-off documentation.

## Commands Run

### Loop 16 OpenRouter Smoke Rerun

```
cd ralph-burning-rewrite && ./scripts/live-backend-smoke.sh openrouter
```

**Result**: Preflight FAIL — HTTP 403 (key limit exceeded, $40/$40 total spending limit)

- **smoke_id**: `smoke-openrouter-20260320043644`
- **Exit code**: 2 (preflight failure)
- **Scratch dir**: `/tmp/rb-smoke-468939` (cleaned up by harness on preflight fail)
- **Evidence preserved**: `/tmp/smoke-openrouter-20260320043644-preflight-evidence.txt`
- **State mutation**: None — no project directory, active-project selection, or
  checked-in workspace state was created or mutated

### Prior Rerun Attempt (Loop 15)

- **smoke_id**: `smoke-openrouter-20260320042526`
- **Exit code**: 2 (same HTTP 403 blocker)
- **State mutation**: None

### Original Run (Loop 10)

- **smoke_id**: `smoke-openrouter-20260319203608`
- **run_status**: `failed` — HTTP 403 credit exhaustion during cycle 2
- **Stages completed**: All 8 standard flow stages in cycle 1 (10 successful
  backend invocations). Failure occurred during re-implementation after final
  review requested changes.

## Results

### Acceptance Criteria Status

| Criterion | Status | Notes |
|-----------|--------|-------|
| Run smoke with funded key, record `run_status = completed` | NOT MET | HTTP 403 at preflight — key credits exhausted |
| Update row 3 to `PASS` with full evidence | NOT MET | Row remains `DEFERRED` per spec guard |
| All 16 rows recorded, all live rows `PASS` | NOT MET | Row 3 is `DEFERRED` |
| Final validation cutover `Ready` | NOT MET | Remains `Not Ready` per spec guard |
| Preflight fail: no state mutation | MET | Exit code 2, no project/workspace state created |
| Mid-run fail: scratch inspectable, no overwrite | N/A | Preflight failed before run started |
| No prior slice reopened | MET | No product code or prior slice modified |
| Slice report created | MET | This document |

### Summary

The OpenRouter API key's total spending limit ($40/$40) remains exhausted. The
smoke harness correctly detects this at preflight via the credit check (HTTP 403)
and exits with code 2 without creating any project or workspace state. Per the
spec's guarding acceptance criterion, row 3 remains `DEFERRED` and cutover
remains `Not Ready`.

The sign-off docs were updated to record this latest rerun attempt without
changing any status values.

## Remaining Gap

The only remaining action to close the parity-plan exit criteria is:

1. Top up the OpenRouter API key credits or raise the spending limit at
   `https://openrouter.ai/settings/keys`
2. Rerun `./scripts/live-backend-smoke.sh openrouter`
3. On success (`run_status = completed`), update row 3 to `PASS` and promote
   cutover to `Ready`

No code changes are needed. The adapter, harness, seed fixture, and all
safeguards are validated and working.

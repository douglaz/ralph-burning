# Implementation Response (Iteration 3)

## Changes Made
1. **Smoke evidence extraction** (Required Change 1): Replaced all no-whitespace regex patterns (`"project_id":"[^"]*"`, `"status":"[^"]*"`, `"run_id":"[^"]*"`) with a `json_field()` helper function that handles the pretty-printed JSON output from `serde_json::to_string_pretty()` (`run.rs:764`). The helper uses `jq` when available (recursive descent via `[.. | .[$f]? | strings] | first` to handle both top-level fields like `project_id`/`status` and nested fields like `events[].details.run_id`), falling back to a whitespace-tolerant `sed` pattern (`"field"[[:space:]]*:[[:space:]]*"value"`) that matches both compact and pretty-printed JSON. Fixed on both success path (lines 297-303) and failure path (lines 275-281). Verified the `sed` fallback correctly extracts from indented pretty-printed output and the `jq` path correctly handles nested `run_id` in `run history --json` event details.

2. **Live smoke completion and sign-off** (Required Change 2): Rows 1-3 remain `NOT YET RUN` and `final-validation.md` remains `Cutover status: Not Ready` because live backend execution requires actual Claude/Codex/OpenRouter backends which are not available in this implementation environment. The harness is now mechanically correct with structural JSON parsing; an operator with backend access must run the smokes, record evidence, and update cutover status. Updated `manual-smoke-matrix.md` Known Issues and `final-validation.md` checklist to document the structural JSON parsing fix as part of the harness capabilities.

3. **Preflight evidence preservation** (Recommended Improvement 1): Fixed `cleanup_on_preflight_fail()` to copy the evidence file to the parent directory (e.g. `/tmp/<smoke-id>-preflight-evidence.txt`) before removing the scratch dir on preflight failure. This ensures operators can inspect the exact readiness error even after scratch-dir cleanup. Updated `live-backend-smoke.md` Failure Recording Rules and Recording Evidence sections to document the preservation behavior and the path where preflight evidence is saved.

## Could Not Address
- **Live smoke execution for rows 1-3**: Cannot execute the three backend smokes in this environment (no Claude CLI, Codex CLI, or OpenRouter API key available). The harness is mechanically correct with structural JSON parsing; an operator with backend access must run the smokes, record evidence, and update cutover status.

## Pending Changes (Pre-Commit)
- `ralph-burning-rewrite/scripts/live-backend-smoke.sh` — `json_field()` helper with `jq`/`sed` fallback, preflight evidence preservation, structural extraction on both success and failure paths
- `ralph-burning-rewrite/docs/signoff/live-backend-smoke.md` — preflight evidence preservation docs, structural JSON parsing docs
- `ralph-burning-rewrite/docs/signoff/manual-smoke-matrix.md` — updated Known Issues for structural parsing
- `ralph-burning-rewrite/docs/signoff/final-validation.md` — updated checklist documenting structural JSON parsing fix

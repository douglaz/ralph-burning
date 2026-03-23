---
artifact: quick-dev-plan-implement
loop: 1
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T19:23:33Z
---

# Preserve artifacts for `finish()`-path failures

## Amendment Closed

The final review found that `finish()` error paths — Claude parse failures and Codex read/parse failures after a zero-exit child — never called `preserve_failure_artifacts`, so debugging artifacts were lost (or actively deleted by `best_effort_cleanup`).

### Changes Made

**`src/adapters/process_backend.rs` — 5 error paths in `finish()`:**

1. **Claude invalid envelope** (line ~106): Added `self.preserve_failure_artifacts(request, &output).await` before the error return. The full raw stdout (containing the invalid JSON) is now saved to `runtime/failed/{id}.failed.raw`.

2. **Claude invalid result JSON** (line ~124): Same — preserves raw output before returning the parse error.

3. **Claude empty result** (line ~149): Same — preserves raw output before returning.

4. **Codex missing last-message** (line ~198): Replaced `best_effort_cleanup(...)` (which *deleted* the schema file) with `self.preserve_failure_artifacts(request, &output).await` (which *moves* schema + message files to `runtime/failed/` and writes raw stdout/stderr).

5. **Codex invalid last-message JSON** (line ~210): Same replacement — the invalid `last-message.json` is now preserved instead of deleted.

**`tests/unit/process_backend_test.rs` — 4 new regression tests:**

| Test | What it verifies |
|------|-----------------|
| `claude_invalid_envelope_preserves_failure_artifacts` | `runtime/failed/{id}.failed.raw` exists and contains the invalid stdout |
| `claude_invalid_result_preserves_failure_artifacts` | `runtime/failed/{id}.failed.raw` exists and contains the unparseable result |
| `codex_missing_last_message_preserves_failure_artifacts` | `runtime/failed/{id}.schema.json` and `.failed.raw` exist |
| `codex_invalid_last_message_preserves_failure_artifacts` | `runtime/failed/{id}.schema.json`, `.last-message.json`, and `.failed.raw` all exist; last-message contains the invalid payload |

### "No Amendments" section

The reviewer confirmed the rest of the implementation is correct (schema enforcement, normalization, composition recursion, existing behavior preserved). No additional changes needed — all acceptance criteria remain satisfied.

### Test results

All 92 library unit tests and 41 integration tests pass (including the 4 new tests).

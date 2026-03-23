---
artifact: quick-dev-plan-implement
loop: 1
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T18:23:09Z
---

# Fix: Close Reviewer Final Review Amendments

All three findings from the reviewer's final review have been addressed with code changes and new tests. Here's how each was closed:

## Amendment 1 — [P1] Claude debug logs persist on every successful invocation

**Root cause**: `--debug-file` was always added to Claude commands but `ResponseDecoder::Claude` didn't track the path, so `cleanup()` was a no-op.

**Fix**:
- Added `debug_file: PathBuf` to `ResponseDecoder::Claude` (line 262), stored at construction (line 501)
- `cleanup()` now deletes the debug file on success (line 58-60)
- `preserve_failure_artifacts()` now moves the debug file to `runtime/failed/` on failure (line 80-84)

**Tests**: `cleanup_claude_removes_debug_file` and `cleanup_claude_skipped_when_invocation_failed` verify both paths.

## Amendment 2 — [P2] Failure-artifact preservation drops malformed-output failures

**Root cause**: `finish()` error paths for Claude used `eprintln!` only (no artifact bundle), and Codex error paths called `best_effort_cleanup()` which *deleted* temp files instead of preserving them.

**Fix**:
- Restructured `finish()` to borrow `&self.response_decoder` instead of consuming it, enabling `self.preserve_failure_artifacts(request, &output).await` calls in all error paths
- All 5 error paths (3 Claude parse failures, 2 Codex decode failures) now write a failure bundle to `runtime/failed/` before returning the error
- `eprintln!` logging retained for real-time feedback; failure bundle provides post-mortem data

**Tests**: `finish_claude_parse_failure_writes_failure_bundle` (verifies `.failed.raw` + debug file moved) and `finish_codex_parse_failure_preserves_artifacts` (verifies schema + message + raw all preserved).

## Amendment 3 — [P2] Stale-session retry leaves failed artifacts after successful recovery

**Root cause**: `preserve_failure_artifacts()` was called immediately for the first stale-session failure (line 881), writing to `runtime/failed/` and setting the `invocation_failed` flag. If the retry succeeded, the stale artifacts remained and `cleanup()` was a no-op.

**Fix**: Removed the premature `preserve_failure_artifacts()` call from the stale-session recovery branch (line 881→replaced with comment explaining the rationale). Now:
- `prepared.cleanup()` actually runs (flag stays false), deleting the first attempt's temp files
- If the retry succeeds, no artifacts linger in `runtime/failed/`
- If the retry fails, `fresh_prepared.preserve_failure_artifacts()` handles it correctly at line 909

---
artifact: quick-dev-apply-fixes
loop: 1
iteration: 1
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T18:29:52Z
---

Line 932 also calls `finish()`, which now includes `self.cleanup().await` — so the retry success path is also fixed.

# Applied Fix: Debug Log Leak on Successful Claude Invocations

## Changes Made

**`src/adapters/process_backend.rs`** — 2 edits:

1. **Line 199**: Added `self.cleanup().await;` in the Claude success branch of `PreparedCommand::finish()`, before the `Ok(InvocationEnvelope { ... })` return. This ensures the `runtime/temp/*.claude-debug.log` file is deleted after every successful Claude run. Both call sites (`invoke()` line 949 normal success and line 932 stale-session retry success) now clean up automatically since they both go through `finish()`.

2. **New test `finish_claude_success_removes_debug_file`**: Creates a real debug file on disk, runs `finish()` with valid Claude envelope output, and asserts the debug file no longer exists afterward. Placed alongside the existing `cleanup_*` and `finish_*` tests in the `#[cfg(test)] mod tests` block.

The Codex path already had equivalent cleanup at line 245 (`best_effort_cleanup`), so only the Claude path needed fixing. Error paths were already correct — they call `preserve_failure_artifacts` (which moves the debug file to `runtime/failed/`) before returning.

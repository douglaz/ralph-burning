---
artifact: quick-dev-plan-implement
loop: 1
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T19:02:31Z
---

# Closure of Final Review Findings

## Amendment [P1]: Claude Debug Logs Persist Full Transcripts — FIXED

Removed the unconditional Claude debug-file wiring and associated failure-log preservation:

- **Removed `--debug-file` CLI arg** from `build_command` (was at line ~494-511) — Claude CLI no longer receives a debug output path.
- **Removed `debug_file` field** from `ResponseDecoder::Claude` — the enum variant only carries `session_resuming`.
- **Removed `invocation_failed` field** from `PreparedCommand` — the `AtomicBool` flag existed solely to coordinate debug-file preservation with cleanup.
- **Reverted `cleanup()` Claude arm** to no-op (was deleting the debug file).
- **Reverted `preserve_failure_artifacts` Claude arm** to no-op — only the Codex path (which pre-existed this branch) retains artifact preservation.
- **Removed `preserve_failure_artifacts` calls** from all three Claude `finish()` error paths — Claude parse failures now return errors directly without writing any artifacts to disk.
- **Removed `self.cleanup().await`** from Claude `finish()` success path — unnecessary since Claude cleanup is now a no-op.
- **Reverted Codex `finish()` error paths** from `preserve_failure_artifacts` back to `best_effort_cleanup` (original behavior: delete temp files on parse error rather than moving them to `runtime/failed/`).
- **Removed 5 tests** that exercised debug-file and preservation behavior (`finish_claude_success_removes_debug_file`, `cleanup_claude_removes_debug_file`, `cleanup_claude_skipped_when_invocation_failed`, `finish_claude_parse_failure_writes_failure_bundle`, `finish_codex_parse_failure_preserves_artifacts`).
- **Removed `truncate_utf8` helper** and `AtomicBool`/`Ordering` imports — no longer used after eprintln removal.

## Amendment [P2]: Nullable Multi-Type Arrays Still Violate Strict Mode — FIXED

The root cause: when `normalize_nullable_type_array` encountered >1 non-null type (e.g., `["string", "integer", "null"]`), it wrote back `{"type": ["string", "integer"]}` on the non-null arm — still a type array, still violating strict mode.

Fix: the `else` branch (multi-type case) now creates **one `anyOf` arm per non-null type** with scalar `type` values:
```json
{"anyOf": [{"type": "string", ...props}, {"type": "integer", ...props}, {"type": "null"}]}
```

Schema-level properties (e.g., `format`) are cloned onto each non-null arm.

Added regression test `enforce_strict_mode_normalizes_nullable_multi_type_array` that verifies a `["string", "integer", "null"]` type array with `"format": "custom"` produces three `anyOf` arms with scalar types and properties preserved.

## Amendment [P2]: Parse Failures Dump Model Output To stderr — FIXED

Removed both raw `eprintln!` calls (was at lines ~120 and ~144) from Claude's `finish()` error paths. The error return values already contain sufficient diagnostic information (parse error message, contract label, content lengths). No replacement logging was added since the error propagation path already provides structured failure context through `FailureClass` and detailed error messages.

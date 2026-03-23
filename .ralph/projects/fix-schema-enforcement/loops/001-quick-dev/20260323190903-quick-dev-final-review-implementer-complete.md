---
artifact: quick-dev-final-review
loop: 1
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T19:09:03Z
---

The error messages still include the relevant details (error text, contract label, lengths). The removed `eprintln!` calls included stdout_head/result_head previews that went to stderr of the daemon process — those are now replaced by the `preserve_failure_artifacts` mechanism which saves the *entire* stdout/stderr to `runtime/failed/`, which is strictly better for debugging.

# Final Review: NO AMENDMENTS

## Summary

The implementation correctly addresses all acceptance criteria from the master prompt:

**Schema enforcement (`normalize_nullable_type_array` + `enforce_strict_mode_schema`):**

1. **Type-array normalization** — `normalize_nullable_type_array` (lines 1020–1077) correctly detects type arrays containing `"null"`, rewrites to `anyOf` format, preserves schema annotations (format, minimum, etc.) on the non-null variant, strips `"default": null` while preserving non-null defaults, and handles the multi-type edge case by creating separate anyOf arms per non-null type.

2. **Non-nullable defaults untouched** — Fields with single-string `type` (e.g., `Vec<T>` → `"array"`, `u32` → `"integer"`) are never touched by the normalization, since it only triggers on `Value::Array` type values. The `required`-completion logic adds them to `required` without modifying their schemas.

3. **Composition recursion** — New `anyOf`/`oneOf`/`allOf` recursion (lines 1142–1149) ensures nested object schemas in composition keywords get `additionalProperties: false` and full `required` arrays. Crucially, this also handles freshly-normalized schemas: when `normalize_nullable_type_array` rewrites a nullable object to `anyOf`, the subsequent recursion applies strict-mode enforcement to the object variant within the same call.

4. **Existing behavior preserved** — The normalization runs before the `is_object` check, so it doesn't interfere. After normalization, the map no longer has a type array, so `is_object` correctly reads the scalar `type` value from any remaining schemas.

**Supplementary changes:**

5. **`preserve_failure_artifacts`** (lines 61–91) — Correctly moves Codex temp files to `runtime/failed/` and writes raw stdout+stderr before `cleanup` runs. The `best_effort_move_file` helper handles cross-filesystem moves and NotFound gracefully. The stale-session retry path correctly skips preservation (not a terminal failure).

6. **`finish` method borrow change** — Switching `match self.response_decoder` to `match &self.response_decoder` enables `preserve_failure_artifacts` (which takes `&self`) to be called before `finish` consumes `self` in the success path. The `*session_resuming` dereference is correct.

7. **`--debug-file` removal and `eprintln!` removal** — The debug logging is replaced by the `preserve_failure_artifacts` mechanism, which preserves full stdout/stderr on disk rather than printing truncated previews to stderr.

8. **Tests** — 8 new unit tests cover all normalization cases (nullable, format preservation, multi-type, non-nullable defaults, oneOf recursion, FinalReviewProposal round-trip, default:null stripping, non-null default preservation). 2 new integration tests cover failure artifact preservation for both Codex and Claude backends.

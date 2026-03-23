---
artifact: quick-dev-plan-implement
loop: 1
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T17:58:59Z
---

# Implementation: Normalize Nullable Type Arrays for OpenAI Strict Mode

## Changes Made

All changes are in `src/adapters/process_backend.rs`:

### 1. New `normalize_nullable_type_array` helper (lines 1043–1083)

Rewrites `{"type": ["T", "null"], ...props}` → `{"anyOf": [{"type": "T", ...props}, {"type": "null"}]}`. Key behaviors:
- **Format-driven detection**: Triggers only on `Value::Array` type containing `"null"` — single-string types (from `Vec<T>`, `u32`, etc.) are never touched.
- **Property preservation**: Uses `std::mem::take(map)` to move all remaining schema properties (`format`, `minimum`, etc.) onto the non-null variant.
- **Correct for multi-type arrays**: If multiple non-null types exist (unlikely from schemars, but correct), they stay as an array.

### 2. Integration into `enforce_strict_mode_schema` (line 1100)

`normalize_nullable_type_array(map)` is called at the top of the `Object(map)` branch, before the `is_object` check. Since nullable type arrays never have `type: "object"`, the normalization doesn't interfere with the existing object-detection logic.

### 3. `anyOf`/`oneOf`/`allOf` recursion (lines 1148–1155)

After existing recursion into `properties`, `definitions`, and `items`, a new block iterates over composition keywords and recurses into each variant. This ensures:
- Freshly-rewritten `anyOf` from nullable normalization gets its variants recursed (relevant if the non-null type is an object).
- Tagged-enum `oneOf` variants get `additionalProperties: false` and full `required` arrays.

### 4. Five new unit tests (lines 1322–1493)

| Test | Validates |
|------|-----------|
| `enforce_strict_mode_normalizes_nullable_type_array` | `Option<String>` → `anyOf` rewrite, added to `required` |
| `enforce_strict_mode_preserves_format_on_nullable_integer` | `format`/`minimum` preserved on non-null variant |
| `enforce_strict_mode_leaves_non_nullable_defaults_unchanged` | `Vec<T>`, `u32` with `#[serde(default)]` keep single-string `type`, no null added |
| `enforce_strict_mode_recurses_into_one_of` | Tagged-enum `oneOf` variants get `additionalProperties`+`required` |
| `enforce_strict_mode_final_review_proposal_round_trip` | End-to-end: `FinalReviewProposal` with `rationale: Option<String>` in `definitions` |

Note: `cargo test` could not be run in this environment (no C linker available), but all existing tests are structurally preserved (the function signature and behavior for single-string `type` schemas is unchanged — normalization is a no-op for them).

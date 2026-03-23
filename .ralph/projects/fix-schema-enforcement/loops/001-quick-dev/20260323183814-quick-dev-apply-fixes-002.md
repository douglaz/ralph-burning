---
artifact: quick-dev-apply-fixes
loop: 1
iteration: 2
project: fix-schema-enforcement
backend: claude
role: implementer
created_at: 2026-03-23T18:38:14Z
---

# Apply-Fixes: Strip `default: null` from non-null anyOf variant

## Reviewer finding addressed

The reviewer identified that `normalize_nullable_type_array` blindly moves all remaining map keys (via `std::mem::take`) into the non-null branch of the `anyOf` rewrite. For schemars 0.8 defaulted `Option<T>` fields, this carries `"default": null` onto the non-null variant, producing `{"type":"string","default":null}` — semantically incorrect for strict mode.

## Changes made

### 1. Fix in `normalize_nullable_type_array` (process_backend.rs:1094-1100)

After `std::mem::take(map)` moves all remaining properties into `non_null_variant`, strip `"default"` if its value is `null`. This is a targeted check — non-null defaults (e.g., `"default": 0` on a nullable integer) are preserved on the non-null variant since they carry meaningful type-specific information.

### 2. Updated `enforce_strict_mode_final_review_proposal_round_trip` test (line 1490)

Added `"default": null` to the `rationale` field fixture to match real schemars 0.8 output for `#[serde(default, skip_serializing_if = "Option::is_none")] pub rationale: Option<String>`. The existing assertions (`any_of[0] == {"type": "string"}`) now implicitly verify the default is stripped.

### 3. New test: `enforce_strict_mode_strips_default_null_from_non_null_variant` (line 1524)

Dedicated regression test: input has `{"default": null, "type": ["string", "null"]}`, output verifies:
- Non-null variant has no `"default"` key
- Non-null variant is exactly `{"type": "string"}`
- Wrapper level has no `"default"` key either

### 4. New test: `enforce_strict_mode_preserves_non_null_default` (line 1557)

Ensures non-null defaults like `"default": 0` on `{"type": ["integer", "null"], "format": "uint32"}` are correctly preserved on the non-null variant (only `null` defaults are stripped).

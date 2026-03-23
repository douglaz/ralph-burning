## Summary

Fix `enforce_strict_mode_schema` in `src/adapters/process_backend.rs` to correctly handle optional (`Option<T>` with `#[serde(default)]`) fields when forcing all properties into the `required` array for OpenAI strict-mode compliance. Currently, the function blindly adds every property key to `required` without accounting for fields that `schemars` intentionally excluded — fields whose schemas may not permit `null`. This causes Claude CLI crashes during `final_review` structured output validation because fields like `FinalReviewProposal.rationale` become required but the backend receives `null`, which fails schema validation when the field's schema doesn't include a null type. Additionally, the function doesn't recurse into `anyOf`/`oneOf`/`allOf` arrays, leaving tagged-union enum schemas (e.g., `RecordProducer`, `StepStatus`) unenforced.

## Acceptance Criteria

1. **Null-wrapping for newly-required fields**: When a property key is in `properties` but was NOT in the original `required` array, its schema must be wrapped in `{"anyOf": [<original_schema>, {"type": "null"}]}` before adding it to `required`. This allows the LLM to send `null` for optional fields. Fields already in `required` are left unchanged.

2. **No double-wrapping**: If a field's schema already permits null (has `"type": "null"` as a variant in an existing `anyOf`, or has `"type"` as an array containing `"null"`), do not wrap it again.

3. **Recursion into `anyOf`/`oneOf`/`allOf`**: The function must recurse into each element of `anyOf`, `oneOf`, and `allOf` arrays so that object schemas nested inside tagged-union enums or composition schemas also get `additionalProperties: false` and full `required` arrays.

4. **Existing behavior preserved**: Properties that were already in the original `required` array retain their original schemas without null-wrapping. `additionalProperties: false` injection and recursion into `properties`, `definitions`, and `items` continue to work as before.

5. **`FinalReviewProposal` no longer crashes**: A schema generated from `FinalReviewProposalPayload` (which contains `FinalReviewProposal` with `Option<String>` rationale) passes OpenAI strict-mode validation when the LLM returns `null` for `rationale`.

## Technical Approach

### 1. Detect originally-optional fields before mutating `required`

In `enforce_strict_mode_schema` at `process_backend.rs:1063-1081`, before adding missing keys to `required`, collect the set of keys that are NOT already present in the `required` array. These are the "newly-required" keys — fields that `schemars` considered optional.

```rust
// Snapshot which keys are already required BEFORE we add the rest
let originally_required: HashSet<String> = match map.get("required") {
    Some(Value::Array(arr)) => arr.iter()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect(),
    _ => HashSet::new(),
};
```

### 2. Null-wrap newly-required field schemas

After adding all property keys to `required` (existing logic at lines 1069-1080), iterate over properties and wrap any field that was NOT in `originally_required`:

```rust
if let Some(Value::Object(props)) = map.get_mut("properties") {
    for (key, schema) in props.iter_mut() {
        if !originally_required.contains(key) {
            wrap_with_null_if_needed(schema);
        }
    }
}
```

The helper `wrap_with_null_if_needed` checks whether the schema already allows null before wrapping:

- If `schema["type"]` is an array containing `"null"` → skip
- If `schema["anyOf"]` is an array containing `{"type": "null"}` → skip
- Otherwise → replace schema value with `{"anyOf": [<original>, {"type": "null"}]}`

### 3. Add recursion into `anyOf`/`oneOf`/`allOf`

After the existing recursion blocks (lines 1083-1098), add three new blocks:

```rust
for keyword in ["anyOf", "oneOf", "allOf"] {
    if let Some(Value::Array(variants)) = map.get_mut(keyword) {
        for variant in variants.iter_mut() {
            enforce_strict_mode_schema(variant);
        }
    }
}
```

This handles `schemars`-generated schemas for `#[serde(tag = "type")]` enums like `RecordProducer` (internally-tagged → `oneOf`), `#[serde(tag, content)]` enums like `PanelPayload` (adjacently-tagged → `oneOf`), and any `Option<T>` fields that use `anyOf` with a `$ref`.

### 4. File changes are confined to a single function + one new helper

All changes are within `enforce_strict_mode_schema` in `process_backend.rs` plus a small `wrap_with_null_if_needed` private helper in the same file. No new modules, no new dependencies.

## Files & Modules

| File | Change |
|------|--------|
| `src/adapters/process_backend.rs` | Modify `enforce_strict_mode_schema` (lines 1051-1100): add originally-required snapshot, null-wrapping for newly-required fields, `anyOf`/`oneOf`/`allOf` recursion. Add private helper `wrap_with_null_if_needed`. |
| `src/adapters/process_backend.rs` (tests module) | Add 4-5 new unit tests in the existing `#[cfg(test)] mod tests` block (starts at line 1180). |

No other files require changes. The function's signature and call sites (`process_backend.rs:464`, `process_backend.rs:518`, `openrouter_backend.rs:145`) remain unchanged.

## Testing Strategy

**Unit tests** (all in `src/adapters/process_backend.rs` tests module, alongside the existing 3 tests):

1. **`enforce_strict_mode_wraps_optional_fields_with_null`**: Schema with one required field and one optional field (not in `required`). After enforcement, the optional field's schema is wrapped in `anyOf` with null, while the required field's schema is unchanged.

2. **`enforce_strict_mode_skips_already_nullable_field`**: Schema where optional field already has `{"anyOf": [{"type": "string"}, {"type": "null"}]}`. After enforcement, no double-wrapping occurs — the `anyOf` array still has exactly 2 elements.

3. **`enforce_strict_mode_skips_type_array_with_null`**: Schema where optional field has `{"type": ["string", "null"]}`. After enforcement, the type array is left as-is, no anyOf wrapping added.

4. **`enforce_strict_mode_recurses_into_any_of`**: Schema with `anyOf` containing two object variants (simulating a tagged enum). After enforcement, both variant objects have `additionalProperties: false` and complete `required` arrays.

5. **`enforce_strict_mode_recurses_into_one_of_and_all_of`**: Same as above but for `oneOf` and `allOf` arrays.

6. **`enforce_strict_mode_final_review_proposal_scenario`**: End-to-end test using a schema shaped like `FinalReviewProposalPayload` with a nested `FinalReviewProposal` that has an optional `rationale`. Verifies that `rationale` ends up in `required` AND its schema allows null.

**Existing tests** (`enforce_strict_mode_adds_missing_required_fields`, `enforce_strict_mode_creates_required_when_absent`, `enforce_strict_mode_recurses_into_nested_objects`) must continue passing. The null-wrapping is additive — those tests' schemas have no originally-required tracking, so they'll now also get null-wrapping on their optional fields, and assertions should be updated if needed (or the test schemas can be adjusted to have all fields originally required).

**CI**: `cargo test` covers all of the above. No integration test changes needed — the function's call sites are unchanged.

## Out of Scope

- **Changing `schemars` version or features**: The fix works with schemars 0.8 as-is.
- **Modifying struct definitions or `#[serde]` attributes**: The schemas are generated correctly by `schemars`; only the strict-mode post-processing is broken.
- **`$ref` resolution**: `enforce_strict_mode_schema` already handles `definitions` recursion. Resolving `$ref` pointers inline is unnecessary — strict-mode enforcement on the definition itself is sufficient.
- **Handling `not` or `if`/`then`/`else` JSON Schema keywords**: These are not generated by `schemars` for any type in this codebase.
- **OpenRouter or Codex-specific schema handling**: All three backends use the same `enforce_strict_mode_schema` function; the fix applies uniformly.
- **Runtime schema validation**: This spec only covers the schema transformation. Validating that LLM responses conform to the transformed schema is handled downstream.
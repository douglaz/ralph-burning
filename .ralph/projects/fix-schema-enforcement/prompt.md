## Summary

Fix `enforce_strict_mode_schema` in `src/adapters/process_backend.rs` to normalize `schemars`-generated nullable type arrays into `anyOf` format compatible with OpenAI strict mode, and to recurse into `anyOf`/`oneOf`/`allOf` composition arrays.

The root cause is a format mismatch: `schemars` 0.8 represents `Option<T>` fields as `{"type": ["T", "null"]}` (a JSON Schema type array), but OpenAI strict mode requires single-string `type` values and uses `{"anyOf": [{"type": "T"}, {"type": "null"}]}` for union types. When `enforce_strict_mode_schema` adds these fields to `required` — correctly, per strict-mode rules — the resulting schema is rejected by the backend validator because it encounters a type array instead of a scalar type. This manifests as Claude CLI crashes during `final_review` structured output validation for schemas containing `Option<T>` fields like `FinalReviewProposal.rationale: Option<String>` (`panel_contracts.rs:132`).

A secondary issue is that the function does not recurse into `anyOf`/`oneOf`/`allOf` arrays, so object schemas inside composition keywords (e.g., tagged-enum variants using `#[serde(tag = "...")]`) would not receive `additionalProperties: false` or full `required` arrays. No tagged enum schemas currently flow through invocation contracts, but `RecordProducer` (`panel_contracts.rs:250`) derives `JsonSchema` with `#[serde(tag = "type")]` and would produce unenforced `oneOf` variants if ever used in a contract.

Critically, the detection mechanism must **not** use "not in original `required`" as a proxy for `Option<T>`. Several non-nullable fields are omitted from `required` by schemars due to `#[serde(default)]` — e.g., `RequirementsReviewPayload.follow_ups: Vec<String>` (`model.rs:434`), `ProjectSeedPayload.follow_ups: Vec<String>` (`model.rs:585`), `ProjectSeedPayload.version: u32` (`model.rs:578`). These fields' schemas use a single `type` string (`"array"` or `"integer"`), not a type array, so they are unaffected by the normalization. Wrapping them with null would create a schema/serde deserialization mismatch where the schema accepts `null` but `serde` rejects it.

## Acceptance Criteria

1. **Type-array normalization to `anyOf`**: Any schema node where `"type"` is an array containing `"null"` must be rewritten to `anyOf` format. For example, `{"type": ["string", "null"]}` becomes `{"anyOf": [{"type": "string"}, {"type": "null"}]}`. Schema-level properties that apply to the non-null type (e.g., `"format"`, `"minimum"`) must be preserved on the non-null variant. This applies recursively at every depth — property schemas, definition schemas, items schemas, and composition-array element schemas.

2. **Non-nullable defaulted fields remain non-nullable**: Fields whose schemars-generated schema uses a single `type` string (e.g., `follow_ups: Vec<String>` → `{"type": "array", ...}`, `version: u32` → `{"type": "integer", ...}`) must NOT have null added to their schema, even when they are newly added to `required`. They are non-nullable in serde and must remain non-nullable in the schema.

3. **Recursion into `anyOf`/`oneOf`/`allOf`**: The function must recurse into each element of `anyOf`, `oneOf`, and `allOf` arrays so that object schemas nested inside composition keywords receive `additionalProperties: false` and full `required` arrays.

4. **Existing behavior preserved**: `additionalProperties: false` injection, `required`-array completion, and recursion into `properties`, `definitions`, and `items` continue to work as before. Properties already in the original `required` array retain their schemas — type-array normalization still applies to them if their schema uses a type array, since the normalization is format-driven, not optionality-driven.

5. **`FinalReviewProposal` no longer crashes**: A schema generated from `FinalReviewProposalPayload` (which nests `FinalReviewProposal` with `rationale: Option<String>`) has `rationale` in `required` with schema `{"anyOf": [{"type": "string"}, {"type": "null"}]}` — not `{"type": ["string", "null"]}`. The schema passes OpenAI strict-mode validation when the LLM returns `null` for `rationale`.

6. **Non-nullable defaults pass schema-then-serde round-trip**: A schema generated from `RequirementsReviewPayload` has `follow_ups` in `required` with schema `{"type": "array", "items": {"type": "string"}}` (unchanged, no null added). A payload with `"follow_ups": []` passes both schema validation and serde deserialization; a payload with `"follow_ups": null` is rejected by the schema.

## Technical Approach

### 1. Add `normalize_nullable_type_array` helper

A private helper that operates on a `&mut serde_json::Map<String, Value>`. It checks whether `"type"` is an array containing `"null"`. If so, it rewrites the node into `anyOf` format:

```rust
/// Rewrite `{"type": ["T", "null"], ...props}` → `{"anyOf": [{"type": "T", ...props}, {"type": "null"}]}`.
/// Leaves schemas with single-string `type` (e.g., `"array"`, `"integer"`) unchanged.
fn normalize_nullable_type_array(map: &mut serde_json::Map<String, Value>) {
    let has_null_in_type_array = match map.get("type") {
        Some(Value::Array(arr)) => arr.iter().any(|v| v.as_str() == Some("null")),
        _ => return,
    };
    if !has_null_in_type_array {
        return;
    }

    // Extract type array
    let types = match map.remove("type") {
        Some(Value::Array(arr)) => arr,
        _ => return,
    };
    let non_null_types: Vec<Value> = types.into_iter()
        .filter(|v| v.as_str() != Some("null"))
        .collect();

    // Build non-null variant: carry over remaining schema properties (format, minimum, etc.)
    let mut non_null_variant = std::mem::take(map);
    if non_null_types.len() == 1 {
        non_null_variant.insert("type".to_owned(), non_null_types.into_iter().next().unwrap());
    } else {
        non_null_variant.insert("type".to_owned(), Value::Array(non_null_types));
    }

    // Replace map contents with anyOf wrapper
    map.insert("anyOf".to_owned(), Value::Array(vec![
        Value::Object(non_null_variant),
        serde_json::json!({"type": "null"}),
    ]));
}
```

This helper is purely format-driven: it triggers on type arrays containing `"null"`, which schemars only generates for `Option<T>`. Non-nullable defaults like `Vec<T>` and `u32` have single-string `type` values and are never touched.

### 2. Integrate normalization into `enforce_strict_mode_schema`

Call `normalize_nullable_type_array(map)` at the top of the `if let Object(map) = value` block, before the `is_object` check. Since nullable type arrays never have `type: "object"` (they have `type: ["string", "null"]` etc.), the normalization does not interfere with the object-detection logic.

```rust
pub(crate) fn enforce_strict_mode_schema(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        // NEW: Convert type arrays like ["string", "null"] to anyOf format
        normalize_nullable_type_array(map);

        let is_object = map.get("type").and_then(|t| t.as_str()) == Some("object");
        if is_object {
            // ... existing additionalProperties + required logic (unchanged)
        }
        // ... existing recursion into properties, definitions, items (unchanged)

        // NEW: Recurse into anyOf/oneOf/allOf
        for keyword in ["anyOf", "oneOf", "allOf"] {
            if let Some(Value::Array(variants)) = map.get_mut(keyword) {
                for variant in variants.iter_mut() {
                    enforce_strict_mode_schema(variant);
                }
            }
        }
    }
}
```

The `anyOf`/`oneOf`/`allOf` recursion block is placed after existing recursion blocks. Note that schemas freshly rewritten by `normalize_nullable_type_array` now have an `anyOf` key, so the new recursion block will descend into their variants — this is correct and ensures the non-null variant also gets normalized if it is itself a nested object.

### 3. No changes to the `required`-completion logic

The existing logic at lines 1063–1081 that adds all property keys to `required` remains unchanged. The null-wrapping approach from the original spec is removed entirely. Instead, the type-array normalization ensures that `Option<T>` fields already have a schema that permits null (via `anyOf`), while non-nullable defaults keep their non-nullable schema. Both categories are correctly added to `required`.

### 4. File changes are confined to a single function + one new helper

All changes are within `enforce_strict_mode_schema` in `process_backend.rs` plus the private `normalize_nullable_type_array` helper in the same file. No new modules, no new dependencies, no signature changes.

## Files & Modules

| File | Change |
|------|--------|
| `src/adapters/process_backend.rs` | Modify `enforce_strict_mode_schema` (lines 1051–1100): add call to `normalize_nullable_type_array` at top of object branch, add `anyOf`/`oneOf`/`allOf` recursion block after existing recursion blocks. Add private helper `normalize_nullable_type_array`. |
| `src/adapters/process_backend.rs` (tests module) | Add 5 new unit tests in the existing `#[cfg(test)] mod tests` block (starts at line 1180). The function is `pub(crate)`, so tests must be in this in-module block — not in `tests/unit/process_backend_test.rs`, which tests the public adapter API via integration-style tests. |

No other files require changes. The function's signature and call sites (`process_backend.rs:464`, `process_backend.rs:518`, `openrouter_backend.rs:145`) remain unchanged.

## Testing Strategy

**Unit tests** (all in the `#[cfg(test)] mod tests` block in `src/adapters/process_backend.rs`, alongside the existing 3 tests):

1. **`enforce_strict_mode_normalizes_nullable_type_array`**: Schema with a property using `{"type": ["string", "null"]}` (simulating `Option<String>` from schemars). After enforcement, the property schema is `{"anyOf": [{"type": "string"}, {"type": "null"}]}` and the property is in `required`.

2. **`enforce_strict_mode_preserves_format_on_nullable_integer`**: Schema with a property using `{"type": ["integer", "null"], "format": "uint32", "minimum": 0}` (simulating `Option<u32>` from schemars). After enforcement, the property schema is `{"anyOf": [{"type": "integer", "format": "uint32", "minimum": 0}, {"type": "null"}]}` — format and minimum are preserved on the non-null variant.

3. **`enforce_strict_mode_leaves_non_nullable_defaults_unchanged`**: Schema shaped like `RequirementsReviewPayload` with `follow_ups: {"type": "array", "items": {"type": "string"}}` not in `required` (simulating `Vec<String>` with `#[serde(default)]`). After enforcement, `follow_ups` is in `required` but its schema is unchanged — still `{"type": "array", ...}`, no `anyOf`, no null.

4. **`enforce_strict_mode_recurses_into_one_of`**: Schema with a `oneOf` array containing two object variants (simulating an internally-tagged enum like `RecordProducer`). After enforcement, both variant objects have `additionalProperties: false` and complete `required` arrays.

5. **`enforce_strict_mode_final_review_proposal_round_trip`**: End-to-end test using a schema shaped like the schemars output for `FinalReviewProposalPayload`: a top-level object with `amendments: {"type": "array", "items": {"$ref": "#/definitions/FinalReviewProposal"}}`, and a `definitions` entry for `FinalReviewProposal` with `body: {"type": "string"}` in required and `rationale: {"type": ["string", "null"]}` NOT in required. After enforcement: `rationale` is in `required`, its schema is `{"anyOf": [{"type": "string"}, {"type": "null"}]}`, and the definition has `additionalProperties: false`.

**Existing tests** (`enforce_strict_mode_adds_missing_required_fields`, `enforce_strict_mode_creates_required_when_absent`, `enforce_strict_mode_recurses_into_nested_objects`) continue passing unchanged. None of these tests use type arrays — their schemas use single-string `type` values, so the normalization is a no-op. The `follow_ups` field in test 1 (line 1196) has `{"type": "array", ...}` which correctly remains non-nullable.

**CI**: `cargo test` covers all of the above. No integration test changes needed — the function's call sites are unchanged.

## Out of Scope

- **Changing `schemars` version or features**: The fix normalizes schemars 0.8 output; it does not require a different version.
- **Modifying struct definitions or `#[serde]` attributes**: The schemas are generated correctly by `schemars`; only the strict-mode post-processing format is incompatible.
- **Adding null to non-nullable schemas**: The original spec proposed wrapping all "newly-required" fields with null. This is explicitly rejected — non-nullable defaulted fields (`Vec<T>`, `u32`, etc.) must remain non-nullable to match serde deserialization behavior.
- **`$ref` resolution**: `enforce_strict_mode_schema` already handles `definitions` recursion. Resolving `$ref` pointers inline is unnecessary — strict-mode enforcement on the definition itself is sufficient.
- **Handling `not` or `if`/`then`/`else` JSON Schema keywords**: These are not generated by `schemars` for any type in this codebase.
- **OpenRouter or Codex-specific schema handling**: All three backends use the same `enforce_strict_mode_schema` function; the fix applies uniformly.
- **Runtime schema validation**: This spec covers only the schema transformation. Validating that LLM responses conform to the transformed schema is handled downstream.
- **Tagged-enum contract schemas**: No tagged enum currently flows through invocation contracts. The `anyOf`/`oneOf`/`allOf` recursion is added for correctness, but there is no immediate crash to fix in this area. If `RecordProducer` or other tagged enums are later added to invocation schemas, they will be handled automatically.
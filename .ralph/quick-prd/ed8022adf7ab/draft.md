## Summary

Add `inline_schema_refs(value: &mut serde_json::Value)` to `src/adapters/process_backend.rs` that resolves all `{"$ref": "#/definitions/Foo"}` references in-place by substituting the referenced definition object. Call it in `build_command()` after `enforce_strict_mode_schema()` for both the Claude and Codex branches, before the schema is serialized to JSON. This eliminates `$ref` from the schema passed to `claude --json-schema`, fixing structured-output validation failures (issue #46).

## Acceptance Criteria

1. Any `{"$ref": "#/definitions/X"}` node in a schema is replaced with a deep clone of the corresponding definition from the top-level `definitions` map.
2. Nested `$ref` (a definition that itself contains `$ref` to another definition) is resolved transitively.
3. The top-level `definitions` key is removed from the schema after inlining.
4. A schema with no `$ref` or `definitions` passes through unchanged.
5. The function is called in `build_command()` for both `BackendFamily::Claude` (line 460) and `BackendFamily::Codex` (line 507) branches, after `enforce_strict_mode_schema()` and before serialization.

## Technical Approach

Add a `pub(crate) fn inline_schema_refs(value: &mut serde_json::Value)` function alongside the existing `enforce_strict_mode_schema` (line 1093) in `src/adapters/process_backend.rs`. Follow the same pattern: a `pub(crate)` standalone function that takes `&mut serde_json::Value`.

**Algorithm:**

1. Extract the top-level `definitions` key as a `serde_json::Map` via `value.as_object_mut()` / `map.remove("definitions")`. If absent, return early — no work to do.
2. Call a recursive helper `resolve_refs(node: &mut serde_json::Value, definitions: &serde_json::Map<String, serde_json::Value>)` on the root value.
3. `resolve_refs` checks if `node` is an object with a single key `"$ref"` whose value is a string starting with `"#/definitions/"`. If so, look up the definition name in `definitions`, clone it into `replacement`, recursively call `resolve_refs` on the replacement (to handle nested refs), then replace `*node = replacement`.
4. Otherwise recurse into all values of objects and all elements of arrays.

**Integration point — `build_command()`:**

Insert `inline_schema_refs(&mut schema_value);` immediately after each `enforce_strict_mode_schema(&mut schema_value);` call:
- Line 460 (Claude branch)
- Line 507 (Codex branch)

Order matters: `enforce_strict_mode_schema` already recurses into `definitions` to add `additionalProperties`/`required`, so running it first ensures the inlined definitions are already strict-mode-compliant.

## Files & Modules

| File | Change |
|---|---|
| `src/adapters/process_backend.rs` | Add `inline_schema_refs()` + helper `resolve_refs()`. Add two call sites in `build_command()`. Add unit tests. |

No new files or modules needed.

## Testing Strategy

Add three `#[test]` functions in the existing `mod tests` block (after the `enforce_strict_mode_*` tests, around line 1569):

1. **`inline_schema_refs_resolves_simple_ref`** — Schema with `definitions.Foo` and a property using `{"$ref": "#/definitions/Foo"}`. Assert `$ref` is replaced with Foo's body and `definitions` key is removed.

2. **`inline_schema_refs_resolves_nested_refs`** — Definition A references definition B via `$ref`. Assert both are inlined transitively and `definitions` is removed.

3. **`inline_schema_refs_no_op_without_refs`** — Schema with no `definitions` or `$ref`. Assert output equals input (clone before, compare after).

4. **Existing test coverage** — The existing `enforce_strict_mode_final_review_proposal_round_trip` test (line 1468) already exercises a schema with `$ref` and `definitions`. After the change, consider extending it to verify inlining + strict-mode together, or leave as-is since `enforce_strict_mode_schema` runs before `inline_schema_refs` and its behavior is unchanged.

All tests run via `cargo test` with no additional dependencies.

## Out of Scope

- Circular `$ref` detection (schemars 0.8 does not produce circular definitions for the derive macro; not a real risk).
- `$ref` paths other than `#/definitions/X` (e.g., remote URLs, `$defs`, nested JSON pointer paths). Only `#/definitions/<name>` is produced by schemars 0.8.
- Modifying `enforce_strict_mode_schema` itself — it continues to process `definitions` for strict-mode compliance before inlining removes them.
- Changes to the Codex/OpenRouter adapters beyond the two existing `build_command()` call sites.
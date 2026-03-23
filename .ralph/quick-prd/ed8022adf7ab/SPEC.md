## Summary

Add `inline_schema_refs(value: &mut serde_json::Value)` to `src/adapters/process_backend.rs` that resolves all `{"$ref": "#/definitions/Foo"}` references in-place by substituting the referenced definition object. Call it after `enforce_strict_mode_schema()` at all three sites that send strict-mode JSON Schema to a backend: the Claude and Codex branches of `build_command()` in `process_backend.rs`, and the `request_body()` function in `openrouter_backend.rs`. This eliminates `$ref` from schemas sent to backends, fixing structured-output validation failures (issue #46).

### Why post-processing instead of `schemars::SchemaSettings::inline_subschemas`

`schemars` 0.8 provides `SchemaSettings { inline_subschemas: true, .. }` which inlines at generation time. We use post-processing instead for three reasons:

1. **Targeted scope.** Schema generation (`schema_for!` in `contracts.rs`) feeds multiple consumers: backend adapters, contract validation/deserialization, and template rendering (`engine.rs:104`). Changing generation affects all of them. Post-processing keeps the change local to the three call sites that need ref-free schemas.
2. **Ordering with strict-mode.** `enforce_strict_mode_schema` already recurses into `definitions` to add `additionalProperties`/`required`. Running it on the original ref-bearing schema, then inlining, guarantees inlined definitions are already strict-mode-compliant without changing `enforce_strict_mode_schema` itself.
3. **Residual refs.** `schemars` still emits `$ref` for recursive types even with `inline_subschemas = true` (documented: "Some references may still be generated in schemas for recursive types"). A post-processing pass gives us a single place to handle any remaining refs if recursive types are ever introduced.

## Acceptance Criteria

1. Any `{"$ref": "#/definitions/X"}` node in a schema is replaced with a deep clone of the corresponding definition from the top-level `definitions` map.
2. Nested `$ref` (a definition that itself contains `$ref` to another definition) is resolved transitively.
3. The top-level `definitions` key is removed from the schema after inlining.
4. A schema with no `$ref` or `definitions` passes through unchanged.
5. A `$ref` whose target is missing from `definitions` is left in place (no panic, no error).
6. A non-object `definitions` value (e.g., array, string) is ignored — the function returns without modification.
7. A schema with `definitions` but no `$ref` anywhere: `definitions` is still removed (the key is dead weight).
8. The function is called in all three backend schema preparation sites:
   - `build_command()` Claude branch (line ~460) after `enforce_strict_mode_schema`.
   - `build_command()` Codex branch (line ~507) after `enforce_strict_mode_schema`.
   - `openrouter_backend.rs` `request_body()` (line ~145) after `enforce_strict_mode_schema`.
9. Integration tests verify the Claude `--json-schema` payload and Codex schema file contain no `$ref` or `definitions` keys for a schema that produces refs.

## Technical Approach

Add a `pub(crate) fn inline_schema_refs(value: &mut serde_json::Value)` function alongside the existing `enforce_strict_mode_schema` (line 1093) in `src/adapters/process_backend.rs`. Follow the same pattern: a `pub(crate)` standalone function that takes `&mut serde_json::Value`.

**Algorithm:**

1. Guard: if `value` is not an object, return immediately.
2. Extract the top-level `definitions` key via `map.remove("definitions")`. If absent, return early.
3. If the removed value is not an object, return early (AC 6 — non-object `definitions` is a no-op).
4. Call a recursive helper `resolve_refs(node: &mut serde_json::Value, definitions: &serde_json::Map<String, serde_json::Value>)` on the root value.
5. `resolve_refs` checks if `node` is an object whose only key is `"$ref"` with a string value starting with `"#/definitions/"`. If so:
   - Extract the definition name (substring after `"#/definitions/"`).
   - Look it up in `definitions`. If found, clone it into `replacement`, recursively call `resolve_refs` on the replacement (to handle nested refs), then assign `*node = replacement`. If not found, leave the node unchanged (AC 5 — missing target is a no-op).
6. Otherwise recurse into all values of objects and all elements of arrays.

The top-level `definitions` key is already removed in step 2 and never re-inserted, satisfying AC 3 and AC 7.

**Integration points:**

Insert `inline_schema_refs(&mut schema_value);` immediately after each `enforce_strict_mode_schema(...)` call:

- `src/adapters/process_backend.rs` line ~460 (Claude branch of `build_command()`)
- `src/adapters/process_backend.rs` line ~507 (Codex branch of `build_command()`)
- `src/adapters/openrouter_backend.rs` line ~145 (`request_body()`)

Order matters: `enforce_strict_mode_schema` already recurses into `definitions` to add `additionalProperties`/`required`, so running it first ensures the inlined definitions are already strict-mode-compliant.

## Files & Modules

| File | Change |
|---|---|
| `src/adapters/process_backend.rs` | Add `inline_schema_refs()` + helper `resolve_refs()`. Add two call sites in `build_command()`. Add unit tests in `mod tests`. |
| `src/adapters/openrouter_backend.rs` | Add one call site in `request_body()` after `enforce_strict_mode_schema`. Update existing `request_body` test to assert no `$ref`/`definitions` in the emitted schema. |
| `tests/unit/process_backend_test.rs` | Add integration assertions to existing `claude_command_construction_and_double_parse` test (and Codex equivalent) verifying the serialized schema contains no `$ref` or `definitions`. |

No new files or modules needed.

## Testing Strategy

### Unit tests (in `src/adapters/process_backend.rs` `mod tests`)

Add four `#[test]` functions after the existing `enforce_strict_mode_*` tests:

1. **`inline_schema_refs_resolves_simple_ref`** — Schema with `definitions.Foo` and a property using `{"$ref": "#/definitions/Foo"}`. Assert `$ref` is replaced with Foo's body and `definitions` key is removed.

2. **`inline_schema_refs_resolves_nested_refs`** — Definition A references definition B via `$ref`. Assert both are inlined transitively and `definitions` is removed.

3. **`inline_schema_refs_no_op_without_refs`** — Schema with no `definitions` or `$ref`. Assert output equals input (clone before, compare after).

4. **`inline_schema_refs_handles_edge_cases`** — Parameterized over sub-cases:
   - `$ref` pointing to a name not present in `definitions` → node left unchanged, no panic.
   - `definitions` is a non-object (e.g., `"definitions": 42`) → schema unchanged (only the `definitions` key itself is removed since it was consumed).
   - `definitions` present but no `$ref` anywhere → `definitions` removed, rest unchanged.

### Integration tests (in `tests/unit/process_backend_test.rs`)

5. **Extend `claude_command_construction_and_double_parse`** — After the existing `args_text.contains("--json-schema")` assertion, parse the `--json-schema` value back into `serde_json::Value` and assert it contains no `"$ref"` keys and no `"definitions"` key at the top level. This verifies AC 8 for the Claude wiring end-to-end through `build_command()`.

6. **Add or extend a Codex `build_command()` test** — Read the schema file written to the temp directory and make the same no-`$ref`/no-`definitions` assertions. This verifies AC 8 for the Codex wiring.

### Integration tests (in `tests/unit/openrouter_backend_test.rs` or equivalent)

7. **Extend the existing OpenRouter `request_body` test** (around line 730 of `openrouter_backend.rs`) — After the existing `assert_eq` on the schema, add assertions that the schema at `body["response_format"]["json_schema"]["schema"]` contains no `"$ref"` keys and no top-level `"definitions"`. This verifies AC 8 for the OpenRouter wiring.

All tests run via `cargo test` with no additional dependencies.

## Out of Scope

- **Circular `$ref` detection.** `schemars` 0.8 does not produce circular definitions for the derive macro. If recursive types are introduced later, `inline_subschemas` still emits `$ref` for them, and our post-processing would likewise leave unresolvable self-references in place (they would hit the missing-target no-op path, AC 5). Cycle detection can be added as a follow-up if needed.
- **`$ref` paths other than `#/definitions/X`** (e.g., remote URLs, `$defs`, nested JSON pointer paths). Only `#/definitions/<name>` is produced by `schemars` 0.8. If a different prefix appears, the node is silently left unchanged.
- **Modifying `enforce_strict_mode_schema` itself.** It continues to process `definitions` for strict-mode compliance before inlining removes them.
- **Changing schema generation** (e.g., using `SchemaSettings { inline_subschemas: true }` at the `schema_for!` call sites). See the rationale in the Summary for why post-processing is preferred.
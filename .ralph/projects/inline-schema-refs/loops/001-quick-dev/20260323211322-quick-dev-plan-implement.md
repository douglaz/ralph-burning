---
artifact: quick-dev-plan-implement
loop: 1
project: inline-schema-refs
backend: claude
role: implementer
created_at: 2026-03-23T21:13:22Z
---

# Implementation: `inline_schema_refs` for Schema Reference Resolution

## Changes Made

### 1. `src/adapters/process_backend.rs` — Core function + unit tests + integration tests

**`inline_schema_refs()`** (after `enforce_strict_mode_schema`, ~line 1155): Extracts `definitions` from the root, then calls `resolve_refs()` to recursively replace `{"$ref": "#/definitions/X"}` nodes with cloned definition bodies. Removing `definitions` in step 2 satisfies AC 3/7.

**`resolve_refs()`**: Recursive helper that checks if a node is a single-key `$ref` object pointing at `#/definitions/<name>`. If found and the target exists, it clones/resolves/replaces; if missing, the node stays unchanged (AC 5). Otherwise recurses into object values and array elements.

**Call sites wired** (AC 8):
- Claude branch of `build_command()` — `inline_schema_refs(&mut schema_value)` after `enforce_strict_mode_schema` (line ~461)
- Codex branch of `build_command()` — same pattern (line ~509)

**Unit tests added** (4 tests):
- `inline_schema_refs_resolves_simple_ref` — AC 1: single `$ref` replaced, `definitions` removed
- `inline_schema_refs_resolves_nested_refs` — AC 2: transitive ref resolution (Outer → Inner)
- `inline_schema_refs_no_op_without_refs` — AC 4: schema without refs passes through unchanged
- `inline_schema_refs_handles_edge_cases` — AC 5/6/7: missing target left unchanged, non-object definitions consumed silently, definitions-with-no-refs still removed

**Integration tests added** (2 tests):
- `build_command_claude_schema_has_no_refs_or_definitions` — uses `build_command()` directly, parses the `--json-schema` arg, asserts no `$ref`/`definitions`
- `build_command_codex_schema_file_has_no_refs_or_definitions` — reads the written schema file, same assertions

### 2. `src/adapters/openrouter_backend.rs` — Call site + test update

**Call site**: `inline_schema_refs(&mut schema)` added after `enforce_strict_mode_schema` in `request_body()` (line ~146).

**Test updated**: `invoke_serializes_request_and_maps_usage_tokens` now applies `inline_schema_refs` to the expected schema and adds explicit `assert_no_refs` / no-`definitions` assertions on the actual HTTP request body.

### 3. `tests/unit/process_backend_test.rs` — Integration assertions

Added `"$ref"` and `"definitions"` absence checks to `claude_command_construction_and_double_parse` on the serialized args text, verifying the end-to-end wiring through `invoke()`.

## Test Results

- **46/46** adapter unit/integration tests pass
- **41/41** external integration tests pass
- All acceptance criteria (AC 1–9) are covered by tests

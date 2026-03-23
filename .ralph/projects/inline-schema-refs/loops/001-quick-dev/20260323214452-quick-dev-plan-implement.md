---
artifact: quick-dev-plan-implement
loop: 1
project: inline-schema-refs
backend: claude
role: implementer
created_at: 2026-03-23T21:44:52Z
---

# Implementation: Strengthen AC9 Integration Test Assertions

## Finding Closed: [P2] AC9 is only partially covered by end-to-end tests

### Root Cause
The Claude test only scanned the flat `claude-args.txt` for `"$ref"`/`"definitions"` substrings without isolating the `--json-schema` argument or proving it's valid JSON. The Codex test never inspected schema file contents at all.

### Changes Made (`tests/unit/process_backend_test.rs`)

**1. Added `assert_no_ref_keys` helper** (line 67) — Recursively walks a `serde_json::Value` and asserts no object contains a `"$ref"` key, reporting the exact JSON path on failure.

**2. Updated `write_fake_claude`** (line 105) — The fake Claude shell script now extracts the `--json-schema` argument value and writes it to `claude-json-schema.json` using `printf '%s'` (preserving the exact JSON without trailing newline issues).

**3. Updated `write_fake_codex`** (line 146) — The fake Codex shell script now parses `--output-schema <path>` from args and copies the schema file to `codex-schema-captured.json` before cleanup removes the original.

**4. Replaced Claude string assertions** (line ~475) — Removed the weak `!args_text.contains("\"$ref\"")` / `!args_text.contains("\"definitions\"")` checks. Replaced with:
- Read `claude-json-schema.json`, parse as `serde_json::Value`
- Assert top-level object has no `"definitions"` key
- Recursively assert no `"$ref"` keys via `assert_no_ref_keys`

**5. Added Codex schema verification** (line ~618) — New assertions in `codex_command_construction_and_temp_files`:
- Read `codex-schema-captured.json`, parse as `serde_json::Value`
- Assert top-level object has no `"definitions"` key
- Recursively assert no `"$ref"` keys via `assert_no_ref_keys`

### Why This Closes the Finding
- Both backends now validate the **actual JSON schema artifact** (parsed, not string-matched)
- The `--json-schema` value is isolated as a discrete argument, not grep'd from a flat string
- Schema file contents for Codex are captured before cleanup destroys them
- A regression in `inline_schema_refs` that leaves any `$ref` or `definitions` in the emitted schema will be caught by the recursive structural assertion

## Bead ID: ralph-burning-0gn

## Goal

Improve failure diagnostics by: (1) including the full error message and raw backend output in the runtime log when a stage fails, and (2) enabling debug/verbose output on Claude and Codex CLIs so transient failures produce actionable diagnostics.

## Problem

When a backend invocation fails (e.g. invalid JSON response), the runtime log (`runtime/logs/run.ndjson`) only emits a terse warning:

```json
{"level":"warn","message":"stage_failed: apply_fixes cycle=1 attempt=1 retry=true"}
```

The actual error details are only in the project journal (`journal.ndjson`):

```json
{"event_type":"stage_failed","details":{"failure_class":"schema_validation_failure","message":"invalid Claude result JSON: expected value at line 1 column 1 (contract: apply_fixes, result_len: 90)"}}
```

Additionally, Claude CLI runs with `--output-format json` but no `--verbose` flag, so when the CLI itself errors (timeout, rate limit, auth failure), the 90-byte stdout is all we get — no diagnostic context from the CLI.

## Changes Required

### 1. Include error details in runtime log
**File:** `src/contexts/workflow_composition/engine.rs` (around line 4560-4580, where `stage_failed` is logged)

Add the `failure_class`, `message`, and raw response length to the runtime log event so operators don't need to cross-reference `journal.ndjson`.

### 2. Capture and log raw backend output on failure
**File:** `src/adapters/process_backend.rs`

When the Claude or Codex CLI returns output that fails schema validation or JSON parsing, log or persist the raw stdout/stderr so the actual response is available for debugging.

### 3. Enable Claude CLI verbose/debug on schema validation errors
**File:** `src/adapters/process_backend.rs` (around line 646-673)

Check if Claude CLI supports a `--verbose` flag or `CLAUDE_CODE_DEBUG` environment variable. If so, consider always enabling it or enabling on retries.

### 4. Check Codex CLI for equivalent debug output
**File:** `src/adapters/process_backend.rs` (around line 675+)

Investigate whether Codex CLI has a `--verbose`, `--debug`, or equivalent flag.

## Acceptance Criteria

- Runtime log `stage_failed` events include the failure_class, error message, and raw response length
- Raw backend output is preserved or logged on failure (not silently discarded)
- Claude CLI debug/verbose output is captured when available
- No change to behavior on successful invocations
- Existing tests pass; new tests cover the enhanced error logging

◐ ralph-burning-gn8 · Harden retry policy to prevent run exit on transient backend failures   [● P0 · IN_PROGRESS]
Owner: master · Type: task
Created: 2026-03-30 · Updated: 2026-03-30

## Goal

Prevent ralph-burning runs from exiting/failing on transient backend issues (Claude/Codex CLI flakiness, rate limits, timeouts) by increasing retry limits and adding missing timeout enforcement.

## Investigation Findings

The system does NOT crash — it exits gracefully after exhausting retry attempts. The problem is the retry budget is too small for real-world backend flakiness:

### Current retry limits (`src/contexts/workflow_composition/retry_policy.rs:34-47`)

| FailureClass | Max Attempts | Retryable |
|---|---|---|
| TransportFailure | 3 | yes |
| SchemaValidationFailure | 2 | yes |
| DomainValidationFailure | 2 | yes |
| Timeout | 2 | yes |
| Cancellation | 1 | no |
| QaReviewOutcomeFailure | 1 | no |

A single transient Claude CLI failure (invalid JSON, exit code 1) gets only 2-3 tries before the entire run is marked as `Failed` and stops. In long-running tasks with 20+ rounds, this is too fragile.

### No hard timeout on backend invocations

**Critical gap:** `src/adapters/process_backend.rs` spawns backend processes and waits indefinitely (`active_child.wait()` at ~line 975). The timeout value in the invocation request is recorded in metadata but never enforced. A hanging Claude/Codex process blocks the entire run forever.

### All non-zero exit codes are TransportFailure

`src/adapters/process_backend.rs:1138-1196` classifies ANY non-zero exit (including exit 127 = binary not found, signal 11 = segfault) as `TransportFailure`. No granularity — a missing binary gets the same 3 retries as a transient network error.

## Changes Required

### 1. Increase retry limits for transient failures
**File:** `src/contexts/workflow_composition/retry_policy.rs:34-47`

- `TransportFailure`: 3 → 5 (network issues, rate limits)
- `SchemaValidationFailure`: 2 → 3 (invalid JSON from CLI)
- `Timeout`: 2 → 3 (backend slowness)

### 2. Add hard timeout enforcement on backend invocations
**File:** `src/adapters/process_backend.rs` (~line 975)

Wrap `spawn_and_wait()` / `active_child.wait()` in `tokio::time::timeout()` using the request's timeout value. If the timeout fires, kill the child process and return `FailureClass::Timeout`.

### 3. Differentiate fatal vs transient exit codes
**File:** `src/adapters/process_backend.rs:1138-1196`

Add classification:
- Exit 127 (binary not found) → new `FailureClass` or mark as non-retryable TransportFailure
- Signal-killed (SIGSEGV, SIGABRT) → TransportFailure (retryable, likely OOM or transient)
- Exit 1 with known error patterns (rate limit, auth) → TransportFailure (retryable)

### 4. Add exponential backoff between retries
**File:** `src/contexts/workflow_composition/engine.rs` (retry loop ~line 4620)

Currently retries are immediate. Add a short delay (e.g. 5s, 15s, 30s) between retry attempts to avoid hammering a rate-limited backend.

## Acceptance Criteria

- TransportFailure allows 5 attempts before run failure
- SchemaValidationFailure allows 3 attempts before run failure
- Backend invocations have a hard timeout (configurable, default ~10min)
- Hanging backend processes are killed after timeout
- Exit 127 (binary not found) fails immediately without retrying
- Retry attempts have backoff delay between them
- Existing tests pass; new tests cover timeout enforcement and retry backoff

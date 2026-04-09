## Bead ID: ralph-burning-wt3

## Goal

Gracefully degrade when a backend exhausts credits or hits persistent rate limits, so runs continue with available backends instead of failing entirely.

## Context

Observed on bead gn8: Codex exited with code 1 and stderr containing "You've hit your usage limit." This was classified as TransportFailure (retryable, 5 attempts). The system retried 5 times, failed every time (credits don't come back by retrying), and killed the entire run — even though the other reviewer (Claude Opus) was working fine.

## Changes Required

### 1. Detect persistent backend unavailability

**File:** `src/adapters/process_backend.rs`

Parse stderr/stdout for known persistent error patterns:
- "usage limit" / "hit your usage limit"
- "quota exceeded"
- "billing" / "credits"
- "try again at <time>"

Introduce a new `FailureClass::BackendUnavailable` (or similar) that is distinct from `TransportFailure`. This class should NOT be retried blindly.

### 2. Graceful panel degradation for final_review

**File:** `src/contexts/workflow_composition/final_review.rs`

When a reviewer in the final_review panel fails with `BackendUnavailable`:
- Skip that reviewer for this round
- Proceed with remaining reviewer(s) if at least one is available
- Log a warning: "reviewer codex/gpt-5.4 unavailable (credits exhausted), proceeding with N/M reviewers"
- Adjust consensus threshold accordingly (or require unanimous from remaining)

### 3. Apply to all multi-backend stages

The degradation logic should work for any stage that uses multiple backends (final_review panel, voter panel), not just final_review reviewers.

## Acceptance Criteria

- "Credits exhausted" errors are detected and classified as BackendUnavailable
- Final review panel proceeds with available reviewers when one backend is unavailable
- Run does NOT fail when a non-critical backend is unavailable
- Warning is logged when a backend is skipped
- If ALL backends are unavailable, the run fails with a clear error message
- Existing tests pass; new tests cover degradation scenarios
- `cargo test && cargo clippy && cargo fmt --check`

## Bead ID: ralph-burning-utm

## Goal

Change the default backend configuration so that the implementer defaults to Codex gpt-5.4 with reasoning_effort=high, and reviewers default to Codex gpt-5.4 xhigh + Opus 4.6.

## Context

Current defaults use Opus for implementation and Codex+Opus for review. The new defaults balance speed/cost for implementation (Codex is faster and cheaper) with thoroughness for review (cross-model review with two independent backends). These defaults should be overridable via workspace or project config.

## Changes Required

1. **Change default implementer**: from Claude Opus to Codex gpt-5.4 with reasoning_effort=high
2. **Change default reviewers**: set to Codex gpt-5.4 with reasoning_effort=xhigh plus Opus 4.6 (two reviewers for independent cross-model review)
3. **Preserve configurability**: workspace and project config overrides must still work
4. **Update any hardcoded references** to the old defaults

## Acceptance Criteria

- Default implementer is Codex gpt-5.4 with reasoning_effort=high
- Default reviewers are Codex gpt-5.4 xhigh and Opus 4.6
- Config overrides at workspace and project level still work correctly
- Existing tests pass (`cargo test && cargo clippy && cargo fmt --check`)

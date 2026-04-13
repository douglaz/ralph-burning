# Support reasoning effort configuration for Claude backends

## Problem

Recent Claude models (Opus 4.6) support a reasoning effort parameter similar to Codex's `model_reasoning_effort`. We need to support per-backend/role configuration of this effort level and default the Claude Opus reviewer in the final review panel to 'max' effort.

## Context

Codex already supports effort via `model_reasoning_effort` (e.g., "xhigh") passed as `-c model_reasoning_effort="xhigh"` to the codex CLI. Claude Code CLI supports a similar concept via the `--model-reasoning-effort` flag or equivalent. Check how the Claude backend adapter constructs its invocation command and add effort support there.

## Requirements

1. **Add effort to Claude backend adapter**: Pass the reasoning effort parameter when invoking Claude CLI. Check how the Codex adapter does it (look for `model_reasoning_effort` in the codebase) and mirror the pattern for Claude.

2. **Per-role effort configuration**: The existing backend configuration system should support effort per role. Check how `model_reasoning_effort` is configured for Codex backends and extend this to Claude backends.

3. **Default 'max' for Claude Opus reviewer**: In the final review panel, when the Claude Opus backend is used as a reviewer, default the effort to 'max'. This should be in the backend resolution or panel configuration code.

4. **Configuration precedence**: effort should be configurable at workspace, project, and role level, following the existing config precedence pattern.

## Implementation hints

- Search for `model_reasoning_effort` and `reasoning_effort` in the codebase to find the Codex implementation
- Search for how the Claude CLI invocation is built (look for `claude` command construction)
- The Claude Code CLI flag is likely `--model-reasoning-effort` or passed through environment/config
- Check the backend adapter code in `src/adapters/` or `src/contexts/`

## Acceptance Criteria
- Claude backend supports effort/reasoning configuration
- Per-role effort can be configured
- Default for Claude Opus reviewer in final_review panel is 'max'
- Existing Codex effort configuration continues to work unchanged
- cargo test && cargo clippy && cargo fmt --check pass

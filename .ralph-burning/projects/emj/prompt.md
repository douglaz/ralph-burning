◐ ralph-burning-emj · Restore token usage and cache efficiency metrics from all backends   [● P1 · IN_PROGRESS]
Owner: master · Type: task
Created: 2026-03-29 · Updated: 2026-03-30

## Summary

The old multibackend-orchestration project tracked three token metrics per invocation: input tokens, output tokens, and cache read tokens (cached_in). It also emitted structured tracing::info! events per attempt with presence booleans. The new ralph-burning only tracks prompt_tokens, completion_tokens, total_tokens in TokenCounts — and the process backend (Claude CLI and Codex CLI) returns TokenCounts::default() for everything, throwing away data that the CLI already provides.

## What Was Lost

1. **Cache read tokens** — Claude CLI outputs `cache_read_input_tokens` in the envelope `usage` field; Codex CLI outputs `cached_input_tokens` in `turn.completed` events. Both are ignored.
2. **All token counts from process backends** — `process_backend.rs` returns `TokenCounts::default()` for both Claude (line 187) and Codex (line 243), discarding available data.
3. **Structured per-invocation tracing** — No `tracing::info!` events with token fields, presence booleans, or session reuse flags.
4. **OpenRouter cache pass-through** — `OpenRouterUsage` only captures 3 standard fields, missing Anthropic's `cache_read_input_tokens` and OpenAI's `prompt_tokens_details.cached_tokens`.

## Implementation

### 1. Extend TokenCounts (model.rs:240-245)
Add `cache_read_tokens: Option<u32>` and `cache_creation_tokens: Option<u32>`.

### 2. Claude CLI usage extraction (process_backend.rs)
- Add `ClaudeUsage` struct with serde aliases for `input_tokens`/`tokens_in`, `output_tokens`/`tokens_out`, `cache_read_input_tokens`/`cached_in`, `cache_creation_input_tokens`.
- Add `usage: Option<ClaudeUsage>` field to `ClaudeEnvelope` (line 1231).
- Map to TokenCounts in Claude decoder (line 187).

Claude CLI `--output-format json` already outputs: `{"result":"...","session_id":"...","usage":{"input_tokens":N,"output_tokens":N,"cache_read_input_tokens":N}}`

### 3. Codex CLI usage extraction (process_backend.rs)
- Add `extract_codex_usage_from_stdout(stdout: &[u8]) -> TokenCounts` that scans NDJSON lines in reverse for last event with `usage` object.
- Codex emits: `{"type":"turn.completed","usage":{"input_tokens":N,"cached_input_tokens":N,"output_tokens":N}}`
- Wire into Codex decoder (line 243) using `&output.stdout`.

### 4. OpenRouter cache fields (openrouter_backend.rs)
- Extend `OpenRouterUsage` with `cache_read_input_tokens`, `cache_creation_input_tokens`, and `prompt_tokens_details: Option<PromptTokensDetails>`.
- Map `cache_read_tokens` from `cache_read_input_tokens.or(prompt_tokens_details.cached_tokens)`.

### 5. Fix stub backend (stub_backend.rs:449)
Add `cache_read_tokens: None, cache_creation_tokens: None` to struct literal.

### 6. Structured tracing (service.rs ~line 238)
Add `tracing::info!` after metadata finalization with: invocation_id, backend, model, attempt, duration_ms, prompt_tokens, completion_tokens, cache_read_tokens, cache_creation_tokens, tokens_reported, cache_reported, session_reused.

## Files

- `src/contexts/agent_execution/model.rs` — extend TokenCounts
- `src/adapters/process_backend.rs` — ClaudeUsage, ClaudeEnvelope.usage, Codex NDJSON extraction
- `src/adapters/openrouter_backend.rs` — cache fields in OpenRouterUsage
- `src/adapters/stub_backend.rs` — struct literal fix
- `src/contexts/agent_execution/service.rs` — tracing

## Acceptance Criteria

- [ ] TokenCounts has cache_read_tokens and cache_creation_tokens fields
- [ ] Claude CLI envelope usage is parsed into TokenCounts (unit test with mock envelope)
- [ ] Claude CLI envelope without usage still deserializes (backward compat test)
- [ ] Codex NDJSON stdout extraction returns correct TokenCounts (unit test)
- [ ] Empty Codex stdout returns TokenCounts::default() (unit test)
- [ ] OpenRouter cache tokens extracted from both Anthropic native and prompt_tokens_details formats (unit tests)
- [ ] tracing::info! event emitted after each invocation with all token fields
- [ ] cargo test passes
- [ ] cargo clippy -- -D warnings clean

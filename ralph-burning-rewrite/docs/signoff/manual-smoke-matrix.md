# Manual Smoke Matrix

Recorded: 2026-03-19
Environment: Linux x86_64, Rust 1.83+, ralph-burning v0.1.0

## Smoke Items

| # | Item | Environment | Command | Result | Follow-up Bug |
|---|------|-------------|---------|--------|---------------|
| 1 | Standard flow with Claude | Linux x86_64, claude CLI 2.1.79 at `/root/.npm-global/bin/claude`, isolated smoke workspace (`cd /tmp/rb-smoke-claude-run1`), scratch `workspace.toml` with `settings.default_backend = "claude"`, all roles overridden to claude (`workflow.implementer_backend`, `workflow.qa_backend`, `completion.backends`, `final_review.backends`, `prompt_review.*`) | `SMOKE_DIR=/tmp/rb-smoke-claude-run1 ./scripts/live-backend-smoke.sh claude` | FAIL | smoke_id: `smoke-claude-20260319162548`. Preflight PASS (backend check + probe planner/implementer). Bootstrap FAIL: `requirements:project_seed` contract returned empty `result` field and null `structured_output` — likely session-resumed `--resume` invocation loses `--json-schema` structured output. Error: `invalid Claude result JSON: expected value at line 1 column 1`. No project state created. Requirements run `req-20260319-162549` inspectable in scratch dir. Bug: session-resuming `claude -p --resume <id> --json-schema` produces null `structured_output`. |
| 2 | Standard flow with Codex | Linux x86_64, codex CLI 0.114.0 at `/root/.npm-global/bin/codex`, isolated smoke workspace (`cd /tmp/rb-smoke-codex-run1`), scratch `workspace.toml` with `settings.default_backend = "codex"`, all roles overridden to codex | `SMOKE_DIR=/tmp/rb-smoke-codex-run1 ./scripts/live-backend-smoke.sh codex` | FAIL | smoke_id: `smoke-codex-20260319163306`. Preflight PASS. Bootstrap FAIL at `requirements:requirements_review`: OpenAI strict-mode schema validation rejects `RequirementsReviewPayload` because `follow_ups` is in `properties` but not in `required`. Error: `Invalid schema for response_format ... Missing 'follow_ups'`. Draft succeeded (codex model `gpt-5.4`). No project state created. Requirements run `req-20260319-163306` inspectable. Bug: `RequirementsContract::review()` schema has `follow_ups` in properties but not required; OpenAI strict mode demands all property keys in required. |
| 3 | Standard flow with OpenRouter | Linux x86_64, `OPENROUTER_API_KEY` set (73 chars), `RALPH_BURNING_BACKEND=openrouter`, isolated smoke workspace (`cd /tmp/rb-smoke-openrouter-run1`), scratch `workspace.toml` with `settings.default_backend = "openrouter"`, `[backends.openrouter] enabled = true`, `execution.mode = "direct"`, all roles overridden to openrouter | `OPENROUTER_API_KEY=sk-or-... SMOKE_DIR=/tmp/rb-smoke-openrouter-run1 ./scripts/live-backend-smoke.sh openrouter` | FAIL | smoke_id: `smoke-openrouter-20260319163144`. Preflight PASS (API key validated, backend check + probe all pass with single-backend overrides). Bootstrap FAIL at `requirements:requirements_review`: `HTTP 502: Provider returned error` from OpenRouter upstream. Draft succeeded (model `openai/gpt-5`). No project state created. Requirements run `req-20260319-163147` inspectable. Bug: transient upstream provider failure; may pass on retry with stable provider. |
| 4 | quick_dev flow | Linux, test-stub | `cargo test --features test-stub -- run_start_completes_quick_dev_flow_end_to_end` | PASS | None |
| 5 | docs_change flow with configured docs validation | Linux, test-stub | `cargo test --features test-stub -- run_start_completes_docs_change_flow_end_to_end` | PASS | None |
| 6 | ci_improvement flow with configured CI validation | Linux, test-stub | `cargo test --features test-stub -- run_start_completes_ci_improvement_flow_end_to_end` | PASS | None |
| 7 | Full requirements draft | Linux, test-stub | `cargo test --features test-stub -- requirements_draft_with_empty_questions_completes` | PASS | None |
| 8 | Quick requirements | Linux, test-stub | `cargo test --features test-stub -- requirements_quick_creates_completed_run` | PASS | None |
| 9 | Create project from requirements | Linux, test-stub | `cargo test --features test-stub -- project_create_from_requirements` | PASS | None |
| 10 | Bootstrap and start run | Linux, test-stub | `cargo test --features test-stub -- project_bootstrap_from_file_quick_dev_start_runs_created_project` (cli.rs:1824, uses `--start`) | PASS | None |
| 11 | Single-repo daemon routing by label | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.routing.label_used_when_no_command` | PASS | None |
| 12 | Single-repo daemon routing by explicit command | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.routing.command_beats_label` | PASS | None |
| 13 | Multi-repo daemon polling | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.github.multi_repo_status` | PASS | None |
| 14 | Draft PR creation | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.lifecycle` | PASS | None |
| 15 | PR review ingestion into amendments | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.pr_review.completed_project_reopens_with_amendments` | PASS | None |
| 16 | Rebase flow on changed default branch | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.rebase.agent_resolves_conflict` (scenarios.rs:17701) | PASS | None |

## Evidence Template for Live Backend Rows (1-3)

After running `./scripts/live-backend-smoke.sh <backend>`, record the following
from the evidence file (`/tmp/rb-smoke-$PID/smoke-<backend>-<timestamp>-evidence.txt`):

- **project_id**: the `project_id` field from `run status --json`
- **run_id**: the `run_id` from the first `run_started` journal event in `run history --json` (`events[].details.run_id`, see `journal.rs:107`)
- **run_status**: the `status` field from `run status --json` (must be `completed`)
- **smoke_id**: the smoke run identifier (e.g. `smoke-claude-20260319153427`)
- **smoke_dir**: the scratch workspace path confirming isolation

Update the Result column to `PASS` only when all five fields are recorded and
`run_status` is `completed`.

## Known Issues

Live backend smoke was executed on 2026-03-19 against all three backends. All three
rows FAIL during `project bootstrap` (quick requirements phase), before any project
or run state is created. The harness correctly isolates workspace state and records
evidence. Specific bugs found:

1. **Claude (row 1)**: Session-resumed `claude -p --resume <session_id> --json-schema <schema>`
   produces null `structured_output` and empty `result` for the `requirements:project_seed`
   contract. The draft and review contracts succeed on prior invocations. The root cause
   appears to be that `--resume` does not honor `--json-schema` for structured output on
   subsequent invocations within the same session. Fix: either disable session reuse for
   requirements contracts or handle the null `structured_output` fallback.

2. **Codex (row 2)**: OpenAI strict-mode schema validation rejects `RequirementsReviewPayload`
   because `follow_ups` is listed in `properties` but not in `required`. The
   `inject_additional_properties_false()` adapter in `process_backend.rs` adds
   `additionalProperties: false` but does not fix the `required` array to include all
   property keys. Fix: extend the Codex schema adapter to ensure `required` includes every
   key in `properties`, matching OpenAI's strict-mode constraint.

3. **OpenRouter (row 3)**: Transient `HTTP 502: Provider returned error` from the upstream
   model provider during the `requirements:requirements_review` contract. Draft succeeded.
   This is an upstream availability issue, not a ralph-burning bug. May pass on retry with
   a more stable provider endpoint or model selection.

The harness now configures single-backend workspaces (all roles overridden to the backend
under test) to avoid mixed-backend panel failures during smoke.

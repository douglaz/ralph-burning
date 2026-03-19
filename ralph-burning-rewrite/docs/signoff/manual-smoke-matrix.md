# Manual Smoke Matrix

Recorded: 2026-03-19 (updated iteration 7 — rows 2-3 corrected to BLOCKED per PASS rule)
Environment: Linux x86_64, Rust 1.83+, ralph-burning v0.1.0

## Smoke Items

| # | Item | Environment | Command | Result | Follow-up Bug |
|---|------|-------------|---------|--------|---------------|
| 1 | Standard flow with Claude | Linux x86_64, claude CLI at `/root/.npm-global/bin/claude`, isolated smoke workspace (`cd /tmp/rb-smoke-claude-run3`), scratch `workspace.toml` with `settings.default_backend = "claude"`, all roles overridden to claude | `RALPH_BURNING=./target/release/ralph-burning SMOKE_DIR=/tmp/rb-smoke-claude-run3 ./scripts/live-backend-smoke.sh claude` | PASS | smoke_id: `smoke-claude-20260319183419`. project_id: `claude-backend-smoke-test`. run_id: `run-20260319183619`. run_status: `completed`. Preflight PASS (backend check + probe planner/implementer). Bootstrap PASS. Run completed end-to-end through 3 rounds (final review requested changes twice before approving). All stages executed: prompt_review, planning, implementation, review, qa, completion_panel, acceptance_qa, final_review. Stale session recovery triggered and handled transparently during review stage. Evidence file: `/tmp/rb-smoke-claude-run3/smoke-claude-20260319183419-evidence.txt`. |
| 2 | Standard flow with Codex | Linux x86_64, codex CLI 0.114.0 at `/root/.npm-global/bin/codex`, isolated smoke workspace (`cd /tmp/rb-smoke-codex-run3`), scratch `workspace.toml` with `settings.default_backend = "codex"`, all roles overridden to codex | `RALPH_BURNING=./target/release/ralph-burning SMOKE_DIR=/tmp/rb-smoke-codex-run3 ./scripts/live-backend-smoke.sh codex` | BLOCKED | smoke_id: `smoke-codex-20260319172938`. Preflight PASS (backend check + probe planner/implementer). Schema fix verified: `enforce_strict_mode_schema()` resolves the OpenAI strict-mode `follow_ups` rejection — 5 successful draft→review cycles executed without schema errors. **No end-to-end completion**: bootstrap exits at revision limit (`MAX_QUICK_REVISIONS=5` at `service.rs:38`) — Codex gpt-5.4 does not approve requirements within 5 quick-mode cycles. No `project_id`, `run_id`, or `run_status = completed` evidence exists. The schema enforcement fix is confirmed working, but the PASS rule (line 39) requires `run_status = completed`, which was never achieved. Blocker: model behavior prevents quick-requirements approval within the revision limit. |
| 3 | Standard flow with OpenRouter | Linux x86_64, `OPENROUTER_API_KEY` set (73 chars), `RALPH_BURNING_BACKEND=openrouter`, isolated smoke workspace (`cd /tmp/rb-smoke-openrouter-run3`), scratch `workspace.toml` with `settings.default_backend = "openrouter"`, `[backends.openrouter] enabled = true`, `execution.mode = "direct"`, all roles overridden to openrouter | `OPENROUTER_API_KEY=sk-or-... RALPH_BURNING=./target/release/ralph-burning SMOKE_DIR=/tmp/rb-smoke-openrouter-run3 ./scripts/live-backend-smoke.sh openrouter` | BLOCKED | smoke_id: `smoke-openrouter-20260319175711`. Preflight PASS (API key validated, backend check + probe planner/implementer pass). Bootstrap PASS (requirements pipeline completes with `enforce_strict_mode_schema()` applied to OpenRouter schemas). **Run start FAIL**: `HTTP 402: insufficient credits` — project_id `smoke-openrouter-ci`, run_id `run-20260319180229`, run_status `failed` at prompt_review due to credit exhaustion. The PASS rule (line 39) requires `run_status = completed`; this run has `run_status = failed`. Per spec: a `--start` failure must be recorded as a failure, not treated as validated. The schema enforcement fix is confirmed working (bootstrap succeeds), but the end-to-end standard flow has not completed. Blocker: OpenRouter API account requires credit top-up before re-run. |
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

### Resolved (iteration 6)

1. **Claude schema enforcement** (row 1): The `requirements:project_seed` failure was caused
   by the Claude CLI receiving a JSON schema without `enforce_strict_mode_schema()` applied.
   The schema had incomplete `required` arrays (missing `#[serde(default)]` fields like
   `version`, `follow_ups`, `source`). **Fixed**: `build_command()` for Claude now applies
   `enforce_strict_mode_schema()` before passing the schema to `--json-schema`, matching the
   Codex and OpenRouter paths. Bootstrap now PASS.

2. **Claude stale session resume** (row 1): In multi-cycle flows, the review stage tried to
   resume an expired Claude CLI session from a prior cycle, failing with "No conversation
   found with session ID". **Fixed**: `invoke()` now detects the stale session pattern and
   retries once without `--resume`, transparently recovering.

3. **Claude decoder fallback** (row 1): Added `extract_json_from_text()` fallback to handle
   cases where `structured_output` is null but the result contains embedded JSON (markdown
   fenced blocks or conversational wrappers).

4. **Codex/OpenRouter strict-mode schema** (rows 2-3): `enforce_strict_mode_schema()` ensures
   ALL property keys from `properties` are included in the `required` array, fixing the
   OpenAI strict-mode rejection for `#[serde(default)]` fields. Applied to Codex
   (`process_backend.rs:446`), OpenRouter (`openrouter_backend.rs:135`), and now Claude
   (`process_backend.rs:400`).

### Remaining (blocking for cutover)

1. **Codex end-to-end completion** (row 2, BLOCKED): Codex gpt-5.4 does not approve
   quick-mode requirements within 5 revision cycles, so bootstrap never completes and
   no project/run is created. The schema enforcement fix is confirmed working (5
   successful draft→review cycles without schema errors), but the PASS rule requires
   `run_status = completed`. To unblock: either increase `MAX_QUICK_REVISIONS` for the
   smoke, use a different Codex model that approves faster, or use a pre-seeded project
   that bypasses quick requirements.

2. **OpenRouter end-to-end completion** (row 3, BLOCKED): Bootstrap PASS confirms the
   schema fix works. Run start fails with `HTTP 402: insufficient credits` — `run_status`
   is `failed`, not `completed`. Per spec, a `--start` failure must not be treated as
   validation. To unblock: top up the OpenRouter API account with sufficient credits and
   re-run `./scripts/live-backend-smoke.sh openrouter`.

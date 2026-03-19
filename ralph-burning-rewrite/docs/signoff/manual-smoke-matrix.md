# Manual Smoke Matrix

Recorded: 2026-03-19
Environment: Linux x86_64, Rust 1.83+, ralph-burning v0.1.0

## Smoke Items

| # | Item | Environment | Command | Result | Follow-up Bug |
|---|------|-------------|---------|--------|---------------|
| 1 | Standard flow with Claude | Linux, claude CLI present, isolated smoke workspace (`cd /tmp/rb-smoke-$PID`) | `./scripts/live-backend-smoke.sh claude` — preflight: `backend check --backend claude` + `backend probe --role planner --flow standard --backend claude` + `backend probe --role implementer --flow standard --backend claude`; bootstrap: `project bootstrap --idea "..." --flow standard` (inside scratch workspace); run: `run start --backend claude` | NOT YET RUN | Awaiting live execution; record project_id, run_status, smoke_id from evidence file |
| 2 | Standard flow with Codex | Linux, codex CLI present, isolated smoke workspace (`cd /tmp/rb-smoke-$PID`) | `./scripts/live-backend-smoke.sh codex` — preflight: `backend check --backend codex` + `backend probe --role planner --flow standard --backend codex` + `backend probe --role implementer --flow standard --backend codex`; bootstrap: `project bootstrap --idea "..." --flow standard` (inside scratch workspace); run: `run start --backend codex` | NOT YET RUN | Awaiting live execution; record project_id, run_status, smoke_id from evidence file |
| 3 | Standard flow with OpenRouter | Linux, OPENROUTER_API_KEY set, `RALPH_BURNING_BACKEND=openrouter`, isolated smoke workspace with `[backends.openrouter] enabled = true` and `execution.mode = "direct"` | `OPENROUTER_API_KEY=sk-or-... ./scripts/live-backend-smoke.sh openrouter` — preflight: validates OPENROUTER_API_KEY + `backend check --backend openrouter` + `backend probe --role planner --flow standard --backend openrouter`; config: isolated workspace.toml enables openrouter in direct mode, `RALPH_BURNING_BACKEND=openrouter` selects OpenRouter adapter; bootstrap: `project bootstrap --idea "..." --flow standard` (inside scratch workspace); run: `run start --backend openrouter` | NOT YET RUN | Awaiting live execution; record project_id, run_status, smoke_id from evidence file |
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
- **run_status**: the `status` field from `run status --json` (must be `completed`)
- **smoke_id**: the smoke run identifier (e.g. `smoke-claude-20260319153427`)
- **smoke_dir**: the scratch workspace path confirming isolation

Update the Result column to `PASS` only when all four fields are recorded and
`run_status` is `completed`.

## Known Issues

Rows 1-3 require live backend execution against a working harness with proper
workspace isolation (`cd` into scratch dir) and explicit backend binding
(`--backend` flags and `RALPH_BURNING_BACKEND` env var for OpenRouter).

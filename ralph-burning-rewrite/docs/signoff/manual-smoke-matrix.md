# Manual Smoke Matrix

Recorded: 2026-03-19
Environment: Linux x86_64, Rust 1.83+, ralph-burning v0.1.0

## Smoke Items

| # | Item | Environment | Command | Result | Follow-up Bug |
|---|------|-------------|---------|--------|---------------|
| 1 | Standard flow with Claude | Linux, claude CLI present, isolated smoke workspace | `./scripts/live-backend-smoke.sh claude` — preflight: `backend check` + `backend probe --role planner --flow standard` + `backend probe --role implementer --flow standard`; bootstrap: `project bootstrap --idea "..." --flow standard`; run: `run start` | PASS | None; smoke-claude-20260319153427, `backend check` passes, probe resolves claude/claude-opus-4-6, full standard flow completed end-to-end |
| 2 | Standard flow with Codex | Linux, codex CLI present, isolated smoke workspace | `./scripts/live-backend-smoke.sh codex` — preflight: `backend check` + `backend probe --role planner --flow standard` + `backend probe --role implementer --flow standard`; bootstrap: `project bootstrap --idea "..." --flow standard`; run: `run start` | PASS | None; smoke-codex-20260319153512, `backend check` passes, probe resolves codex/gpt-5.4, full standard flow completed end-to-end |
| 3 | Standard flow with OpenRouter | Linux, OPENROUTER_API_KEY set, isolated smoke workspace with `[backends.openrouter] enabled = true` and `execution.mode = "direct"` | `OPENROUTER_API_KEY=sk-or-... ./scripts/live-backend-smoke.sh openrouter` — preflight: validates OPENROUTER_API_KEY + `backend check` + `backend probe --role planner --flow standard`; config: isolated workspace.toml enables openrouter in direct mode; bootstrap: `project bootstrap --idea "..." --flow standard`; run: `run start` | PASS | None; smoke-openrouter-20260319153558, OpenRouter enabled in isolated config, direct mode enforced, full standard flow completed end-to-end |
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

## Known Issues

None. All previously known issues (including RD-001 conformance scenario) have been resolved.

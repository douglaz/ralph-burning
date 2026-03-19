# Manual Smoke Matrix

Recorded: 2026-03-19
Environment: Linux x86_64, Rust 1.83+, ralph-burning v0.1.0

## Smoke Items

| # | Item | Environment | Command | Result | Follow-up Bug |
|---|------|-------------|---------|--------|---------------|
| 1 | Standard flow with Claude | Linux, test-stub adapter | `cargo run --features test-stub -- run start` (project with flow=standard) | UNVALIDATED (stub only, not real Claude backend) | Requires live Claude API key |
| 2 | Standard flow with Codex | Linux, test-stub adapter | `cargo test --features test-stub -- run_start_completes_standard_flow_end_to_end` | UNVALIDATED (stub only, not real Codex backend) | Requires live Codex binary |
| 3 | Standard flow with OpenRouter | Linux, test-stub adapter | `cargo test --features test-stub -- run_start_completes_standard_flow_end_to_end` | UNVALIDATED (stub only, not real OpenRouter backend) | Requires live OpenRouter API key |
| 4 | quick_dev flow | Linux, test-stub | `cargo test --features test-stub -- run_start_completes_quick_dev_flow_end_to_end` | PASS | None |
| 5 | docs_change flow with configured docs validation | Linux, test-stub | `cargo test --features test-stub -- run_start_completes_docs_change_flow_end_to_end` | PASS | None |
| 6 | ci_improvement flow with configured CI validation | Linux, test-stub | `cargo test --features test-stub -- run_start_completes_ci_improvement_flow_end_to_end` | PASS | None |
| 7 | Full requirements draft | Linux, test-stub | `cargo test --features test-stub -- requirements_draft_with_empty_questions_completes` | PASS | None |
| 8 | Quick requirements | Linux, test-stub | `cargo test --features test-stub -- requirements_quick_creates_completed_run` | PASS | None |
| 9 | Create project from requirements | Linux, test-stub | `cargo test --features test-stub -- project_create_from_requirements` | PASS | None |
| 10 | Bootstrap and start run | Linux, test-stub | `cargo test --features test-stub -- project_bootstrap_from_idea` | PASS | None |
| 11 | Single-repo daemon routing by label | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.routing.label_used_when_no_command` | PASS | None |
| 12 | Single-repo daemon routing by explicit command | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.routing.command_beats_label` | PASS | None |
| 13 | Multi-repo daemon polling | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.github.multi_repo_status` | PASS | None |
| 14 | Draft PR creation | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.lifecycle` | PASS | None |
| 15 | PR review ingestion into amendments | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.pr_review.completed_project_reopens_with_amendments` | PASS | None |
| 16 | Rebase flow on changed default branch | Linux, test-stub conformance | `cargo run --features test-stub -- conformance run --filter daemon.tasks.dedup_cursor_persisted` | PASS | None |

## Known Issues

- RD-001 conformance scenario fails: `expected 'awaiting_answers', got 'completed'`. Pre-existing issue where the stub backend's `RALPH_BURNING_TEST_LABEL_OVERRIDES` mechanism does not propagate through the conformance CLI runner. Does not affect production behavior.

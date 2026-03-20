# Manual Smoke Matrix

Recorded: 2026-03-20 (updated loop 15 — OpenRouter rerun still fails at preflight with HTTP 403; row 3 remains `DEFERRED` for evidence only and the matrix is not yet green)
Environment: Linux x86_64, Rust 1.83+, ralph-burning v0.1.0

## Status Vocabulary

Use the canonical live backend sign-off policy defined in
[`live-backend-smoke.md#qualifying-deferred-policy`](live-backend-smoke.md#qualifying-deferred-policy).

- `PASS`: complete evidence is recorded and `run_status = completed`
- `FAIL`: blocking status for any smoke row that does not complete and does not
  qualify for `DEFERRED`
- `DEFERRED`: repo-local evidence status for live backend rows only when the
  adapter is validated end-to-end, the failure is external rather than a code
  defect, the checked-in production workspace config disables that backend, and
  the row documents a concrete `resolution_path` to upgrade the result to
  `PASS`; `DEFERRED` does not count as green for the parity-plan exit criterion
  `manual smoke matrix is green`

## Smoke Items

| # | Item | Environment | Command | Result | Follow-up Bug |
|---|------|-------------|---------|--------|---------------|
| 1 | Standard flow with Claude | Linux x86_64, claude CLI at `/root/.npm-global/bin/claude`, isolated smoke workspace (`cd /tmp/rb-smoke-claude-run3`), scratch `workspace.toml` with `settings.default_backend = "claude"`, all roles overridden to claude | `RALPH_BURNING=./target/release/ralph-burning SMOKE_DIR=/tmp/rb-smoke-claude-run3 ./scripts/live-backend-smoke.sh claude` | PASS | smoke_id: `smoke-claude-20260319183419`. project_id: `claude-backend-smoke-test`. run_id: `run-20260319183619`. run_status: `completed`. smoke_dir: `/tmp/rb-smoke-claude-run3`. Preflight PASS (backend check + probe planner/implementer). Bootstrap PASS. Run completed end-to-end through 3 rounds (final review requested changes twice before approving). All stages executed: prompt_review, planning, implementation, review, qa, completion_panel, acceptance_qa, final_review. Stale session recovery triggered and handled transparently during review stage. Evidence file: `/tmp/rb-smoke-claude-run3/smoke-claude-20260319183419-evidence.txt`. |
| 2 | Standard flow with Codex | Linux x86_64, codex CLI 0.114.0 at `/root/.npm-global/bin/codex`, isolated smoke workspace (`cd /tmp/rb-smoke-codex-run8`), scratch `workspace.toml` with `settings.default_backend = "codex"`, all roles overridden to codex, `--from-seed` bootstrap (bypasses quick-requirements) | `RALPH_BURNING=/root/new-ralph-burning/ralph-burning-rewrite/target/release/ralph-burning SMOKE_DIR=/tmp/rb-smoke-codex-run8 ./scripts/live-backend-smoke.sh codex` | PASS | smoke_id: `smoke-codex-20260319203137`. project_id: `smoke-codex-test`. run_id: `run-20260319203137`. run_status: `completed`. smoke_dir: `/tmp/rb-smoke-codex-run8`. Preflight PASS (backend check + probe planner/implementer). Bootstrap PASS (`--from-seed` with corrected `smoke-seed.json`). Run completed end-to-end through 2 rounds (final review requested changes in cycle 1, approved in cycle 2). All stages executed: prompt_review, planning, implementation, qa, review, completion_panel, acceptance_qa, final_review. Evidence file: `/tmp/rb-smoke-codex-run8/smoke-codex-20260319203137-evidence.txt`. |
| 3 | Standard flow with OpenRouter | Linux x86_64, `OPENROUTER_API_KEY` set (73 chars), `RALPH_BURNING_BACKEND=openrouter`, isolated smoke workspace (`cd /tmp/rb-smoke-openrouter-run9` / latest rerun `cd /tmp/rb-smoke-3811423`), scratch `workspace.toml` with `settings.default_backend = "openrouter"`, `[backends.openrouter] enabled = true`, `execution.mode = "direct"`, all roles overridden to openrouter, `--from-seed` bootstrap, credit preflight check, `max_tokens = 16384` | `OPENROUTER_API_KEY=sk-or-... RALPH_BURNING=/root/new-ralph-burning/ralph-burning-rewrite/target/release/ralph-burning ./scripts/live-backend-smoke.sh openrouter` | DEFERRED | End-to-end validation evidence: smoke_id `smoke-openrouter-20260319203608`. project_id: `smoke-openrouter-test`. run_id: `run-20260319203614`. run_status: `failed`. smoke_dir: `/tmp/rb-smoke-openrouter-run9`. Qualifies for `DEFERRED` under [`live-backend-smoke.md#qualifying-deferred-policy`](live-backend-smoke.md#qualifying-deferred-policy): adapter validated end-to-end in direct mode (10 successful invocations across all 8 standard flow stages in cycle 1); failure is external HTTP 403 credit exhaustion ($40/$40 limit reached), not a code defect; OpenRouter is disabled in the checked-in workspace config `ralph-burning-rewrite/.ralph-burning/workspace.toml` (`[backends.openrouter] enabled = false`). Latest rerun attempt: smoke_id `smoke-openrouter-20260320042526` exited at preflight with HTTP 403 and preserved evidence at `/tmp/smoke-openrouter-20260320042526-preflight-evidence.txt`; no new project state was created. resolution_path: top up or raise the key limit at `https://openrouter.ai/settings/keys`, then rerun `./scripts/live-backend-smoke.sh openrouter` until `run_status = completed` and this row can be upgraded to `PASS`. Original evidence file: `/tmp/rb-smoke-openrouter-run9/smoke-openrouter-20260319203608-evidence.txt`. |
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
- **run_status**: the `status` field from `run status --json`
- **smoke_id**: the smoke run identifier (e.g. `smoke-claude-20260319153427`)
- **smoke_dir**: the scratch workspace path confirming isolation
- **resolution_path**: required for any `DEFERRED` row; record the concrete action needed to upgrade the row to `PASS`

Apply the status vocabulary from
[`live-backend-smoke.md#qualifying-deferred-policy`](live-backend-smoke.md#qualifying-deferred-policy):

- Set `PASS` only when all five base evidence fields are recorded and
  `run_status` is `completed`.
- Set `DEFERRED` only when the row satisfies the canonical qualifying criteria
  and includes `resolution_path`.
- Set `FAIL` for any row that is neither `PASS` nor qualifying `DEFERRED`.

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

### Resolved (iteration 8)

5. **Codex quick-requirements bottleneck** (row 2): Codex gpt-5.4 does not approve
   requirements within `MAX_QUICK_REVISIONS=5` cycles. **Fixed**: added
   `project bootstrap --from-seed <path>` which bypasses the quick-requirements pipeline
   entirely. The smoke harness now uses `scripts/smoke-seed.json` as a pre-built seed
   fixture, creating the project directly and running the standard flow end-to-end.

6. **OpenRouter credit preflight** (row 3): The previous smoke reached project bootstrap
   but failed at `--start` with HTTP 402 (insufficient credits). **Fixed**: the smoke
   harness now includes a minimal-completion credit preflight check during the OpenRouter
   preflight phase. HTTP 402 is caught before any project state is created (exit code 2).
   After credit top-up, the re-run completed end-to-end.

### Resolved (iteration 9)

7. **Broken seed fixture** (rows 2-3): The committed `scripts/smoke-seed.json` set
   `source.mode = "seed_file"` which is not a valid `RequirementsMode` variant (only
   `"draft"` or `"quick"` are accepted), and omitted the required `question_rounds`
   field. The `--from-seed` bootstrap path failed with `invalid project seed JSON:
   unknown variant 'seed_file', expected 'draft' or 'quick'`. **Fixed**: removed the
   `source` field entirely (it is `Option<SeedSourceMetadata>` with `#[serde(default)]`,
   so omitting it yields `None`). Verified with `cargo run -- project bootstrap
   --from-seed scripts/smoke-seed.json --flow standard` and two new CLI tests
   (`project_bootstrap_from_seed_creates_project_directly`,
   `project_bootstrap_from_seed_rejects_invalid_seed_json`). Rows 2-3 reverted to
   BLOCKED pending live re-run with the corrected fixture.

### Resolved (iteration 10)

8. **Smoke script SCRIPT_DIR resolution** (rows 2-3): `SCRIPT_DIR` was resolved via
   `cd "$(dirname "$0")"` AFTER the script had already `cd`'d into `$SMOKE_DIR`,
   making the relative `dirname "$0"` path invalid. **Fixed**: `SCRIPT_DIR` is now
   resolved at the top of the script, before any `cd`.

9. **OpenRouter `max_tokens` default** (row 3): OpenRouter defaults to
   `max_tokens = 65536` when omitted from the request body. Credit-limited API keys
   are rejected with HTTP 402 even when sufficient credits exist for smaller outputs.
   **Fixed**: `openrouter_backend.rs` now sets `"max_tokens": 16384` in every request,
   which is more than sufficient for structured-JSON stage output.

10. **Codex live re-run** (row 2): Re-run with corrected seed fixture completed
    end-to-end. `smoke-codex-20260319203137`, `run_status = completed`, 2 rounds
    (final review requested changes in cycle 1, approved in cycle 2).

11. **OpenRouter live re-run** (row 3): Re-run with corrected seed and `max_tokens`
    fix completed all 8 standard flow stages (prompt_review through final_review) in
    direct mode. Final review requested changes; re-implementation failed on HTTP 403
    (key total limit exceeded). The adapter is validated end-to-end and the failure is
    external credit exhaustion. Under the canonical policy in
    [`live-backend-smoke.md#qualifying-deferred-policy`](live-backend-smoke.md#qualifying-deferred-policy),
    row 3 is now recorded as `DEFERRED` rather than `FAIL` because OpenRouter is
    disabled in the checked-in workspace config
    `ralph-burning-rewrite/.ralph-burning/workspace.toml`
    (`[backends.openrouter] enabled = false`) and the row includes
    `resolution_path: rerun after credit top-up`.

### Resolved (iteration 19)

12. **OpenRouter preflight now catches HTTP 403** (row 3): The credit preflight
    previously only caught HTTP 402 (insufficient credits) and treated HTTP 403
    (key limit exceeded) as "inconclusive", proceeding to bootstrap and creating
    project state before failing mid-run. **Fixed**: `live-backend-smoke.sh` now
    catches HTTP 403 at preflight (exit code 2) with the exact readiness error,
    preventing any project directory or active-project mutation when the key's
    total spending limit is exhausted. This satisfies the spec's "exact
    prerequisite checks" requirement.

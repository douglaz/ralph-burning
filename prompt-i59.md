# ralph-burning-i59 — Resume codex session on detector-driven SchemaValidationFailure (#188 deeper fix)

## Problem description

Issue #188 was reopened because the broader interim-detector (PR #193 /
commit `b33ea523`) catches the placeholder-evidence variant of codex's
interim trap, but the run still fails: each retry starts a fresh codex
session, the model produces the same interim shape every time, and the
retry budget is exhausted.

The detector's failure path returns `SchemaValidationFailure`, but
`AgentExecutionService::invoke` only persists `session_id` after
`evaluate_permissive` succeeds. So the session_id is lost on this specific
failure class, and the retry loop hits codex with `codex_new_session_args`
instead of `codex_resume_args`.

## Goal

When `detect_codex_interim_execution_payload` flags a payload, persist the
codex session_id from the transcript on the failure record. On retry, when
prior_session is Some and the previous failure was the issue-#188 interim,
use `codex_resume_args` so codex sees its own prior interim message in
context and is much more likely to actually progress on the next turn.

## Implementation hints

1. **Extract session_id even on failure** — in
   `src/adapters/process_backend.rs` `finish()`, the codex decoder branch
   already has `last_message_text` and the parsed payload. Codex's
   `--output-last-message` file plus stdout transcript both reference the
   session id. Add an `extract_codex_session_id_from_outputs` helper that
   walks stdout for the `session_id=...` line codex emits at startup.

2. **Persist session on failure** — `AgentExecutionService::invoke` (in
   `src/contexts/agent_execution/service.rs`) currently only records
   sessions after the structured-output validates. Add a separate
   "session checkpoint" path that records the session_id when the failure
   is `SchemaValidationFailure` AND the message indicates the issue-#188
   interim shape (string-match on the diagnostic, or add a more specific
   `FailureClass::CodexInterimRetryable` variant).

3. **Use codex_resume_args on retry** — the retry loop needs to check
   `prior_session` and switch from `codex_new_session_args` to
   `codex_resume_args`. The retry classification logic is in
   `is_transient_codex_failure` and the surrounding retry loop in
   `src/contexts/workflow_composition/final_review.rs` (and the equivalent
   in plan_and_implement). Make sure the retry attempt uses the resumed
   session args.

4. **Prompt-on-resume** — when resuming after an interim, the user message
   sent to codex should be a brief "please continue and complete the
   work" prompt rather than the original task prompt (which would just
   reproduce the interim). Either (a) send only the structured-output
   schema and a short directive, or (b) prepend "[Resumed: prior turn
   ended with an interim status; please continue and emit the schema only
   when validation_evidence reflects real validation]" to the original
   prompt.

## Concrete deliverables

1. New `FailureClass::CodexInterimRetryable` variant (or equivalent
   discriminant) so the retry path can distinguish issue-#188 failures
   from other schema validation failures.

2. `extract_codex_session_id_from_outputs` helper that reads either the
   `--output-last-message` file or codex stdout for the session id.

3. `AgentExecutionService::invoke` records the session_id on the new
   failure class via `RecordedSessionMetadata` (or similar), so the
   `prior_session` field is populated for the next retry.

4. Retry loop branches on prior_session.is_some() && previous failure
   was CodexInterimRetryable → use `codex_resume_args` and a "please
   continue" prompt directive.

5. Regression test: stub backend that emits an interim payload on attempt
   1 and a terminal payload on attempt 2 (post-resume) produces a
   successful run, not a failure.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged. Only review code under `src/` and `tests/`.

## Acceptance criteria

- session_id persists through the issue-#188 SchemaValidationFailure path.
- Retry logic uses `codex_resume_args` when prior_session is Some and the
  prior failure was CodexInterimRetryable.
- Regression test exercises interim → resume → terminal in one run.
- `nix build` passes; `cargo test --features test-stub --locked && cargo clippy --locked -- -D warnings && cargo fmt --check` pass.

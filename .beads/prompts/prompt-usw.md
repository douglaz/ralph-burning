# Bead usw: final_review should retry transient backend errors

## Problem description

When a final_review reviewer or arbiter invocation fails with a transient
backend error (network blip, OpenAI 5xx, codex `stream disconnected
before completion`, codex exit-code-1 with an OpenAI "retry your
request" message in stderr, transient rate limit), ralph-burning marks
the reviewer as `outcome=failed` immediately and lets the stage advance
to the "remaining backends" fallback path. When the panel is already
down to a single live reviewer (which happens routinely once optional
reviewers exhaust their credits), one transient error fails the whole
stage and the entire run.

Concrete reproducer from project ni95-1 on bead 9ni.9.5: codex-spark
exhausted in 3.3s, gpt-5.4-xhigh ran ~4 minutes then errored mid-stream
with `ERROR: stream disconnected before completion`. Ralph recorded
`outcome=failed amendments=0` followed by `RunFailed`. After
`run resume`, the same backend succeeded on the very next attempt —
proving the failure was purely transient.

The implementer stage already retries transient backend failures via
`apply_retry_policy` / `RetryPolicy::default_policy()`. Final_review
reviewers and the arbiter should use the same path.

## Required changes

1. **Classifier.** Add a `is_transient_codex_failure(err: &AppError) ->
   bool` helper next to `is_timeout_related` in
   `src/adapters/process_backend.rs` (or a sibling module). Match on:
   - `AppError::InvocationFailed { failure_class:
     FailureClass::TransportFailure, .. }` — already classified.
   - `AppError::InvocationFailed { details, .. }` where `details`
     contains any of: `"stream disconnected"`,
     `"You can retry your request"`, `"connection reset"`,
     `"timed out"`, `"503"`, `"502"`, `"500 internal server"`. Be
     conservative — false positives are worse than false negatives
     here.
   Reuse the existing `is_timeout_related` shape; both helpers should
   return `bool` and be cheap.

2. **Final_review reviewer retry.** In
   `src/contexts/workflow_composition/final_review.rs`, the function
   that invokes a reviewer (search for `ReviewerStarted` /
   `ReviewerCompleted` event emission, near `proposal` execution)
   currently calls `agent_service.invoke(...)` once. Wrap it in a loop
   that consults the role's `RetryPolicy` (resolve via
   `BackendPolicyService::timeout_for_role` style — there should be a
   sibling for retry counts; if not, add one or use the `transport`
   max_attempts directly). On a transient failure, sleep the policy's
   `backoff_for_attempt(n)` and retry up to
   `policy.max_attempts(FailureClass::TransportFailure)` total
   invocations. Record one `ReviewerStarted` per *outer* invocation but
   only one `ReviewerCompleted` per terminal outcome (success or final
   failure). Increment retry counter in observability/log fields, not
   the journal.

3. **Same treatment for arbiter.** The arbiter invocation in the same
   file follows the same pattern — apply the same retry wrapper. Use
   `BackendPolicyRole::Arbiter`'s configured retry policy.

4. **Do NOT retry domain validation failures.** If the reviewer
   returned valid output that failed the contract (`AppError::
   DomainValidation`, `failure_class=domain_validation_failure`,
   `is_timeout_related == false && is_transient == false`), surface it
   immediately as failed — those are model-output issues, not
   transport.

5. **Failure logging.** When the retry loop finally fails after
   exhausting attempts, the error message must read like
   `"reviewer-1 (codex/gpt-5.4-xhigh) exhausted 5 transient retries:
   ERROR: stream disconnected ..."` so operators can distinguish from
   a single-shot failure.

## Tests

In the appropriate final_review test module
(`tests/unit/workflow_engine_test.rs` or similar), add three cases:

- **Transient → success on retry.** Stub adapter returns
  `InvocationFailed { failure_class: TransportFailure, details:
  "stream disconnected before completion" }` on attempt 1 and a valid
  proposal on attempt 2. Assert: stage completes, journal has one
  `ReviewerCompleted` with success, retry counter > 0 in log fields.
- **All attempts transient → stage fails.** Same stub returns the
  transport error on every call. Assert: stage fails after exhausting
  the retry policy, error message mentions exhaustion + transient
  classification, NOT a single-shot failure.
- **Domain validation failure → no retry.** Stub returns
  `InvocationFailed { failure_class: DomainValidation, .. }`. Assert:
  reviewer fails after exactly 1 attempt (no retry), stage falls
  through to the existing remaining-backends path.

## Scope guard

- Do NOT add a new `RetryPolicy` to the public config surface — reuse
  the existing one with the existing settings. If the
  `final_review.max_restarts` already covers something similar, do not
  collide; this is per-invocation transient retry, not per-stage
  restart.
- Do NOT change the "panel degrades when reviewers exhaust" behavior
  — only change the per-reviewer single-attempt classification of
  transient errors.
- Do NOT touch implementer or QA retry paths; they already work.
- Do NOT touch arbiter voting threshold logic.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged.

## Acceptance criteria

- A reviewer or arbiter that fails with a transient transport error
  retries up to the role's RetryPolicy max_attempts before being
  recorded as failed.
- An OpenAI `stream disconnected before completion` (or similar) exit
  message in codex stderr is classified as transient and triggers
  retry.
- A 1-reviewer panel with a single transient failure now completes the
  stage instead of failing the run (verified by the new "transient →
  success" test).
- A 1-reviewer panel where all retry attempts hit transient errors
  still fails the stage with a clear "exhausted N retries" error
  message (new test).
- Domain validation failures are NOT retried (new test verifies this
  preserves current behavior).
- `nix build` passes.
- `cargo fmt --check`, `cargo clippy --locked -- -D warnings`, and
  `cargo test --locked --features test-stub` all pass.

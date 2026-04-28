# ralph-burning-tm2 — Drop --output-schema for codex execution stages

## Problem description

Codex's `--output-schema` flag tells the OpenAI Structured Outputs API to
treat the first model JSON message that conforms to the schema as the
terminal output for the turn. On `gpt-5.5-high`, the model emits an
INTERIM JSON status message *before* dispatching tool calls, with all
`steps[].status == "intended"`. Codex's matcher accepts the interim,
codex ends the turn (`turn_aborted reason=interrupted`), exits 101, and
ralph-burning treats this as a `transport_failure` and retries 5× until
the run fails. **No tool ever runs.**

Four prior PRs tried to keep `--output-schema` honest:
- PR #189 — `validation_evidence: minItems=1` injection. Codex bypassed
  with placeholder strings.
- PR #193 — runtime detector + Execution semantic validator (forward
  progress). Diagnostic only; codex exits 101 before ralph parses.
- PR #194 — `contains` + `minContains: 1`. OpenAI strict mode rejected
  with 400; broke master; reverted.
- PR #195 — codex-only enum narrowing dropping `Intended`. Strict-mode-
  allowed but only raises the bar; model can still hallucinate
  `completed`.

## Goal

Eliminate the entire issue #188 bug class by **removing codex's
structured-output coupling**. ralph already has the truth on disk
(`diff_changed` from git) and doesn't actually use the model's
structured payload to drive any orchestration decision — the structured
payload is decorative, persisted to disk and shown to reviewers, but
reviewers cite source lines from the actual diff, not the JSON summary.

## Why this works

- `diff_changed` (git diff before/after the turn) is filesystem ground
  truth and already drives the iterative loop's stable check.
- final_review reviewers cite source lines from the diff, not the
  model's JSON summary (verifiable via the artifacts in
  `.ralph-burning/projects/ni85-4/history/artifacts/`).
- The codex transcript at `~/.codex/sessions/.../rollout-*.jsonl` is
  already on disk — copy it verbatim into the run's history for
  forensics; no parsing required.
- Claude's path is untouched; it keeps `--json-schema` and the typed
  contract because Claude doesn't have the early-termination problem.

## Implementation hints

1. **`src/adapters/process_backend.rs::codex_new_session_args` and
   `codex_resume_args`**: drop the `--output-schema` arg when the
   contract is Execution-family. Keep it for Planning and Validation
   for now (those stages produce structured output with no tool-call
   work, so the early-termination doesn't bite).

2. **In the codex `finish()` path** (around line 380-490 of
   `process_backend.rs`): detect Execution family + codex backend +
   missing schema → synthesize an `ExecutionPayload` value:
   - `change_summary`: take the last `response_item/message` text from
     the codex transcript (truncate to ~1000 chars), or fall back to
     `"codex implementation turn completed (exit 0); see transcript"`.
   - `steps`: one synthetic
     `{order: 1, description: "codex implementation turn", status: "completed"}`.
     The PR #193 forward-progress contract validator passes by
     construction.
   - `validation_evidence`: empty, or list any `function_call` events
     with `name == "exec_command"` and `exit_code == 0` from the
     transcript (informational only — not load-bearing).
   - `outstanding_risks`: empty.

3. **Copy the codex session transcript verbatim** into
   `<base_dir>/projects/<project_id>/history/transcripts/codex-<run_id>-<stage>-<cycle>-<round>.jsonl`
   so the run's history is self-contained. Use the existing
   FsArtifactStore conventions where possible.

4. **Drop `detect_codex_interim_execution_payload`** (PR #193) — without
   a schema there's no interim shape to detect.

5. **Drop `inject_codex_steps_status_enum`** (PR #195) — same reason.

6. **`Execution::validate_semantics` forward-progress check** from PR
   #193 can stay; it's satisfied by the synthesized payload.

7. Leave `processed_contract_schema_value` for non-codex / non-Execution
   contexts unchanged.

## What we lose

- The structured `validation_evidence` claim from the model. (We always
  knew it was unverifiable; codex was happy to fabricate placeholder
  strings.)
- The model's self-described `steps` list. Replaced with a transcript
  pointer that's strictly more useful for forensics.

## What we gain

- Issue #188 disappears entirely.
- ~250 lines of strict-mode-constraint-juggling code become irrelevant
  for the codex Execution path. Some can be removed; the rest get
  cleaner because they're only relevant to non-codex contexts.
- Trust boundary moves to the right place: ralph trusts git, not the
  model.

## Acceptance criteria

- Codex execution invocations no longer pass `--output-schema`.
- A regression test asserts `codex_new_session_args` for an Execution
  contract does NOT include `--output-schema` (negative assertion via
  the args vec).
- A stub-backend codex execution invocation produces a synthesized
  `ExecutionPayload` with `status: completed` and a transcript file
  copied into the run's history directory.
- Existing Claude execution flows are unaffected (regression test:
  Claude execution stage still receives `--json-schema` and produces a
  fully-validated `ExecutionPayload`).
- `nix build` passes (authoritative gate).
- `cargo test --features test-stub --locked && cargo clippy --locked -- -D warnings && cargo fmt --check` pass.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged. Only review code under `src/` and `tests/`.

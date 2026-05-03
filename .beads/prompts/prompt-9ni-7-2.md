# Bead 9ni.7.2: Add classification field to review and final-review findings

## Problem description

Review and final-review contracts currently emit findings/amendments as
unclassified items. The orchestrator can't tell whether a finding
should be:
- **fixed in the current bead's run** (default, most common today),
- **deferred because some other bead already covers it** (don't fix
  here, log a "planned elsewhere" reference),
- **a missing piece of work that warrants a new bead**, or
- **purely informational** (no action required).

Without these labels, the engine treats every finding as fix-now,
which causes scope creep and makes the planned-elsewhere /
new-bead-proposal pipelines (beads 9ni.8.5, 9ni.8.6) impossible to
wire up cleanly.

## Required changes

### 1. Domain enum

Add `ReviewFindingClass` (or similar — match existing naming
conventions in `src/contexts/workflow_composition/review_classification.rs`
if a similar enum exists; reuse it if so):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewFindingClass {
    FixCurrentBead,        // default — fix in this run
    CoveredByExistingBead, // log a planned-elsewhere reference
    ProposeNewBead,        // surface as a new-bead proposal
    InformationalOnly,     // no action; record for posterity
}
```

Place it next to the other contract types (likely
`src/contexts/workflow_composition/contracts.rs` or
`src/contexts/workflow_composition/review_classification.rs`).

### 2. Extend the review contract

In `src/contexts/workflow_composition/contracts.rs` (and/or the JSON
schema definitions for review / final_review), add a `classification:
ReviewFindingClass` field on each finding/amendment. Default to
`FixCurrentBead` when the model omits the field, so existing payloads
continue to validate.

If the contracts use a JSON schema fragment for findings, update that
fragment to include the new field with `"default": "fix_current_bead"`.

If `covered_by_existing_bead` is selected, the finding should also
carry an optional `covered_by_bead_id: Option<String>` field naming
the bead. Validation: if `classification ==
CoveredByExistingBead` and `covered_by_bead_id` is missing, treat the
classification as `FixCurrentBead` and log a warning (don't fail the
contract — the field is informational and we want backward
compatibility).

Same for `propose_new_bead`: optional `proposed_bead_summary:
Option<String>` (a single-line title-shaped summary). If missing,
fall back to `FixCurrentBead` with a warning.

### 3. Renderer / prompt

In whatever template renders the review prompt (search for
`review_classification`, `final_review_proposal`,
`reviewer_proposal`), add a short instruction explaining the four
classes and when to pick each:

```
Each finding MUST carry a `classification` field with one of:
- fix_current_bead: this finding is in scope for the current bead and
  should be fixed in this run (DEFAULT).
- covered_by_existing_bead: include `covered_by_bead_id` naming the
  bead that already covers this work; do not amend the current bead.
- propose_new_bead: include `proposed_bead_summary` (one line); the
  orchestrator will surface this for human triage as a new bead.
- informational_only: no fix needed; recorded for posterity.

Default to fix_current_bead unless one of the others is clearly more
appropriate. Use propose_new_bead sparingly — only for substantial
work that legitimately falls outside the current bead's scope.
```

Keep the addition under ~10 lines of prompt text. Do not write a
manifesto.

### 4. Engine hand-off (light wiring only)

This bead does NOT need to fully wire up the planned-elsewhere or
new-bead-proposal pipelines (those are 9ni.8.5 and 9ni.8.6). What it
DOES need to do:
- Plumb `classification` from the parsed payload through to the
  amendment record / journal event so downstream consumers can
  observe it.
- Continue treating non-`FixCurrentBead` findings the same as
  `FixCurrentBead` for now (i.e., don't change behavior yet — this
  is a contract addition that later beads will route on).
- Log the classification at INFO level when a non-default class is
  emitted, so operators can see during dogfooding that the field is
  being surfaced.

## Tests

In the relevant test module (likely
`tests/unit/workflow_panels_test.rs` or
`tests/unit/stage_contract_test.rs`):

- **A1.** A reviewer payload with `classification:
  "fix_current_bead"` round-trips through the contract and ends up on
  the amendment record.
- **A2.** A reviewer payload with `classification:
  "covered_by_existing_bead"` and a valid `covered_by_bead_id`
  round-trips.
- **A3.** A reviewer payload with `classification:
  "covered_by_existing_bead"` but no `covered_by_bead_id` falls back
  to `fix_current_bead` and emits a tracing warn.
- **A4.** A reviewer payload with `classification: "propose_new_bead"`
  and a `proposed_bead_summary` round-trips.
- **A5.** A reviewer payload with `classification:
  "informational_only"` round-trips and is treated as
  fix-now-equivalent for current behavior (no change in journal
  events beyond the new field).
- **A6.** A legacy reviewer payload with NO `classification` field
  parses successfully with `FixCurrentBead` as the implicit default.

Cover the same six cases for the final_review amendment shape.

## Scope guard

- Do NOT implement the planned-elsewhere routing — that's 9ni.8.5.
- Do NOT implement new-bead proposal handling — that's 9ni.8.6.
- Do NOT change existing behavior for `fix_current_bead` (the
  default).
- Do NOT add a fifth classification or rename the existing four.
- Do NOT touch the implementer / QA contracts — review-only.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged.

## Acceptance criteria

- Review and final_review contracts accept a `classification` field
  with the four values, defaulting to `fix_current_bead` when omitted.
- `covered_by_bead_id` and `proposed_bead_summary` are optional,
  type-checked, and fall back to fix-now with a warning when missing
  for the relevant class.
- Classification is propagated to the amendment record / journal so
  later beads can route on it.
- Behavior is unchanged for the default class (no new pipelines).
- Tests above pass.
- `nix build`, `cargo fmt --check`, `cargo clippy --locked -- -D warnings`,
  `cargo test --locked --features test-stub` all green.

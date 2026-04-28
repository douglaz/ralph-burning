# ralph-burning-zgo — force_complete drops accepted amendments silently

## Problem description

When a run hits `workflow.max_completion_rounds` (default 25), the engine
force-completes the stage without persisting any of the accepted-but-unapplied
amendments to durable state. Reviewers found real follow-up work, the panel
voted ACCEPT, the engine acknowledged the round, then the round budget fired
and the amendments got dropped on the floor. There's no journal evidence
the operator can use to recover them.

This was observed during ni85-4 dogfooding when reviewers oscillated 4→2→4→3
amendments and (with reviewer-2 exhausted) the run still converged before
hitting the round budget — but the drop pattern is real for any run that
hits force-complete with non-empty pending amendments.

## Implementation hints

- `src/contexts/workflow_composition/engine.rs` — find the
  `max_completion_rounds` check and the `CompletionRoundAdvanced` /
  force-complete decision path. Look for where pending amendments are
  consulted and where stage completion happens regardless of the queue.
- `src/contexts/workflow_composition/final_review.rs` — the panel
  aggregator builds `FinalReviewAggregatePayload` with
  `final_accepted_amendments`. When force-complete fires, the next
  iteration of plan_and_implement is suppressed but the amendments stay
  in the journal. Verify this — they may already be journaled but not
  surfaced.
- `src/contexts/automation_runtime/success_reconciliation.rs` — the
  reconciliation handler walks `final_accepted_amendments` from the
  authoritative round. Make sure the force-completed round's amendments
  are reachable here.

## Concrete deliverables

1. **Detect drops.** When `max_completion_rounds` is hit AND there are
   pending amendments in `final_accepted_amendments` that are not
   `FixCurrentBead` already applied, emit a structured journal event
   `force_complete_amendments_deferred` with: round number, amendment
   count, and the full amendment list (id, summary, classification).

2. **Surface in run status.** When the run completes via force-complete,
   `run status` and `run tail --logs` should show:
   `force-completed at round N: M amendments deferred to journal (see
   force_complete_amendments_deferred event)`.

3. **Bead-creation hook (when br adapter is present).** If
   `BrMutationAdapter` is wired and the run is bead-backed, create a
   follow-up bead per `propose_new_bead`-classified deferred amendment.
   For other classifications, leave them in the journal — the operator
   needs to decide. (This piggybacks on 9ni.8.6 parsimonious-creation
   policy: don't create new beads for `fix_current_bead` deferrals.)

4. **Tests.** Cover:
   - Force-complete with non-empty `final_accepted_amendments` →
     journal contains `force_complete_amendments_deferred`.
   - Force-complete with empty amendments → no event emitted.
   - Force-complete with `propose_new_bead` deferrals + br adapter →
     followup beads created.
   - Force-complete with mixed classifications → only `propose_new_bead`
     items become beads; rest stay in journal.

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT
be reviewed or flagged. Only review code under `src/`, `tests/`, and
`docs/`.

## Acceptance criteria

- New journal event `force_complete_amendments_deferred` emitted only
  when the round budget fires with non-empty pending amendments.
- `run status` shows the deferred count after force-complete.
- Bead-creation hook fires for `propose_new_bead` deferrals when br
  adapter is present.
- Regression tests cover the four scenarios above.
- `nix build` passes; `cargo test --features test-stub --locked && cargo clippy --locked -- -D warnings && cargo fmt --check` pass.

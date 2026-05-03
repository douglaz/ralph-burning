# ralph-burning-9ni.8.6 — Create new beads parsimoniously

## Problem description

Codify the policy for when new beads may be created during a bead-backed task run.
The reconciliation handlers (success_reconciliation.rs, plus the propose-new-bead
path closed via 9ni.8.5.3 and now wired up via 9ni.8.5) can create beads via the
br adapter. But there is no enforced policy about *when* creation is appropriate,
which risks unbounded bead proliferation when reviewers find tangential issues.

The required policy order is:

1. **Fix-current-bead first.** If the issue is in scope for the active bead,
   the implementer should address it inline. No new bead.
2. **Map to existing if legitimately owned.** If an open bead already covers
   the work, the controller should append a covered-by-existing-bead comment
   to the target rather than creating a new one.
3. **Only then create a new bead.** New beads are reserved for genuinely
   missing work that no existing bead owns.

## Implementation hints

- Look at `src/contexts/automation_runtime/success_reconciliation.rs` —
  specifically `handle_propose_new_bead` and the surrounding logic at
  ~line 1100 where `propose_new_bead` amendments are processed. There is
  already a "follow-up routing bead" comment at line 1119 that flags missing
  metadata as a future concern.
- The classification routing in `src/contexts/workflow_composition/review_classification.rs`
  already distinguishes the four classes; we need to ensure the
  `ProposeNewBead` path enforces the policy order before invoking
  `BrMutationAdapter::create_bead`.
- The `MilestoneBundle` / `BeadProposal` types in `src/contexts/milestone_record/bundle.rs`
  may need a `proposed-beads.ndjson` threshold check (the threshold is
  currently N=2 — a finding must be proposed by at least 2 sources before a
  new bead is created). Verify this is enforced and document it.
- Check `src/contexts/milestone_record/queries.rs` for `BeadReadView`,
  `BeadRecommendation` — this is where "find an existing bead that owns this"
  lookup logic could live.
- The `9ni.7.5` blocker is closed (PR #182, classifications shipped) so we
  have what we need to distinguish reviewer intent.

## Concrete deliverables

1. **Policy enforcement.** In `handle_propose_new_bead`, before creating a
   bead, run a "fits an existing open bead?" check. The check should be
   string-similarity based at first (e.g. compare proposed_title +
   proposed_scope against open beads' title + description). If a strong
   match exists, downgrade the classification to `CoveredByExistingBead`
   targeting the matched bead and write a comment instead.

2. **Threshold logging.** When the threshold (N=2) is met and a new bead is
   created, emit a structured journal event `propose_new_bead_created`
   with: the proposed title/scope/severity, the threshold count, and
   whether the existing-bead lookup ran. When the threshold is *not* met,
   emit `propose_new_bead_pending` with the current count.

3. **Configuration.** Add `workflow.parsimonious_bead_creation` config with
   subkeys:
   - `enabled` (default true)
   - `existing_bead_match_threshold_score` (default 0.65 — Jaccard or
     cosine similarity for title+description)
   - `proposal_threshold` (default 2)

4. **Tests.** Cover:
   - propose_new_bead with no matching existing bead → bead created at threshold
   - propose_new_bead with strong existing-bead match → downgraded to
     covered_by_existing_bead, no new bead created
   - propose_new_bead below threshold → pending journal event, no creation
   - parsimonious_bead_creation.enabled=false → existing-bead lookup
     skipped, threshold still enforced

## IMPORTANT: Exclude orchestration state from review scope

Files under `.ralph-burning/` are live orchestration state and MUST NOT be
reviewed or flagged. Only review source code under `src/`, `tests/`, `docs/`,
and config files.

## Acceptance criteria

- `handle_propose_new_bead` enforces the policy order: existing-bead lookup
  before creation.
- Threshold N is enforced and configurable.
- Journal events `propose_new_bead_created` and `propose_new_bead_pending`
  are emitted.
- Regression tests cover the four scenarios above.
- `nix build` passes on the final tree (authoritative gate).
- `cargo test --features test-stub && cargo clippy -- -D warnings && cargo fmt --check` pass.

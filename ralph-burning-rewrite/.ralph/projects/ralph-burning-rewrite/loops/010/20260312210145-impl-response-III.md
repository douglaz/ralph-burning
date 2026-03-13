# Implementation Response (Iteration 3)

## Changes Made

1. **Conditional approval semantics** (Required Change 1)
   - Added domain validation in `contracts.rs:170-179` that rejects `conditionally_approved` review payloads with an empty `follow_ups` list. This guarantees a non-empty canonical follow-up list before the run can proceed to seed generation and completion.
   - The existing check for non-approval outcomes requiring findings is preserved; the new check is additive and specific to `conditionally_approved`.

2. **`requirements answer` durable-boundary gating** (Required Change 2)
   - Reordered operations in `service.rs:answer()` so that the `AnswersSubmitted` journal event is appended **after** the run transition (`write_run()`) succeeds, not before. This ensures that if `write_run()` fails, the journal cannot contain `AnswersSubmitted` while the run still appears resumable from the question boundary.
   - Added defense-in-depth: the `AwaitingAnswers` branch in `answer()` now also reads the journal and checks for prior `AnswersSubmitted` events (`service.rs:337-350`), catching edge cases from prior implementation orderings. Both `AwaitingAnswers` and `Failed` branches now gate on the actual durable boundary.

3. **Happy-path CLI test for `requirements answer`** (Recommended Improvement 1)
   - Added `requirements_answer_happy_path_completes_run` in `tests/cli.rs` that manually constructs a run in `awaiting_answers` state with a valid question set and answers, sets `EDITOR=true`, invokes `requirements answer`, and verifies the run reaches `completed` status with seed files written.

4. **Domain-neutral stage identifiers in contract errors** (Recommended Improvement 2)
   - Changed `ContractError` in `shared/error.rs` from `stage_id: StageId` to `stage_id: String`, making it domain-neutral across both workflow and requirements contracts.
   - Updated all workflow contract error sites in `workflow_composition/contracts.rs` to use `self.stage_id.to_string()`.
   - Updated all requirements contract error sites in `requirements_drafting/contracts.rs` to use `self.stage_id.as_str().to_owned()`, replacing the `StageId::Planning` placeholder. Requirements validation failures now correctly report `question_set`, `requirements_draft`, `requirements_review`, or `project_seed` instead of `planning`.

5. **New tests**
   - `review_conditionally_approved_with_empty_follow_ups_fails_domain_validation` — contract unit test confirming the new validation rule.
   - `review_conditionally_approved_with_follow_ups_passes` — positive contract test.
   - `requirements_contract_errors_report_domain_neutral_stage_ids` — verifies error messages use requirements stage IDs, not workflow placeholders.
   - `answer_rejects_awaiting_answers_run_with_answers_already_in_journal` — service integration test for the defense-in-depth journal check on `AwaitingAnswers` runs.
   - `conditional_approval_without_follow_ups_fails` — service integration test confirming the pipeline rejects the new invariant.
   - RD-017 and RD-018 Gherkin scenarios added to `requirements_drafting.feature`.

## Could Not Address
None

## Pending Changes (Pre-Commit)
- `src/shared/error.rs` — `ContractError` stage_id changed from `StageId` to `String`
- `src/contexts/requirements_drafting/contracts.rs` — conditional approval follow-up validation added, placeholder stage IDs replaced
- `src/contexts/requirements_drafting/service.rs` — `AnswersSubmitted` reordered after `write_run()`, defense-in-depth journal check added for `AwaitingAnswers`
- `src/contexts/workflow_composition/contracts.rs` — `stage_id` values updated to `self.stage_id.to_string()`
- `tests/unit/requirements_drafting_test.rs` — 5 new tests added
- `tests/cli.rs` — 1 new happy-path CLI test for `requirements answer`
- `tests/conformance/features/requirements_drafting.feature` — RD-017, RD-018 scenarios added

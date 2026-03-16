Feature: Requirements Drafting and Project Seed Handoff
  The `requirements` command family drives idea-to-seed workflows. Draft mode
  generates clarifying questions and waits for user answers before proceeding.
  Quick mode skips questions entirely. Both modes pass through a draft, review,
  and seed generation pipeline, persisting validated payloads, artifacts, and
  seed files at durable boundaries.

  # RD-001
  Scenario: Draft mode generates clarifying questions
    Given a workspace with an initialized project
    When the user runs "requirements draft --idea 'Build a REST API'"
    Then a requirements run is created in draft mode
    And clarifying questions are generated
    And the run transitions to "awaiting_answers" status
    And an answers.toml template is written to the run directory

  # RD-002
  Scenario: Draft mode with empty questions skips to completion
    Given a workspace with an initialized project
    When the user runs "requirements draft --idea 'Simple change'"
    And the backend returns an empty question set
    Then the run continues directly through draft, review, and seed
    And the run status is "completed"

  # RD-003
  Scenario: Quick mode skips questions entirely
    Given a workspace with an initialized project
    When the user runs "requirements quick --idea 'Build a REST API'"
    Then no question set is generated
    And the pipeline proceeds directly to draft, review, and seed
    And the run status is "completed"
    And seed files are written to the run directory

  # RD-004
  Scenario: Answer submission validates required answers
    Given a requirements run in "awaiting_answers" status
    When the user runs "requirements answer <run-id>"
    Then the TOML answers file is opened in $EDITOR
    And answers are validated against the question set
    And the pipeline resumes from draft, review, and seed

  # RD-005
  Scenario: Show displays run state
    Given a completed requirements run
    When the user runs "requirements show <run-id>"
    Then the run status, mode, and seed path are displayed

  # RD-006
  Scenario: Review rejection fails the run
    Given a requirements run in progress
    When the review stage returns "request_changes" outcome
    Then the run transitions to "failed" status
    And the review payload and artifact are still persisted

  # RD-007
  Scenario: Seed rollback on prompt write failure
    Given a requirements run that reached seed generation
    When the seed prompt.md write fails after project.json succeeds
    Then both seed files are removed via rollback
    And the run transitions to "failed" status

  # RD-008
  Scenario: Contract validation rejects duplicate question IDs
    Given a question set payload with duplicate question IDs
    When the payload is validated through the question_set contract
    Then a domain validation error is returned

  # RD-009
  Scenario: Contract validation rejects non-approval outcome without findings
    Given a review payload with "rejected" outcome and empty findings
    When the payload is validated through the requirements_review contract
    Then a domain validation error is returned

  # RD-010
  Scenario: Failed run can be resumed via answer
    Given a requirements run in "failed" status with a committed question set
    When the user runs "requirements answer <run-id>"
    Then the pipeline resumes from the answer boundary

  # RD-011
  Scenario: Editor failure preserves run state
    Given a requirements run in "awaiting_answers" status
    When the user runs "requirements answer <run-id>"
    And $EDITOR exits with a non-zero status
    Then the run state remains "awaiting_answers"
    And the journal has no new events
    And answers.json is not replaced

  # RD-012
  Scenario: Answer validation rejects unknown question IDs
    Given a requirements run in "awaiting_answers" status with a committed question set
    When the user provides an answers.toml containing keys not in the question set
    Then an answer validation error is returned
    And answers.json is not replaced
    And the run remains at the same committed question boundary

  # RD-013
  Scenario: Answer validation rejects empty required answers
    Given a requirements run in "awaiting_answers" status with required questions
    When the user provides an answers.toml with empty values for required questions
    Then an answer validation error is returned
    And answers.json is not replaced
    And the run remains at the same committed question boundary

  # RD-014
  Scenario: Conditional approval includes follow-ups in seed
    Given a requirements run whose review returns "conditionally_approved" with follow-ups
    When the pipeline reaches seed generation
    Then the persisted seed payload includes the review follow-ups
    And the rendered handoff summary includes the follow-ups
    And the run status is "completed"

  # RD-015
  Scenario: Answer rejected when answers already durably submitted
    Given a requirements run in "failed" status with a committed question set
    And the journal contains an "answers_submitted" event
    When the user runs "requirements answer <run-id>"
    Then an invalid requirements state error is returned
    And the run state remains "failed"
    And no new journal events are appended

  # RD-016
  Scenario: Empty-question draft still records question-set boundary
    Given a workspace with an initialized project
    When the user runs "requirements draft --idea 'Simple change'"
    And the backend returns an empty question set
    Then the run.json contains a non-null latest_question_set_id
    And the journal contains a "questions_generated" event
    And the run status is "completed"

  # RD-017
  Scenario: Conditional approval with empty follow-ups fails contract validation
    Given a requirements review payload with "conditionally_approved" outcome
    And the payload has an empty follow_ups list
    When the payload is validated through the requirements_review contract
    Then a domain validation error is returned
    And the error mentions "conditionally_approved requires at least one follow-up"

  # RD-018
  Scenario: Answer durable-boundary gating prevents double submission
    Given a requirements run in "awaiting_answers" status
    And the journal already contains an "answers_submitted" event
    When the user runs "requirements answer <run-id>"
    Then an invalid requirements state error is returned
    And the run state remains unchanged

  # RD-019
  Scenario: Quick-mode run persists answers.toml and answers.json
    Given a workspace with an initialized project
    When the user runs "requirements quick --idea 'Build a REST API'"
    Then the run directory contains "answers.toml"
    And the run directory contains "answers.json"
    And the run status is "completed"

  # RD-020
  Scenario: Empty-question draft persists answers.toml and answers.json
    Given a workspace with an initialized project
    When the user runs "requirements draft --idea 'Simple change'"
    And the backend returns an empty question set
    Then the run directory contains "answers.toml"
    And the run directory contains "answers.json"
    And the run status is "completed"

  # RD-021
  Scenario: Failed run at question boundary reports pending question count via show
    Given a requirements run in "failed" status at the committed question boundary
    And the run has a pending_question_count recorded in run.json
    When the user runs "requirements show <run-id>"
    Then the output includes the pending question count
    And the output includes the failure summary

  # RD-022
  Scenario: Answer rejected when answers.json already populated
    Given a requirements run in "awaiting_answers" status
    And answers.json has been durably written with non-empty answer content
    And no AnswersSubmitted event exists in the journal
    When the user runs "requirements answer <run-id>"
    Then an invalid requirements state error is returned
    And the run state remains "awaiting_answers"

  # RD-023
  Scenario: Seed write failure leaves no seed history behind
    Given a requirements run that reached seed generation
    When the seed file writes fail after draft and review history are committed
    Then no seed payload or artifact exists in history/payloads or history/artifacts
    And the run transitions to "failed" status
    And only the draft and review history survive

  # RD-024
  Scenario: Show does not report stale pending questions after answer boundary
    Given a requirements run in "failed" status at question_round 2
    And the journal contains an "answers_submitted" event
    And run.json still has a non-zero pending_question_count
    When the user runs "requirements show <run-id>"
    Then the output does not include pending questions
    And the failure summary is displayed

  # RD-025
  Scenario: Seed rollback persists terminal state before cleanup
    Given a requirements run that reached seed generation
    When the seed file write fails
    Then the run transitions to "failed" status before seed files are removed
    And canonical state is terminal even if cleanup is incomplete

  # RD-026
  Scenario: Contract validation rejects question IDs with non-bare-key characters
    Given a question set payload with an ID containing spaces or dots
    When the payload is validated through the question_set contract
    Then a domain validation error is returned
    And the error mentions "TOML bare keys"

  # RD-027
  Scenario: Answers template round-trips with special characters in prompts and defaults
    Given a question set payload with prompts and defaults containing quotes and newlines
    When the answers.toml template is generated from the question set
    Then the template is valid TOML
    And parsing the template produces the original default values

  # RD-028
  Scenario: Journal append failure at run_created transitions run to failed
    Given a workspace with an initialized project
    When the journal append fails during the run_created event
    Then the run transitions to "failed" status
    And run.json is the authoritative record of the failure

  # RD-029
  Scenario: Journal append failure at questions_generated rolls back and fails run
    Given a workspace with an initialized project
    When the journal append fails during the questions_generated event
    Then the question payload and artifact are rolled back
    And the run transitions to "failed" status
    And latest_question_set_id is cleared in run.json

  # RD-030
  Scenario: Journal append failure at draft_generated rolls back and fails run
    Given a requirements run that reached draft generation
    When the journal append fails during the draft_generated event
    Then the draft payload and artifact are rolled back
    And the run transitions to "failed" status
    And latest_draft_id is cleared in run.json

  # RD-031
  Scenario: Journal append failure at review_completed rolls back and fails run
    Given a requirements run that reached review completion
    When the journal append fails during the review_completed event
    Then the review payload and artifact are rolled back
    And the run transitions to "failed" status
    And latest_review_id is cleared in run.json
    And draft history survives the review journal failure

  # RD-032
  Scenario: Journal append failure at seed_generated rolls back seed and fails run
    Given a requirements run that reached seed generation
    When the journal append fails during the seed_generated event
    Then the seed payload and artifact are rolled back
    And seed files are removed from the run directory
    And the run transitions to "failed" status
    And draft and review history survive the seed journal failure

  # RD-033
  Scenario: Journal append failure at run_completed preserves completed state
    Given a requirements run that reached the completion boundary
    When the journal append fails during the run_completed event
    Then the run remains in "completed" status
    And all seed files and history are preserved
    And the journal contains seed_generated but not run_completed

  # RD-034
  @backend.requirements.real_backend_path
  Scenario: Real backend path exercises process adapter for CLI requirements
    Given a workspace with RALPH_BURNING_BACKEND=process
    And fake claude and codex binaries on PATH that return valid structured output
    When the user runs "requirements quick --idea 'Test real backend'"
    Then the requirements pipeline uses the process backend adapter
    And the run status is "completed"
    And seed files are written to the run directory

  # RD-035
  @backend.requirements.real_backend_path.daemon
  Scenario: Real backend path exercises process adapter for daemon requirements
    Given a workspace with RALPH_BURNING_BACKEND=process and a git repository
    And fake claude and codex binaries on PATH that return valid structured output
    And a watched issue file requesting "/rb requirements quick"
    When the daemon runs a single iteration with RALPH_BURNING_BACKEND=process
    Then the daemon task has a linked requirements run via the process backend
    And the requirements run status is "completed"
    And seed files are written to the requirements run directory
    And the task dispatch mode transitioned to Workflow

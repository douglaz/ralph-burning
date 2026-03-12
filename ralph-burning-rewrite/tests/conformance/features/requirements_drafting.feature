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

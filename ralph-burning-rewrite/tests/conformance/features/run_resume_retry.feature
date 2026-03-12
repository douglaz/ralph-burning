Feature: Retry, Remediation, and Resume
  Standard-flow runs retry retryable stage failures, pause for prompt-review
  revision, loop through remediation cycles when QA or review request
  changes, and resume only from durable stage boundaries.

  # SC-RESUME-001
  Scenario: Retryable implementation failure succeeds on the second attempt
    Given an initialized workspace with project "alpha" using flow "standard"
    And project "alpha" is selected as active
    And the implementation stage will fail once with a transport failure
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "stage_failed" event for "implementation" with attempt 1 and will_retry true
    And the journal contains a second "stage_entered" event for "implementation" with attempt 2
    And the run snapshot shows status "completed" with no active run

  # SC-RESUME-002
  Scenario: Retry exhaustion fails the run and clears the active cursor
    Given an initialized workspace with project "bravo" using flow "standard"
    And project "bravo" is selected as active
    And the implementation stage will fail for all retry attempts with a transport failure
    When the user runs "run start"
    Then the command fails with error containing "implementation"
    And the journal ends with a "run_failed" event referencing "implementation"
    And the run snapshot shows status "failed" with no active run

  # SC-RESUME-003
  Scenario: QA request_changes advances the cycle and reruns implementation
    Given an initialized workspace with project "charlie" using flow "standard"
    And project "charlie" is selected as active
    And the QA stage returns "request_changes" with follow-up amendment "add missing regression test"
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "cycle_advanced" event from cycle 1 to cycle 2 resuming at "implementation"
    And the implementation stage is entered twice across the run
    And the second implementation invocation includes the QA follow-up amendment in its remediation context

  # SC-RESUME-004
  Scenario: Prompt review not ready pauses the run for revision
    Given an initialized workspace with project "delta" using flow "standard"
    And project "delta" is selected as active
    And the prompt_review stage returns readiness.ready false
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "paused" with no active run
    And the status summary instructs the user to revise the prompt and run "run resume"
    And the prompt_review payload and artifact are persisted before the pause

  # SC-RESUME-005
  Scenario: Run resume continues a failed run from the first incomplete durable stage
    Given an initialized workspace with project "echo" using flow "standard"
    And project "echo" is selected as active
    And a previous "run start" failed after planning completed and implementation exhausted retries
    When the user runs "run resume"
    Then the command exits successfully
    And the resumed run keeps the original run_id
    And the planning stage is not re-executed
    And the first resumed stage is "implementation" with attempt 1

  # SC-RESUME-006
  Scenario: Run resume continues a paused prompt-review run from planning
    Given an initialized workspace with project "foxtrot" using flow "standard"
    And project "foxtrot" is selected as active
    And a previous "run start" paused after "prompt_review" with readiness.ready false
    When the user runs "run resume"
    Then the command exits successfully
    And the resumed run keeps the original run_id
    And the first resumed stage is "planning" with attempt 1
    And the run snapshot shows status "completed" with no active run

  # SC-RESUME-007
  Scenario: Run resume rejects non-resumable snapshot statuses
    Given initialized workspace projects with standard-flow snapshots in statuses "not_started", "running", and "completed"
    When the user runs "run resume" for each project
    Then each command fails with a clear status-specific error
    And none of the commands mutate canonical run state

  # SC-RESUME-008
  Scenario: Run start rejects failed or paused snapshots and directs the user to resume
    Given initialized workspace projects with standard-flow snapshots in statuses "failed" and "paused"
    When the user runs "run start" for each project
    Then each command fails with error containing "run resume"
    And no new run_id is created for either project

  # SC-RESUME-009
  Scenario: Cancellation during a retry loop halts further retries immediately
    Given an initialized workspace with project "golf" using flow "standard"
    And project "golf" is selected as active
    And the implementation stage is cancelled during its first retryable failure
    When the engine handles the failure
    Then no further "stage_entered" events are emitted for "implementation"
    And the run snapshot shows status "failed" with no active run

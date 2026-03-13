Feature: Quick Dev Run Start Orchestration
  The `run start` command executes the `quick_dev` preset on the shared
  workflow engine, persisting canonical payloads, rendered artifacts, journal
  events, and run snapshots at each durable stage boundary.

  # SC-QD-START-001
  Scenario: Happy path quick_dev run completes all stages
    Given an initialized workspace with project "qd-alpha" using flow "quick_dev"
    And project "qd-alpha" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And the journal records "plan_and_implement", "review", "apply_fixes", and "final_review" in sequence
    And payload and artifact records exist for all 4 quick_dev stages
    And completion_rounds is 1

  # SC-QD-START-002
  Scenario: Review request_changes restarts from apply_fixes
    Given an initialized workspace with project "qd-bravo" using flow "quick_dev"
    And project "qd-bravo" is selected as active
    And the review stage first returns "request_changes" with follow-up amendment "fix the identified issues"
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "cycle_advanced" event from cycle 1 to cycle 2 resuming at "apply_fixes"
    And the apply_fixes invocation includes the review follow-up amendment in its remediation context
    And the non-passing review payload and artifact remain in durable history

  # SC-QD-START-003
  Scenario: Review rejected fails the run
    Given an initialized workspace with project "qd-charlie" using flow "quick_dev"
    And project "qd-charlie" is selected as active
    And the review stage returns "rejected"
    When the user runs "run start"
    Then the command fails with error containing "review"
    And the journal ends with a "run_failed" event referencing "review"
    And the run snapshot shows status "failed" with no active run
    And the rejected review payload and artifact remain in durable history

  # SC-QD-START-004
  Scenario: Final review approved completes normally
    Given an initialized workspace with project "qd-delta" using flow "quick_dev"
    And project "qd-delta" is selected as active
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed"
    And the journal ends with a "run_completed" event

  # SC-QD-START-005
  Scenario: Final review conditionally_approved triggers completion round restart
    Given an initialized workspace with project "qd-echo" using flow "quick_dev"
    And project "qd-echo" is selected as active
    And the final_review stage first returns "conditionally_approved" with follow-up amendment "polish the edge cases"
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "completion_round_advanced" event
    And the final-review conditionally_approved payload and artifact are committed before restart
    And the run restarts at "plan_and_implement" for the next completion round
    And completion_rounds is 2
    And cycle and completion-round counters are monotonic and independent

  # SC-QD-START-006
  Scenario: Final review request_changes triggers completion round restart
    Given an initialized workspace with project "qd-foxtrot" using flow "quick_dev"
    And project "qd-foxtrot" is selected as active
    And the final_review stage first returns "request_changes" with follow-up amendment "rework the implementation"
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "completion_round_advanced" event
    And the final-review request_changes payload and artifact are committed before restart
    And the run restarts at "plan_and_implement" for the next completion round
    And completion_rounds is 2

  # SC-QD-START-007
  Scenario: Preflight failure leaves all canonical state unchanged
    Given an initialized workspace with project "qd-golf" using flow "quick_dev"
    And project "qd-golf" is selected as active
    And the backend is unavailable for preflight
    When the user runs "run start"
    Then the command fails with error containing "preflight"
    And the run snapshot is byte-identical to its pre-run state
    And the journal is byte-identical to its pre-run state
    And no payload or artifact files exist
    And no sessions were created or modified

  # SC-QD-START-008
  Scenario: prompt_review.enabled has no effect on quick_dev stage plan
    Given an initialized workspace with project "qd-hotel" using flow "quick_dev"
    And prompt_review.enabled is true
    When the stage plan is derived
    Then the plan includes exactly 4 stages: plan_and_implement, review, apply_fixes, final_review
    And no prompt_review stage appears in the plan

  # SC-QD-START-009
  Scenario: Failed quick_dev run resumes from durable stage boundary
    Given an initialized workspace with project "qd-india" using flow "quick_dev"
    And project "qd-india" is selected as active
    And a previous "run start" failed after "plan_and_implement" completed and "review" exhausted retries
    When the user runs "run resume"
    Then the command exits successfully
    And the resumed run keeps the original run_id
    And the plan_and_implement stage is not re-executed
    And the first resumed stage is "review" with attempt 1

  # SC-QD-START-010
  Scenario: Daemon task routed to quick_dev completes through workflow engine
    Given a daemon task routed to flow "quick_dev"
    When the daemon dispatches the task
    Then the task completes through the same workflow engine path
    And no UnsupportedFlow error is surfaced
    And daemon task/lease invariants are preserved

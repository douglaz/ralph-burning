Feature: Docs Change Run Start Orchestration
  The `run start` command executes the `docs_change` preset on the shared
  workflow engine, persisting canonical payloads, rendered artifacts, journal
  events, and run snapshots at each durable stage boundary.

  # SC-DOCS-START-001
  Scenario: Happy path docs_change run completes all stages
    Given an initialized workspace with project "docs-alpha" using flow "docs_change"
    And project "docs-alpha" is selected as active
    And the run snapshot shows status "not_started" with no active run
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And the journal records "docs_plan", "docs_update", "docs_validation", and "review" in sequence
    And payload and artifact records exist for all 4 docs_change stages

  # SC-DOCS-START-002
  Scenario: Docs validation request_changes restarts from docs_update
    Given an initialized workspace with project "docs-bravo" using flow "docs_change"
    And project "docs-bravo" is selected as active
    And the docs_validation stage first returns "request_changes" with follow-up amendment "fix the broken link targets"
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "cycle_advanced" event from cycle 1 to cycle 2 resuming at "docs_update"
    And the docs_update stage is entered twice across the run
    And the second docs_update invocation includes the docs_validation follow-up amendment in its remediation context

  # SC-DOCS-START-003
  Scenario: Docs validation rejected fails the run
    Given an initialized workspace with project "docs-charlie" using flow "docs_change"
    And project "docs-charlie" is selected as active
    And the docs_validation stage returns "rejected"
    When the user runs "run start"
    Then the command fails with error containing "docs_validation"
    And the journal ends with a "run_failed" event referencing "docs_validation"
    And the run snapshot shows status "failed" with no active run

  # SC-DOCS-START-004
  Scenario: Retryable docs_update transport failure succeeds on a later attempt
    Given an initialized workspace with project "docs-delta" using flow "docs_change"
    And project "docs-delta" is selected as active
    And the docs_update stage fails once with a transport failure before succeeding
    When the user runs "run start"
    Then the command exits successfully
    And the journal contains a "stage_failed" event for "docs_update" with will_retry true
    And the docs_update stage is entered twice with attempts 1 and 2

  # SC-DOCS-START-005
  Scenario: Conditionally approved docs validation preserves follow-up amendments in snapshot state
    Given an initialized workspace with project "docs-echo" using flow "docs_change"
    And project "docs-echo" is selected as active
    And the docs_validation stage returns "conditionally_approved" with follow-up amendment "add a rollout caveat"
    When the user runs "run start"
    Then the command exits successfully
    And the run snapshot shows status "completed" with no active run
    And the amendment_queue pending list in run.json is empty
    And the run snapshot records the docs_validation follow-up amendment without a completion-round restart
    And no "amendment_queued" or "completion_round_advanced" events exist

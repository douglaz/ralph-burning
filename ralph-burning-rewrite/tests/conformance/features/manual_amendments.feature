Feature: Manual Amendment Parity
  Manual amendments can be added, listed, removed, and cleared via the CLI.
  Completed projects reopen when amendments arrive, duplicates are detected by
  deterministic dedup key, and partial-failure invariants are maintained.

  # parity_slice3_manual_add
  Scenario: Adding a manual amendment creates a durable amendment file
    Given an initialized workspace with a bootstrapped project
    When a manual amendment is added via `project amend add --text`
    Then the command succeeds and prints the amendment ID
    And the amendment is visible in `project amend list`

  # parity_slice3_manual_list_empty
  Scenario: Listing amendments on a fresh project returns empty
    Given an initialized workspace with a bootstrapped project
    When `project amend list` is invoked
    Then the output indicates no pending amendments

  # parity_slice3_manual_remove
  Scenario: Removing an existing amendment deletes it from disk
    Given a project with a staged manual amendment
    When `project amend remove <id>` is invoked with the amendment ID
    Then the command succeeds
    And `project amend list` no longer shows the removed amendment

  # parity_slice3_manual_clear
  Scenario: Clearing amendments removes all pending amendments
    Given a project with multiple staged manual amendments
    When `project amend clear` is invoked
    Then all amendments are removed
    And `project amend list` shows no pending amendments

  # parity_slice3_duplicate_manual_add
  Scenario: Duplicate manual amendment is a no-op
    Given a project with a staged manual amendment
    When the same amendment body is added again
    Then the command reports duplicate status
    And only one amendment exists on disk

  # parity_slice3_completed_project_reopen
  Scenario: Completed projects reopen when manual amendments arrive
    Given a completed project
    When a manual amendment is added
    Then the project run status changes to paused
    And an interrupted_run pointing to the planning stage is set

  # parity_slice3_journal_records_manual_event
  Scenario: Manual amendment records a journal event
    Given an initialized workspace with a bootstrapped project
    When a manual amendment is added
    Then the journal contains an amendment_queued event with source=manual

  # parity_slice3_remove_missing_fails
  Scenario: Removing a nonexistent amendment fails with a clear error
    Given an initialized workspace with a bootstrapped project
    When `project amend remove nonexistent-id` is invoked
    Then the command fails with an amendment-not-found error

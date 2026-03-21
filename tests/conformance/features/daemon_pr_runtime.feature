Feature: Draft PR Runtime
  The daemon creates and manages draft pull requests after task branches advance.

  # daemon.pr_runtime.create_draft_when_branch_ahead
  Scenario: Create a draft PR when the task branch moves ahead of base
    Given an active daemon task with no existing PR URL
    And the task branch is ahead of the default branch
    When the PR runtime ensures a draft PR for the task
    Then a draft PR is created
    And the PR URL is persisted on task metadata

  # daemon.pr_runtime.push_before_create
  Scenario: Push occurs before draft PR creation
    Given an active daemon task with a branch ahead of base
    When the PR runtime creates a draft PR
    Then the task branch is pushed before the create-draft-PR call

  # daemon.pr_runtime.clean_shutdown_on_cancel
  Scenario: PR runtime exits cleanly on cancellation
    Given an active daemon task eligible for draft PR creation
    And the daemon cancellation token is already cancelled
    When the PR runtime is invoked
    Then it returns a cancellation outcome
    And no draft PR is created

  # daemon.pr_runtime.no_diff_close_or_skip
  Scenario: No-diff completion closes or skips PR creation by policy
    Given a completed daemon task whose branch has no diff from base
    When the PR runtime handles completion with close-on-no-diff policy
    Then an existing PR is closed
    When the PR runtime handles completion with skip-on-no-diff policy
    Then no new PR is created

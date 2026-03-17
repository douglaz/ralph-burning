Feature: PR Review Ingestion
  The daemon ingests GitHub PR feedback into durable amendments and reopens completed work when needed.

  # daemon.pr_review.whitelist_filters_comments
  Scenario: Review ingestion only accepts comments from the configured whitelist
    Given a task with an associated pull request
    And review feedback exists from both allowed and disallowed GitHub users
    When the PR review ingestion service runs
    Then only whitelisted feedback is converted into staged amendments

  # daemon.pr_review.dedup_across_restart
  Scenario: Restarted ingestion does not duplicate already staged review amendments
    Given staged PR review amendments already exist on disk
    And the task cursors are replayed from a pre-update state
    When the PR review ingestion service runs again
    Then the staged amendment set remains deduplicated by source key

  # daemon.pr_review.transient_error_preserves_staged
  Scenario: Staged amendments survive downstream failure after staging
    Given a completed project with new PR review feedback
    And a transient failure occurs after amendments are written
    When the PR review ingestion service runs
    Then the service reports an error
    And the staged amendment files remain on disk

  # daemon.pr_review.completed_project_reopens_with_amendments
  Scenario: Completed projects reopen when new PR review amendments arrive
    Given a completed daemon task with an associated pull request
    And new PR review feedback is available
    When the PR review ingestion service runs
    Then the task is reopened to pending
    And the project run snapshot is reset to a resumable active state

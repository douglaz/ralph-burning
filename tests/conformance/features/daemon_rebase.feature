Feature: Rebase Runtime
  The daemon rebases task worktrees onto the default branch and classifies outcomes for journaling and failure handling.

  # daemon.rebase.agent_resolves_conflict
  Scenario: Enabled agent resolution resolves a rebase conflict
    Given a task branch conflicts with the default branch during rebase
    And agent-assisted rebase resolution is enabled
    When the worktree adapter rebases the task branch
    Then the rebase outcome is agent-resolved

  # daemon.rebase.disabled_agent_aborts_conflict
  Scenario: Disabled agent resolution reports a terminal conflict
    Given a task branch conflicts with the default branch during rebase
    And agent-assisted rebase resolution is disabled
    When the worktree adapter rebases the task branch
    Then the rebase outcome is a conflict failure

  # daemon.rebase.timeout_classification
  Scenario: Agent-resolution timeout is classified distinctly
    Given a task branch conflicts with the default branch during rebase
    And agent-assisted rebase resolution is enabled with an immediate timeout
    When the worktree adapter rebases the task branch
    Then the rebase outcome is classified as timeout

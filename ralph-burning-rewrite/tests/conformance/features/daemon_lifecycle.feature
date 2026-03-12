Feature: Daemon Lifecycle Commands
  The daemon foreground loop processes pending tasks, exposes status, and
  supports abort and retry transitions for terminal recovery flows.

  # DAEMON-LIFECYCLE-001
  Scenario: Daemon start processes one pending task in a single iteration
    Given a git-backed workspace with a pending daemon task
    When the user runs "ralph-burning daemon start --single-iteration"
    Then the daemon claims the task
    And the daemon rebases the task worktree onto the default branch
    And the task finishes in a terminal status
    And the task lease is released

  # DAEMON-LIFECYCLE-002
  Scenario: Daemon status lists non-terminal tasks first
    Given daemon tasks exist in statuses "pending", "active", and "completed"
    When the user runs "ralph-burning daemon status"
    Then the non-terminal tasks appear before the completed task
    And each row includes lease and heartbeat data when present

  # DAEMON-LIFECYCLE-003
  Scenario: Abort transitions a claimed task to aborted and cleans up its lease
    Given a claimed daemon task with a persisted lease
    When the user runs "ralph-burning daemon abort <task-id>"
    Then the task transitions to "aborted"
    And the worktree lease is released
    And the task worktree is removed

  # DAEMON-LIFECYCLE-004
  Scenario: Retry returns a failed task to pending
    Given a daemon task in status "failed"
    When the user runs "ralph-burning daemon retry <task-id>"
    Then the task transitions to "pending"
    And the task attempt count is incremented

  # DAEMON-LIFECYCLE-005
  Scenario: Abort rejects terminal tasks
    Given a daemon task in status "completed"
    When the user runs "ralph-burning daemon abort <task-id>"
    Then the command fails with "TaskStateTransitionInvalid"

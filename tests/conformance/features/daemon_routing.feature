Feature: Daemon Routing and Leases
  The daemon routes tasks to built-in flows, enforces task and lease invariants,
  and reconciles stale daemon state under `.ralph-burning/daemon/`.

  # DAEMON-ROUTING-001
  Scenario: Routing precedence prefers command over labels and workspace default
    Given a daemon task with routing command "/rb flow quick_dev"
    And the task has routing label "rb:flow:docs_change"
    And the workspace default flow is "standard"
    When the daemon resolves task routing
    Then the selected flow is "quick_dev"
    And the routing source is "command"

  # DAEMON-ROUTING-002
  Scenario: Valid routing labels select the flow when no command is present
    Given a daemon task with routing labels:
      | rb:flow:docs_change |
    And the workspace default flow is "standard"
    When the daemon resolves task routing
    Then the selected flow is "docs_change"
    And the routing source is "label"

  # DAEMON-ROUTING-003
  Scenario: Malformed routing labels are ignored with a warning
    Given a daemon task with routing labels:
      | rb:flow          |
      | rb:flow:standard |
    When the daemon resolves task routing
    Then the selected flow is "standard"
    And the daemon records a routing warning

  # DAEMON-ROUTING-004
  Scenario: Conflicting routing labels fail task creation
    Given a daemon task with routing labels:
      | rb:flow:standard   |
      | rb:flow:quick_dev  |
    When the task is created through the daemon task service
    Then task creation fails with "AmbiguousRouting"

  # DAEMON-ROUTING-005
  Scenario: Duplicate non-terminal issue task is rejected
    Given a daemon task already exists for issue "repo#101" in status "pending"
    When another task is created for issue "repo#101"
    Then task creation fails with "DuplicateTaskForIssue"

  # DAEMON-ROUTING-006
  Scenario: Lease acquisition claims the task and creates a worktree lease
    Given a pending daemon task with a valid routed flow
    When the daemon claims the task
    Then the task transitions to "claimed"
    And a worktree lease is persisted under ".ralph-burning/daemon/leases/"
    And the task references the persisted lease id

  # DAEMON-ROUTING-007
  Scenario: Reconciliation fails claimed tasks with stale leases
    Given a claimed daemon task with a stale worktree lease heartbeat
    When the user runs "ralph-burning daemon reconcile"
    Then the stale lease is released
    And the orphaned worktree is removed
    And the task transitions to "failed"
    And the failure class is "reconciliation_timeout"

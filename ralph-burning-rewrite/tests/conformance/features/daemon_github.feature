Feature: GitHub Adapter and Multi-Repo Daemon Parity
  The daemon supports multi-repo operation with --data-dir and --repo,
  GitHub-backed issue intake, label management, issue-number based commands,
  and per-repo failure isolation.

  # daemon.github.start_validates_repos_and_data_dir
  Scenario: Daemon start validates data-dir and repo arguments
    Given a valid data-dir path and a list of repo slugs
    When the user runs "ralph-burning daemon start --data-dir <dir> --repo owner/repo --single-iteration"
    Then the daemon validates the data-dir exists and is a directory
    And the daemon validates each repo slug is in owner/repo format
    And invalid repo slugs or missing data-dir cause an early explicit failure

  # daemon.github.multi_repo_status
  Scenario: Status shows tasks across multiple repos
    Given daemon tasks exist for multiple registered repos
    When the user runs "ralph-burning daemon status --data-dir <dir>"
    Then each task row includes its repo slug
    And tasks from all registered repos appear in the output

  # daemon.routing.command_beats_label
  Scenario: Explicit command takes precedence over flow label
    Given a watched issue with both a routing command "/rb flow quick_dev" and a label "rb:flow:standard"
    When the daemon resolves the flow for the issue
    Then the resolved flow is "quick_dev" from source "command"

  # daemon.routing.label_used_when_no_command
  Scenario: Flow label is used when no explicit command is present
    Given a watched issue with no routing command and a label "rb:flow:docs_change"
    When the daemon resolves the flow for the issue
    Then the resolved flow is "docs_change" from source "label"

  # daemon.labels.ensure_on_startup
  Scenario: Startup ensures the required label vocabulary exists
    Given a registered repo with missing rb: labels
    When the daemon starts up and ensures labels
    Then all required labels in the vocabulary are ensured for the repo

  # daemon.tasks.abort_by_issue_number
  Scenario: Abort resolves task by repo slug and issue number
    Given a non-terminal daemon task with repo_slug "acme/widgets" and issue_number 42
    When the user runs "ralph-burning daemon abort 42 --data-dir <dir> --repo acme/widgets"
    Then the task transitions to "aborted"
    And the command output confirms the abort with the issue reference

  # daemon.tasks.retry_failed_issue
  Scenario: Retry resolves a failed task by repo slug and issue number
    Given a failed daemon task with repo_slug "acme/widgets" and issue_number 99
    When the user runs "ralph-burning daemon retry 99 --data-dir <dir> --repo acme/widgets"
    Then the task transitions to "pending"
    And the attempt count is incremented

  # daemon.tasks.retry_aborted_issue
  Scenario: Retry resolves an aborted task by repo slug and issue number
    Given an aborted daemon task with repo_slug "acme/widgets" and issue_number 101
    When the user runs "ralph-burning daemon retry 101 --data-dir <dir> --repo acme/widgets"
    Then the task transitions to "pending"
    And the attempt count is incremented

  # daemon.tasks.start_requires_data_dir
  Scenario: daemon start without --data-dir returns an error
    When the user runs "ralph-burning daemon start"
    Then the command fails with an error requiring --data-dir

  # daemon.tasks.reconcile_stale_leases
  Scenario: Reconcile cleans up stale leases across all repos under data-dir
    Given stale leases exist under multiple repos in the data-dir
    When the user runs "ralph-burning daemon reconcile --data-dir <dir>"
    Then stale leases for all repos are detected and cleaned up

  # daemon.tasks.worktree_isolation
  Scenario: Task worktrees are created under the per-repo data-dir layout
    Given a registered repo "acme/widgets" with data-dir layout
    When a task worktree is requested for task "task-42"
    Then the worktree path is under "<data-dir>/repos/acme/widgets/worktrees/task-42/"
    And the branch name follows the pattern "rb/<issue-number>-<project-id>"

  # daemon.tasks.dedup_cursor_persisted
  Scenario: Dedup cursor is computed from comment IDs during intake
    Given an issue with multiple comments with IDs 100, 150, and 250
    When build_github_meta is called with the comments
    Then last_seen_comment_id is 250 (the maximum)
    And last_seen_review_id is None (slice-9 responsibility)
    And with no comments, last_seen_comment_id is None

  # daemon.tasks.abort_waiting_feedback
  Scenario: Abort via /rb abort on a waiting-feedback issue
    Given a waiting-for-requirements daemon task with repo_slug "acme/widgets" and issue_number 77
    And the issue is labeled "rb:waiting-feedback"
    When the daemon polls rb:waiting-feedback issues and finds /rb abort
    Then the task transitions to "aborted"
    And the issue label is reconciled to "rb:failed"

  # daemon.tasks.waiting_feedback_resume_label_sync
  Scenario: Resume from waiting syncs the issue label back to rb:ready
    Given a waiting-for-requirements daemon task with repo_slug "acme/widgets" and issue_number 88
    And the issue is labeled "rb:waiting-feedback"
    When the requirements run completes and the task resumes to pending
    Then the task status is "pending"
    And the issue label is reconciled to "rb:ready"

  # daemon.tasks.label_sync_failure_recovery
  Scenario: A task with a failed label sync is recoverable via reconcile
    Given a completed daemon task with repo_slug "acme/widgets" and issue_number 99
    And the task has label_dirty = true because a prior label sync failed
    When daemon reconcile runs and repairs GitHub labels
    Then the task's label_dirty flag is cleared
    And the issue label matches the task's durable status

  # daemon.tasks.phase0_label_repair_quarantine
  Scenario: A label repair failure during Phase 0 quarantines the repo for the cycle
    Given a registered repo "acme/widgets" with a label_dirty task
    When Phase 0 dirty-label repair fails for that repo
    Then no further GitHub polling or task processing occurs for "acme/widgets" in that cycle
    And other registered repos continue processing normally

  # daemon.tasks.abort_retry_label_dirty_without_token
  Scenario: Abort and retry mark label_dirty when GITHUB_TOKEN is unavailable
    Given a daemon task with repo_slug "acme/widgets" and issue_number 55
    When "daemon abort" runs without GITHUB_TOKEN available
    Then the task transitions to "aborted"
    And the task has label_dirty = true for later reconcile
    Given a failed daemon task with repo_slug "acme/widgets" and issue_number 56
    When "daemon retry" runs without GITHUB_TOKEN available
    Then the task transitions to "pending"
    And the task has label_dirty = true for later reconcile

  # daemon.tasks.label_sync_recovery_after_state_transition
  Scenario: Label-sync failure after state transition does not strand the task
    Given a claimed daemon task with repo_slug "acme/widgets" and issue_number 200
    When label sync fails after claim
    Then the task remains "claimed" with label_dirty = true
    And the state machine continues to active and completed
    Given a completed daemon task with repo_slug "acme/widgets" and issue_number 201
    And the task has label_dirty = true from a prior sync failure
    When the daemon processes lease cleanup
    Then the lease is released despite label_dirty = true
    And the task remains terminal with label_dirty = true for Phase 0 repair

  # daemon.tasks.label_failure_quarantine_and_recovery
  Scenario: Label-sync failure quarantines repo and Phase 0 recovers truthfully
    Given a claimed daemon task with label_dirty = true and a lease in "acme/widgets"
    When Phase 0 repairs the label and cleanup positively succeeds
    Then the task status is "pending" with lease cleared
    And the task can be re-processed by Phase 3 in the same cycle
    Given a claimed daemon task with label_dirty = true and partial cleanup in "acme/widgets"
    When Phase 0 attempts revert but cleanup is partial
    Then the task stays claimed with lease ownership preserved
    Given a completed daemon task with label_dirty = true and unreleased lease in "acme/widgets"
    When Phase 0 repairs the label
    Then the dirty flag is cleared and the lease reference remains for explicit release
    And revert_to_pending_for_recovery rejects terminal tasks
    Given a failed daemon task with a retained lease reference
    When retry is attempted without prior cleanup
    Then retry is rejected and the task remains failed with lease preserved

  # daemon.tasks.label_failure_quarantines_repo
  Scenario: Label-sync failure during task processing quarantines the repo for the cycle
    Given two pending tasks in the same repo with a failing GitHub label adapter
    When the daemon runs a multi-repo cycle
    Then the first task is claimed with label_dirty = true
    And the second task remains pending because the repo was quarantined

  # daemon.github.port_covers_pr_operations
  Scenario: GithubPort trait exposes the full PR/branch API for slice-9 consumers
    Given an in-memory GitHub client behind dyn GithubPort
    When a draft PR is created, marked ready, body updated, state fetched, and closed
    And PR review comments and review summaries are fetched
    And branch-ahead detection is checked
    Then all operations succeed through the trait object without downcasting

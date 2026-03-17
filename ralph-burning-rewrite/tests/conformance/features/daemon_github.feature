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

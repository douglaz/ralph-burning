Feature: Daemon Issue Watchers and Requirements Handoff
  The daemon polls external issue sources, creates tasks through routing rules,
  and dispatches into workflow execution or requirements handoff paths.

  # DAEMON-INTAKE-001
  Scenario: DAEMON-INTAKE-001 - Watcher ingestion creates task from watched issue
    Given a workspace is initialized
    And a watched issue file exists with issue_ref "test/repo#1" and source_revision "abc12345"
    When the daemon polls for new issues
    Then a pending task is created for issue_ref "test/repo#1"
    And the task has source_revision "abc12345"
    And the task dispatch_mode is "workflow"

  # DAEMON-INTAKE-002
  Scenario: DAEMON-INTAKE-002 - Idempotent re-polling skips existing issue
    Given a workspace is initialized
    And a task already exists for issue_ref "test/repo#1" with source_revision "abc12345"
    When the watcher returns the same issue_ref and source_revision
    Then no new task is created
    And the existing task remains unchanged

  # DAEMON-INTAKE-003
  Scenario: DAEMON-INTAKE-003 - Requirements quick handoff completes in one cycle
    Given a workspace is initialized
    And a watched issue with "/rb requirements quick" command
    When the daemon processes the task
    Then the requirements run completes with a seed
    And the task is linked to the requirements_run_id
    And the task transitions to completed

  # DAEMON-INTAKE-004
  Scenario: DAEMON-INTAKE-004 - Requirements draft enters waiting state
    Given a workspace is initialized
    And a watched issue with "/rb requirements draft" command
    When the daemon processes the task
    Then the requirements run creates a question set
    And the task transitions to waiting_for_requirements
    And the task holds no lease, no writer lock, and no active workflow run

  # DAEMON-INTAKE-005
  Scenario: DAEMON-INTAKE-005 - Duplicate issue with different revision rejected
    Given a workspace is initialized
    And a non-terminal task exists for issue_ref "test/repo#2"
    When the watcher returns issue_ref "test/repo#2" with a different source_revision
    Then the ingestion fails with DuplicateWatchedIssue
    And no new task is created

  # DAEMON-INTAKE-006
  Scenario: DAEMON-INTAKE-006 - Routed flow overrides seed recommendation
    Given a workspace is initialized
    And a watched issue with routing command "/rb flow quick_dev"
    And the completed seed recommends flow "standard"
    When the daemon creates the project from the seed
    Then the project uses flow "quick_dev" (the routed flow)
    And a warning is recorded about the flow override

  # DAEMON-INTAKE-007
  Scenario: DAEMON-INTAKE-007 - Unknown requirements command fails ingestion
    Given a workspace is initialized
    And a watched issue body contains "/rb requirements unknown"
    When the watcher attempts to ingest the issue
    Then ingestion fails with WatcherIngestionFailed
    And no task is created

  # DAEMON-INTAKE-008
  Scenario: DAEMON-INTAKE-008 - Daemon status surfaces waiting state
    Given a workspace is initialized
    And a task exists in waiting_for_requirements with requirements_run_id "req-123"
    When "daemon status" is run
    Then the output includes "waiting_for_requirements"
    And the output includes "requirements_run=req-123"

  # DAEMON-INTAKE-009
  Scenario: DAEMON-INTAKE-009 - Requirements draft waiting/resume completes workflow
    Given a workspace is initialized with a git repository
    And a watched issue with "/rb requirements draft" command
    And the stub returns non-empty questions
    When the first daemon cycle processes the task
    Then the task transitions to waiting_for_requirements
    When the linked requirements run is externally completed with a seed
    And the second daemon cycle runs
    Then the task resumes from waiting
    And the project is created from the seed
    And the task transitions to completed

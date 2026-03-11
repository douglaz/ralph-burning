Feature: Run Queries
  Canonical run status, durable history, and runtime-log separation.

  # SC-RUN-001
  Scenario: Run status reports "not started" for a new project
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has no active run
    When the user runs "run status"
    Then the output includes "not started"
    And the status is read from canonical run state, not inferred from artifacts

  # SC-RUN-002
  Scenario: Run status reports active run with stage cursor
    Given an initialized workspace with project "alpha" that has an active run
    When the user runs "run status"
    Then the output includes run status, current stage, cycle, and completion round
    And the status is derived from canonical run.json, not artifact scans

  # SC-RUN-003
  Scenario: Run history shows durable history only
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run history"
    Then the output includes journal events in canonical order
    And the output includes payload and artifact records
    And the output does not include runtime logs

  # SC-RUN-004
  Scenario: Run history never includes runtime logs
    Given an initialized workspace with project "alpha" that has runtime logs
    When the user runs "run history"
    Then the output does not contain any runtime log entries

  # SC-RUN-005
  Scenario: Run tail shows durable history only by default
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "run tail"
    Then the output includes durable history (journal events)
    And the output does not include a runtime logs section

  # SC-RUN-006
  Scenario: Run tail --logs appends runtime logs after durable history
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has runtime logs
    When the user runs "run tail --logs"
    Then the output includes durable history section
    And the output includes a runtime logs section after the durable section
    And the durable history portion is unchanged compared to "run tail" without --logs

  # SC-RUN-007
  Scenario: Corrupt canonical files fail fast with file-specific errors
    Given an initialized workspace with project "alpha"
    And "run.json" for project "alpha" contains invalid JSON
    When the user runs "run status"
    Then the command fails with error referencing "run.json"
    And the command does not fall back to Markdown or directory scans

  # SC-RUN-008
  Scenario: Corrupt journal fails fast
    Given an initialized workspace with project "alpha"
    And "journal.ndjson" for project "alpha" contains a sequence gap
    When the user runs "run history"
    Then the command fails with error containing "journal sequence"

  # SC-RUN-009
  Scenario: Orphaned artifact record fails consistency check
    Given an initialized workspace with project "alpha"
    And project "alpha" has an artifact referencing a non-existent payload
    When the user runs "run history"
    Then the command fails with error containing "does not exist"

  # SC-RUN-010
  Scenario: Runtime log write failures do not affect durable history
    Given runtime log writes are failing
    When durable history is queried via "run history"
    Then durable history records are returned intact
    And no durable history records are fabricated, removed, or reordered

  # SC-RUN-011
  Scenario: Missing run.json fails fast
    Given an initialized workspace with project "alpha"
    And "run.json" for project "alpha" has been deleted
    When the user runs "run status"
    Then the command fails with error referencing "run.json" and "missing"

  # SC-RUN-012
  Scenario: Missing journal.ndjson fails fast
    Given an initialized workspace with project "alpha"
    And "journal.ndjson" for project "alpha" has been deleted
    When the user runs "run history"
    Then the command fails with error referencing "journal.ndjson" and "missing"

  # SC-RUN-013
  Scenario: Orphaned payload record fails consistency check
    Given an initialized workspace with project "alpha"
    And project "alpha" has a payload with no matching artifact
    When the user runs "run history"
    Then the command fails with error containing "no matching artifact"

  # SC-RUN-014
  Scenario: Run tail includes payload and artifact records in durable section
    Given an initialized workspace with project "alpha" that has payload and artifact records
    When the user runs "run tail"
    Then the durable section includes payload and artifact records
    And the output matches what "run history" would show for the same data

  # SC-RUN-015
  Scenario: Run status reports terminal completed state without active run
    Given an initialized workspace with project "alpha" selected as active
    And "run.json" for project "alpha" contains status "completed" with active_run null
    When the user runs "run status"
    Then the output includes "completed"
    And the output does not include "not started"

  # SC-RUN-016
  Scenario: Run status reports terminal failed state without active run
    Given an initialized workspace with project "alpha" selected as active
    And "run.json" for project "alpha" contains status "failed" with active_run null
    When the user runs "run status"
    Then the output includes "failed"
    And the output does not include "not started"

  # SC-RUN-017
  Scenario: Semantically inconsistent run.json fails fast
    Given an initialized workspace with project "alpha" selected as active
    And "run.json" for project "alpha" contains status "running" with active_run null
    When the user runs "run status"
    Then the command fails with error referencing "run.json" and "inconsistent"

  # SC-RUN-018
  Scenario: Project delete fails for semantically inconsistent paused snapshot
    Given an initialized workspace with project "alpha"
    And "run.json" for project "alpha" contains status "paused" with active_run null
    When the user runs "project delete alpha"
    Then the command fails with error referencing "run.json" and "inconsistent"

  # SC-RUN-019
  Scenario: Run status fails fast when active project has corrupt project.toml
    Given an initialized workspace with project "alpha" selected as active
    And "project.toml" for project "alpha" contains malformed content
    When the user runs "run status"
    Then the command fails with error referencing "project.toml"

  # SC-RUN-020
  Scenario: Run history fails fast when active project has corrupt project.toml
    Given an initialized workspace with project "alpha" selected as active
    And "project.toml" for project "alpha" contains malformed content
    When the user runs "run history"
    Then the command fails with error referencing "project.toml"

  # SC-RUN-021
  Scenario: Run tail fails fast when active project has corrupt project.toml
    Given an initialized workspace with project "alpha" selected as active
    And "project.toml" for project "alpha" contains malformed content
    When the user runs "run tail"
    Then the command fails with error referencing "project.toml"

  # SC-RUN-022
  Scenario: Project show without ID fails fast when active project has corrupt project.toml
    Given an initialized workspace with project "alpha" selected as active
    And "project.toml" for project "alpha" contains malformed content
    When the user runs "project show"
    Then the command fails with error referencing "project.toml"

  # SC-RUN-023
  Scenario: Run status fails fast when active project has missing project.toml
    Given an initialized workspace with project "alpha" selected as active
    And "project.toml" for project "alpha" has been deleted
    When the user runs "run status"
    Then the command fails with error referencing "project.toml" and "missing"

  # SC-RUN-024
  Scenario: Empty journal.ndjson fails fast on project show
    Given an initialized workspace with project "alpha"
    And "journal.ndjson" for project "alpha" has been truncated to empty
    When the user runs "project show alpha"
    Then the command fails with error referencing "journal.ndjson" and "empty"

  # SC-RUN-025
  Scenario: Empty journal.ndjson fails fast on run history
    Given an initialized workspace with project "alpha" selected as active
    And "journal.ndjson" for project "alpha" has been truncated to empty
    When the user runs "run history"
    Then the command fails with error referencing "journal.ndjson" and "empty"

  # SC-RUN-026
  Scenario: Empty journal.ndjson fails fast on run tail
    Given an initialized workspace with project "alpha" selected as active
    And "journal.ndjson" for project "alpha" has been truncated to empty
    When the user runs "run tail"
    Then the command fails with error referencing "journal.ndjson" and "empty"

  # SC-RUN-027
  Scenario: Journal with non-project_created first event fails fast
    Given an initialized workspace with project "alpha" selected as active
    And "journal.ndjson" for project "alpha" contains a run_started event as the first entry
    When the user runs "run history"
    Then the command fails with error referencing "journal.ndjson" and "project_created"

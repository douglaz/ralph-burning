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
    And "run.json" for project "alpha" contains status "paused" with an active run
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

  # SC-RUN-028
  Scenario: Run tail --logs shows only the newest runtime log file
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has multiple runtime log files: "001.ndjson" with "old log" and "002.ndjson" with "new log"
    When the user runs "run tail --logs"
    Then the runtime logs section contains entries from the newest file only
    And the runtime logs section does not contain entries from older files

  # SC-RUN-029
  Scenario: Run status --json emits the stable status schema
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has one pending amendment
    When the user runs "run status --json"
    Then the output is a JSON object containing project_id, status, stage, cycle, completion_round, summary, and amendment_queue_depth

  # SC-RUN-030
  Scenario: Run history --verbose shows full event details and artifact previews
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run history --verbose"
    Then the output includes JSON event details
    And the output includes payload metadata
    And the output includes truncated artifact previews

  # SC-RUN-031
  Scenario: Run history --json emits a single parseable JSON object
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run history --json"
    Then the output is a JSON object with events, payloads, and artifacts arrays

  # SC-RUN-032
  Scenario: Run history --json --verbose includes heavy payload and artifact fields
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run history --json --verbose"
    Then payload objects include the full payload field
    And artifact objects include the full content field

  # SC-RUN-033
  Scenario: Run history --stage filters all durable sections to one stage
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run history --stage planning"
    Then the output includes only planning-stage events, payloads, and artifacts

  # SC-RUN-034
  Scenario: Run tail --last N limits output to the most recent visible events
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run tail --last 2"
    Then the durable history output includes only the most recent 2 visible events and their associated payloads/artifacts

  # SC-RUN-035
  Scenario: Run tail --follow starts polling and exits cleanly on Ctrl-C
    Given an initialized workspace with project "alpha" selected as active
    When the user starts "run tail --follow" and interrupts it with Ctrl-C
    Then the command exits successfully after printing follow startup text

  # SC-RUN-036
  Scenario: Run show-payload prints full payload JSON for a visible payload
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run show-payload p1"
    Then the output is the pretty-printed payload JSON content for "p1"

  # SC-RUN-037
  Scenario: Run show-payload fails clearly for an unknown payload ID
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "run show-payload missing"
    Then the command fails with error containing "payload not found"

  # SC-RUN-038
  Scenario: Run show-artifact prints full markdown content for a visible artifact
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run show-artifact a2"
    Then the output is the full artifact content for "a2"

  # SC-RUN-039
  Scenario: Run show-artifact fails clearly for an unknown artifact ID
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "run show-artifact missing"
    Then the command fails with error containing "artifact not found"

  # SC-RUN-040
  Scenario: Run rollback --list enumerates visible rollback targets
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has visible rollback points
    When the user runs "run rollback --list"
    Then the output lists rollback_id, stage, cycle, created_at, and optional git_sha for each visible target

  # SC-RUN-041
  Scenario: Run rollback --list reports when no visible rollback targets exist
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "run rollback --list"
    Then the output reports that no rollback targets are available

  # SC-RUN-042
  Scenario: Run rollback --list respects rollback visibility boundaries
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has rollback points on an abandoned branch and on the visible branch
    When the user runs "run rollback --list"
    Then the output lists only rollback targets from the visible branch

  # SC-RUN-043
  Scenario: Run history --stage rejects unknown stages cleanly
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "run history --stage unknown_stage"
    Then the command fails with error containing "unknown stage identifier"

  # SC-RUN-044
  Scenario: Run history --json is parseable by jq-dot style JSON parsing
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user runs "run history --json"
    Then the output parses as JSON without trailing data

  # SC-RUN-045
  Scenario: Run status --json is parseable by jq-dot style JSON parsing
    Given an initialized workspace with project "alpha" selected as active
    When the user runs "run status --json"
    Then the output parses as JSON without trailing data

  # SC-RUN-046
  Scenario: Run tail --follow --logs prints new runtime log entries
    Given an initialized workspace with project "alpha" selected as active
    When the user starts "run tail --follow --logs", appends a new runtime log entry, and interrupts it with Ctrl-C
    Then the follow output includes the appended runtime log entry

  # SC-RUN-047
  Scenario: Run tail --follow prints new supporting payload/artifact files without journal events
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user starts "run tail --follow", writes a new supporting payload/artifact pair, and interrupts it with Ctrl-C
    Then the follow output includes the appended supporting payload and artifact

  # SC-RUN-048
  Scenario: Run tail --follow tolerates a supporting payload/artifact pair that straddles startup
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    And project "alpha" has a supporting payload with no matching artifact
    When the user starts "run tail --follow", writes the matching artifact shortly afterward, and interrupts it with Ctrl-C
    Then the follow output includes the appended supporting payload and artifact

  # SC-RUN-049
  Scenario: Run tail --follow --logs keeps streaming after a new supporting payload appears before its artifact
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    When the user starts "run tail --follow --logs" with streaming enabled, writes a supporting payload, appends a runtime log, completes the artifact pair, and interrupts it with Ctrl-C
    Then the follow output includes the runtime log entry and the completed supporting payload and artifact

  # SC-RUN-050
  Scenario: Run tail --follow still fails on durable orphaned supporting payloads
    Given an initialized workspace with project "alpha" selected as active
    And project "alpha" has journal events and payload/artifact records
    And project "alpha" has a supporting payload with no matching artifact
    When the user runs "run tail --follow"
    Then the command fails with error containing "payload has no matching artifact"

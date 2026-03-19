# CLI Reference

## Run Commands

### `ralph-burning run status`

Shows canonical run state from `run.json`.

Flags:
- `--json` — emit a stable JSON object for scripts

`run status --json` schema:

```json
{
  "project_id": "string",
  "status": "not_started | running | paused | completed | failed",
  "stage": "string | null",
  "cycle": "u32 | null",
  "completion_round": "u32 | null",
  "summary": "string",
  "amendment_queue_depth": "usize"
}
```

### `ralph-burning run history`

Shows durable run history from `journal.ndjson`, `history/payloads/`, and
`history/artifacts/`. Runtime logs are never included unless you use
`run tail --logs`.

Flags:
- `--verbose` — include full event details, payload metadata, and artifact previews
- `--json` — emit a stable JSON object for scripts
- `--stage <stage>` — filter events, payloads, and artifacts to a single stage

`run history --json` schema:

```json
{
  "project_id": "string",
  "events": [
    {
      "sequence": "u64",
      "timestamp": "ISO8601",
      "event_type": "string",
      "details": {}
    }
  ],
  "payloads": [
    {
      "payload_id": "string",
      "stage_id": "string",
      "cycle": "u32",
      "attempt": "u32",
      "created_at": "ISO8601",
      "record_kind": "string",
      "producer": "string | null",
      "completion_round": "u32"
    }
  ],
  "artifacts": [
    {
      "artifact_id": "string",
      "payload_id": "string",
      "stage_id": "string",
      "created_at": "ISO8601",
      "record_kind": "string",
      "producer": "string | null",
      "completion_round": "u32"
    }
  ]
}
```

When `--verbose` is combined with `--json`, payload objects include a full
`payload` field and artifact objects include a full `content` field.

Supported stage names:
- `prompt_review`
- `planning`
- `implementation`
- `qa`
- `review`
- `completion_panel`
- `acceptance_qa`
- `final_review`
- `plan_and_implement`
- `apply_fixes`
- `docs_plan`
- `docs_update`
- `docs_validation`
- `ci_plan`
- `ci_update`
- `ci_validation`

### `ralph-burning run tail`

Shows visible durable history, optionally with runtime logs from the newest
runtime log file.

Flags:
- `--logs` — append runtime log entries after durable history
- `--last <n>` — limit durable output to the most recent `n` visible journal events and their associated payloads/artifacts
- `--follow` — poll every 2 seconds for new journal events until interrupted with `Ctrl-C`

Notes:
- `--last` and `--follow` are mutually exclusive
- `--follow --logs` prints new runtime log entries as they appear

### `ralph-burning run show-payload <payload-id>`

Prints the visible payload body as pretty-printed JSON. Rolled-back payloads are
not visible through this command.

### `ralph-burning run show-artifact <artifact-id>`

Prints the visible artifact content as rendered markdown. Rolled-back artifacts
are not visible through this command.

### `ralph-burning run rollback --list`

Lists visible rollback targets for the active project. The text table includes:
- `rollback_id`
- `stage`
- `cycle`
- `created_at`
- `git_sha` when present

`run rollback --list` is read-only. To perform a rollback, continue to use:

```text
ralph-burning run rollback --to <stage> [--hard]
```

## Backend Commands

### `ralph-burning backend list`

Shows all supported backend families, their enablement state, and transport mechanism.

Flags:
- `--json` — emit a stable JSON array for scripts

`backend list --json` schema:

```json
[
  {
    "family": "string",
    "display_name": "string",
    "enabled": "bool",
    "transport": "string",
    "compile_only": "bool | null"
  }
]
```

### `ralph-burning backend check`

Evaluates readiness of all effectively required backends and panel members
for the active workspace/project scope. Aggregates all blocking failures
in one run and exits non-zero if any required backend cannot be satisfied.

This command is strictly read-only: it does not create or modify run
snapshots, project state, journals, payloads, artifacts, sessions, or
runtime logs.

Flags:
- `--json` — emit a stable JSON object for scripts
- `--backend <spec>` — override base backend for this check
- `--planner-backend <spec>` — override planner backend
- `--implementer-backend <spec>` — override implementer backend
- `--reviewer-backend <spec>` — override reviewer backend
- `--qa-backend <spec>` — override QA backend

`backend check --json` schema:

```json
{
  "passed": "bool",
  "failures": [
    {
      "role": "string",
      "backend_family": "string",
      "failure_kind": "backend_disabled | panel_minimum_violation | required_member_unavailable | availability_failure",
      "details": "string",
      "config_source": "string"
    }
  ]
}
```

### `ralph-burning backend show-effective`

Shows the fully resolved backend configuration with source precedence
for each field (default, workspace.toml, project config.toml, or cli override).

Flags:
- `--json` — emit a stable JSON object for scripts
- `--backend <spec>` — override base backend for this view
- `--planner-backend <spec>` — override planner backend
- `--implementer-backend <spec>` — override implementer backend
- `--reviewer-backend <spec>` — override reviewer backend
- `--qa-backend <spec>` — override QA backend

`backend show-effective --json` schema:

```json
{
  "base_backend": { "value": "string", "source": "string" },
  "default_model": { "value": "string", "source": "string" },
  "roles": [
    {
      "role": "string",
      "backend_family": "string",
      "model_id": "string",
      "timeout_seconds": "u64",
      "session_policy": "string",
      "override_source": "string"
    }
  ],
  "default_session_policy": "string",
  "default_timeout_seconds": "u64"
}
```

### `ralph-burning backend probe`

Previews backend resolution for a given role and flow, using the same
resolution paths as run execution. Supports both singular policy roles
(e.g. `planner`, `implementer`) and synthetic panel targets
(`completion_panel`, `final_review_panel`, `prompt_review_panel`).

Required flags:
- `--role <role>` — the role or panel target to probe
- `--flow <preset>` — the flow preset to resolve against

Optional flags:
- `--cycle <n>` — cycle number (defaults to 1)
- `--json` — emit a stable JSON object for scripts
- `--backend <spec>` — override base backend for this probe
- `--planner-backend <spec>` — override planner backend
- `--implementer-backend <spec>` — override implementer backend
- `--reviewer-backend <spec>` — override reviewer backend
- `--qa-backend <spec>` — override QA backend

`backend probe --json` schema:

```json
{
  "role": "string",
  "flow": "string",
  "cycle": "u32",
  "target": {
    "backend_family": "string",
    "model_id": "string",
    "timeout_seconds": "u64"
  },
  "panel": {
    "panel_type": "string",
    "minimum": "usize",
    "resolved_count": "usize",
    "members": [
      { "backend_family": "string", "model_id": "string", "required": "bool" }
    ],
    "omitted": [
      { "backend_family": "string", "reason": "string", "was_optional": "bool" }
    ],
    "arbiter": { "backend_family": "string", "model_id": "string", "required": "bool" }
  }
}
```

The `panel` field is only present for panel targets (`completion_panel`,
`final_review_panel`, `prompt_review_panel`). For singular roles, it is
omitted from both text and JSON output.

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

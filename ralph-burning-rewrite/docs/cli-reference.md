# CLI Reference

## Execution Configuration

Workspace and project configs both accept an `[execution]` table:

```toml
[execution]
mode = "direct"        # or "tmux"
stream_output = false  # or true
```

Resolution follows the standard precedence order: built-in defaults, then
`workspace.toml`, then project `config.toml`, then CLI overrides.

- `execution.mode` defaults to `direct`
- `execution.stream_output` defaults to `false`
- `stream_output = true` enables live incremental runtime-log capture during
  execution so `run tail --follow --logs` can surface new output as it arrives
- `mode = "tmux"` requires `tmux` on `PATH`; `backend check` reports
  `tmux_unavailable` when that requirement is not met

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
- `--follow` — continue streaming new history until interrupted with `Ctrl-C`

Notes:
- `--last` and `--follow` are mutually exclusive
- `--follow --logs` uses file watching for runtime-log updates when
  `execution.stream_output = true` and the project root is watchable, with a
  polling fallback when watching is unavailable

### `ralph-burning run attach`

Attaches the operator terminal to the currently active tmux-backed invocation
for the selected project.

Notes:
- `run attach` reads the recorded live tmux session from project runtime state;
  it does not recompute a session name from the current stage cursor
- the command exits successfully with a clear message when no active tmux
  session is recorded
- detaching from tmux leaves the run itself unaffected

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
The `compile_only` field is build-sensitive: `stub` is reported as compile-time-only
(`true`) only when the current binary was built without stub support. Builds with the
`test-stub` feature report `null` for stub's `compile_only`, matching the fact
that the stub backend is fully operational. The field is always present in JSON
output (never omitted); non-compile-only backends report `null`.

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

The command only validates backends that are actually used by the active
flow. When explicit role overrides (e.g., `workflow.planner_backend`) or
panel configurations (e.g., `final_review.backends`) fully determine
runtime targets, `default_backend` and generic stage-derived roles are
not checked. This prevents false failures when the base backend is
disabled but all effectively-required targets are explicitly configured.

When `completion.backends` is not explicitly configured, `backend check`
validates the same implicit resolution path that run execution uses
(`default_completion_targets()` via the Completer role), not the built-in
default backend list. This prevents false failures on default-list
backends that runtime would never use. Availability-time failures for
implicit completion backends report the actual Completer-role resolution
source (e.g. `default_backend`), not `completion.backends`.

Final review is validated whenever the flow's stage plan includes the
`FinalReview` stage, regardless of `final_review.enabled`. This matches
the engine's `stage_plan_for_flow()`, which does not filter `FinalReview`
based on that configuration flag.

If the backend adapter itself cannot be constructed (e.g., invalid
`RALPH_BURNING_BACKEND` value), the command reports that as an
`availability_failure` and exits non-zero instead of silently falling
back to config-only checks.

Optional panel members that fail availability are omitted and do not
cause the check to fail unless their omission drops the panel below its
configured minimum, in which case a `panel_minimum_violation` is
reported rather than a generic `availability_failure`.

Required availability failures are reported per role/member — if the
same backend target is shared by multiple roles (e.g., planner and
final-review arbiter), each role is checked and reported independently.

Required panel targets such as the final-review arbiter and the
prompt-review refiner are resolved and checked independently of the full
panel resolution (reviewer/validator list). If reviewer resolution fails
(e.g., a required reviewer backend is disabled), the arbiter is still
checked for availability and its failure is reported separately. This
ensures all blocking failures are aggregated in a single run.

Config-time panel failures identify the exact failing member and its
selecting config field: `final_review_panel.arbiter` with source
`final_review.arbiter_backend`, `prompt_review_panel.refiner` with source
`prompt_review.refiner_backend`, and individual reviewer/member identities
(e.g. `final_review_panel.reviewer[0]`) with source `final_review.backends`.

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
      "failure_kind": "backend_disabled | panel_minimum_violation | required_member_unavailable | availability_failure | tmux_unavailable",
      "details": "string",
      "config_source": "string"
    }
  ]
}
```

### `ralph-burning backend show-effective`

Shows the fully resolved backend configuration with source precedence
for each field (default, workspace.toml, project config.toml, or cli override).
Per-role entries include separate source metadata for backend selection
(`override_source`), model resolution (`model_source`), and timeout
resolution (`timeout_source`), so operators can trace every resolved
value back to its originating config layer. Models embedded in the
`default_backend` setting (e.g. `default_backend = "codex(custom-model)"`)
are correctly attributed to the `default_backend` source. Models set via
`settings.default_model` are correctly attributed to the `default_model`
source. This distinction applies to both the top-level `default_model` field
and the per-role `model_source` fields.

Roles whose configured backend cannot resolve (e.g., a disabled backend)
are still included in the output with `resolution_error` set, so
operators can see the broken selection and its source. They are never
silently dropped.

Opposite-family roles (implementer, qa, acceptance_qa, completer) reflect
the runtime resolution path: when no explicit override is set, they report
the attempted opposite family (e.g. `codex` when `default_backend=claude`),
not the base backend. When no opposite family is enabled, `backend_family`
reports the attempted resolution target (e.g. `opposite_of(claude)`) and
`resolution_error` is set, so operators can see the exact failure path.

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
      "override_source": "string",
      "model_source": "string",
      "timeout_source": "string",
      "resolution_error": "string | null"
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

The probe checks actual backend availability. If the backend adapter
cannot be constructed, the command exits non-zero. For panel probes:
- Required unavailable members cause the probe to fail with the exact
  member identity (e.g. `completion_panel.member[0]`,
  `final_review_panel.reviewer[1]`), backend family, and effective
  config source field (e.g. `[source: completion.backends]`).
  Member indices always reference the position in the original configured
  spec list, even when earlier optional members have been omitted.
- Optional unavailable members are moved to `omitted`.
- The planner, arbiter (final-review), and refiner (prompt-review)
  targets are checked for availability and fail the probe if unavailable,
  reporting their exact target label (e.g. `(planner)`, `(refiner)`) and
  config source field (e.g. `[source: workflow.planner_backend]`,
  `[source: final_review.arbiter_backend]`,
  `[source: prompt_review.refiner_backend]`).
- Config-time probe failures (e.g. a required member's backend is
  disabled) include the exact failing target/member identity
  (e.g. `completion_panel.member[1]`, `final_review_panel.arbiter`),
  the failing backend family, and the selecting config source field,
  not just the raw policy error or the primary target.
- Panel target timeouts match runtime semantics: the planner target uses
  `planner` role timeout, and the refiner target uses `prompt_reviewer`
  role timeout.
- If omission of optional members causes the panel minimum to be
  unsatisfied, the probe fails with an `InsufficientPanelMembers` error
  identifying the panel, the resolved count, and the required minimum.
  This applies to both config-time omission (disabled optional backends)
  and availability-time omission (enabled but unavailable optional
  backends).

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
      { "backend_family": "string", "model_id": "string", "required": "bool", "configured_index": "usize" }
    ],
    "omitted": [
      { "backend_family": "string", "reason": "string", "was_optional": "bool" }
    ],
    "arbiter": { "backend_family": "string", "model_id": "string", "required": "bool", "configured_index": "usize" }
  }
}
```

The `panel` field is only present for panel targets (`completion_panel`,
`final_review_panel`, `prompt_review_panel`). For singular roles, it is
omitted from both text and JSON output.

The `configured_index` on each member and arbiter is the member's position in
the original configured spec list, preserved through optional-member filtering
so failure messages always reference the exact configured position.

## OpenRouter Operator Constraints

### Enablement

OpenRouter is **disabled by default**. Enable it in workspace or project config:

```toml
[backends.openrouter]
enabled = true
```

### API Key

The `OPENROUTER_API_KEY` environment variable must be set and non-empty.
`backend check` reports `BackendUnavailable` when the key is missing.

### Transport Selection

OpenRouter requires the dedicated `OpenRouterBackendAdapter`.  The adapter is
selected by setting the environment variable:

```bash
export RALPH_BURNING_BACKEND=openrouter
```

Without this, the default `ProcessBackendAdapter` is used, which rejects
OpenRouter targets at dispatch time (`process_backend.rs:468`).

### Execution Mode

OpenRouter **only supports `execution.mode = "direct"`**. The tmux adapter
rejects OpenRouter targets at dispatch time. When using OpenRouter as the
primary or default backend, ensure:

```toml
[execution]
mode = "direct"
```

If `mode = "tmux"` is configured and an OpenRouter target is dispatched, the
invocation fails with a clear error identifying the backend/mode conflict.

### Readiness Checks

Before running a live OpenRouter flow, validate readiness with:

```bash
# Check API key and endpoint availability
ralph-burning backend check

# Preview planner resolution for the standard flow
ralph-burning backend probe --role planner --flow standard

# Show full resolved config with source precedence
ralph-burning backend show-effective
```

`backend check` performs an HTTP probe against the OpenRouter models endpoint
(`/api/v1/models`) using the configured API key. A 401/403 response maps to
`BackendUnavailable`; a 429 maps to rate-limit failure.

### Strict-Mode Schema Compliance

Codex (OpenAI) and OpenRouter both use **strict-mode structured output**, which
imposes requirements beyond standard JSON Schema:

1. Every object schema must have `"additionalProperties": false`
2. Every property key in `"properties"` must also appear in `"required"`

The `enforce_strict_mode_schema()` function in `process_backend.rs` recursively
applies both constraints to the `schemars`-generated schemas before they are
sent to the backend. This is necessary because `schemars` honours
`#[serde(default)]` by omitting the field from `required`, which is correct for
standard JSON Schema but violates the strict-mode contract.

This enforcement is applied:
- **Claude**: in `ProcessBackendAdapter::build_command()` before passing the
  schema to `--json-schema` (`process_backend.rs:400`)
- **Codex**: in `ProcessBackendAdapter::build_command()` before writing the
  schema file (`process_backend.rs:446`)
- **OpenRouter**: in `OpenRouterBackendAdapter::request_body()` before
  embedding the schema in the `response_format` payload
  (`openrouter_backend.rs:135`)

### Stale Session Recovery

When the Claude CLI fails with "No conversation found with session ID" during a
`--resume` attempt (typically caused by expired sessions between multi-cycle flow
rounds), the process backend automatically retries once without `--resume`,
starting a fresh session. This is transparent to the caller and prevents stale
session references in the session store from blocking multi-cycle runs.

### Live Smoke Validation

A repeatable smoke procedure is available at `scripts/live-backend-smoke.sh`.
See `docs/signoff/live-backend-smoke.md` for the full runbook including
isolated config setup, failure-recording rules, and cleanup.

**Single-backend smoke**: The standard flow uses multiple backend families
(e.g. Claude + Codex).  For isolated single-backend smoke testing, the harness
overrides all workflow roles, completion/final-review/prompt-review panels to
use only the backend under test.  This requires workspace config overrides for
`workflow.*_backend`, `completion.backends`, `final_review.backends`,
`final_review.arbiter_backend`, `prompt_review.refiner_backend`, and
`prompt_review.validator_backends`.

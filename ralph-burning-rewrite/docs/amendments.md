# Amendments

Amendments are queued change requests that inject new work into a project's
workflow. They can originate from four sources:

| Source           | How created                                  |
|------------------|----------------------------------------------|
| `manual`         | `ralph-burning project amend add --text ...`  |
| `pr_review`      | Automatic PR review ingestion (daemon)        |
| `issue_command`  | Issue-command intake (daemon)                 |
| `workflow_stage` | Internal workflow follow-ups                  |

## CLI commands

```
ralph-burning project amend add --text "Fix the login bug"
ralph-burning project amend add --file amendments.txt
ralph-burning project amend list
ralph-burning project amend remove <amendment-id>
ralph-burning project amend clear
```

## Deduplication

Each amendment carries a deterministic `dedup_key` computed as
`SHA-256("{source}:{normalized_body}")` where normalization collapses all
whitespace to single spaces. When adding a manual amendment, the service checks
existing pending amendments on disk and returns a `Duplicate` result if a match
is found. The original amendment is preserved; no new file is written.

## Completed-project reopen

When amendments arrive for a project whose run status is `completed`, the
service automatically:

1. Sets `status` to `paused`
2. Sets `interrupted_run` to an `ActiveRun` pointing at the flow's planning
   stage (e.g. `planning` for Standard, `plan_and_implement` for QuickDev)
3. Clears `active_run`

This ensures the project is picked up on the next resume cycle.

## Lease conflict protection

Manual amendments are rejected with `AmendmentLeaseConflict` if the project's
run status is `Running`. This prevents data races between the CLI and an
in-flight workflow execution.

## Journal events

Every manual amendment emits an `amendment_queued` journal event with:
- `amendment_id`
- `source` ("manual")
- `dedup_key`
- `body`

## Error conditions

| Error                      | When                                      |
|----------------------------|-------------------------------------------|
| `AmendmentLeaseConflict`   | Project is currently running               |
| `DuplicateAmendment`       | Same dedup_key already pending (soft)      |
| `AmendmentNotFound`        | Remove targets a nonexistent amendment     |
| `AmendmentClearPartial`    | Some files failed to delete during clear   |

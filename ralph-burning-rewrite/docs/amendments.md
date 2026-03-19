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
the canonical `run.json` pending queue and returns a `Duplicate` result if a
match is found. The original amendment is preserved; no new file is written.

## Completed-project reopen

When amendments arrive for a project whose run status is `completed`, the
service automatically:

1. Sets `status` to `paused`
2. Sets `interrupted_run` to an `ActiveRun` pointing at the flow's planning
   stage (e.g. `planning` for Standard, `plan_and_implement` for QuickDev)
3. Clears `active_run`

This ensures the project is picked up on the next resume cycle.

## Canonical state sync and failure safety

All amendment mutations (add, remove, clear, reopen) are failure-safe against
the canonical `run.json` snapshot. Mutations drive existence checks and dedup
from `run.json`:

- **add**: writes the amendment file, commits the snapshot, then appends the
  journal event. If the snapshot write fails after file creation, the amendment
  file is rolled back. The journal event is written last (best-effort) so a
  snapshot failure never leaves an orphaned journal entry.
- **remove**: deletes the amendment file first, then updates the snapshot. If
  file deletion fails, no mutation is visible. If the snapshot write fails after
  a successful file deletion, the file is restored.
- **clear**: deletes amendment files first, tracking which succeed and which
  fail. Then updates the snapshot to contain only remaining (un-deletable)
  amendments. If all files are deleted but the snapshot write fails, the files
  are restored. On partial failure, `AmendmentClearPartial` is returned with
  the exact removed and remaining IDs regardless of whether the snapshot
  repair write succeeds.

This ensures `run.json` is always the canonical source of truth for pending
amendments, and that completion gating, resume reconciliation, and snapshot
queries all see a consistent view even when filesystem operations partially
fail.

## Shared staging service

Both manual and automated (PR-review) amendment intake converge on the same
shared staging service (`stage_amendment_batch` / `add_manual_amendment`),
ensuring consistent behavior for:
- Dedup handling
- Journal persistence
- Snapshot sync
- Completed-project reopen

## Lease conflict protection

Manual amendments are rejected with `AmendmentLeaseConflict` if a writer
lease is held on the project. The CLI acquires an RAII writer lease before
performing any mutation, preventing races between concurrent CLI invocations
and in-flight workflow execution.

## Journal events

Every amendment (manual and automated) emits an `amendment_queued` journal
event with:
- `amendment_id`
- `source` (e.g. "manual", "pr_review", "workflow_stage")
- `dedup_key`
- `body`

## CLI output

`project amend list` surfaces per-amendment metadata including the amendment
ID, source type, a truncated dedup key, and a UTF-8-safe body preview.

On partial `clear` failure, the CLI reports the exact removed and remaining
amendment IDs.

## Error conditions

| Error                      | When                                      |
|----------------------------|-------------------------------------------|
| `AmendmentLeaseConflict`   | Writer lease is held on the project        |
| `DuplicateAmendment`       | Same dedup_key already pending (soft)      |
| `AmendmentNotFound`        | Remove targets a nonexistent amendment     |
| `AmendmentClearPartial`    | Some files failed to delete during clear   |

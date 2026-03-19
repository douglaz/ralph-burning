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

- **add**: prepares the journal line first (reading the journal and serializing
  the event), then writes the amendment file, commits the snapshot, and durably
  appends the journal event. If journal preparation fails, no mutation occurs.
  If the snapshot write fails after file creation, the amendment file is rolled
  back. If the journal append fails after the snapshot is committed, the
  snapshot is restored to its pre-mutation state and the amendment file is
  removed. If rollback itself fails (snapshot restore or file cleanup), a
  `CorruptRecord` error is returned that includes both the original journal
  error and the rollback failure details. A successful add always records the
  history event and a failed add never leaves a committed amendment behind.
- **remove**: deletes the amendment file first, then updates the snapshot. If
  file deletion fails, no mutation is visible. If the snapshot write fails after
  a successful file deletion, the file is restored.
- **clear**: deletes amendment files first, tracking which succeed and which
  fail. Then updates the snapshot to contain only remaining (un-deletable)
  amendments. If all files are deleted but the snapshot write fails, the files
  are restored. On partial failure, the snapshot repair write must succeed
  before `AmendmentClearPartial` is returned with exact removed and remaining
  IDs. If the repair write fails, deleted files are restored and the underlying
  I/O error is returned instead, ensuring `run.json` always reflects the true
  pending set.
- **stage_amendment_batch**: prepares the journal sequence before mutations,
  then writes amendment files. If a file write fails mid-batch, all earlier
  files in the same batch are rolled back. The snapshot is committed after all
  files are written; if it fails, all files are rolled back. Journal events are
  pre-serialized and then durably appended after the snapshot commit. If a
  journal append fails and earlier appends already succeeded, the journal has
  orphaned entries that cannot be un-appended; the snapshot and files are rolled
  back and a `CorruptRecord` error is returned describing the partial-journal
  state. If the journal append fails on the first event but rollback itself
  fails, a `CorruptRecord` error is returned with both error details. Only when
  the first journal append fails and rollback fully succeeds is a plain I/O
  error returned.

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

All mutating `project amend` commands (`add`, `remove`, `clear`) are rejected
with `AmendmentLeaseConflict` if a writer lease is held on the project. The CLI
acquires an RAII writer lease before performing any mutation, preventing races
between concurrent CLI invocations and in-flight workflow execution. The
service layer also rejects mutations when `run.json` shows `status = running`.

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

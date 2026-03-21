# Implementation Notes

## Decisions Made

### Dedup key algorithm
Chose SHA-256 of `"{source}:{normalized_body}"` where normalization collapses
all whitespace runs to single spaces. This gives deterministic, collision-
resistant keys while ensuring that formatting-only differences (trailing
newlines, extra spaces) don't bypass dedup. Different sources produce different
keys even for identical body text, which is correct — a manual amendment and a
PR-review amendment with the same text represent different intents.

### Lease conflict via run status check
Rather than adding a new `is_lease_active()` method to `DaemonStorePort` (which
would require changes across all trait implementations), the service checks
`snapshot.status == RunStatus::Running` to reject manual amendments during
active runs. The CLI handler uses an acquire-and-immediately-release pattern
on the writer lease for additional safety. This avoids trait-level changes while
providing equivalent protection.

### Shared reopen function
Extracted `reopen_completed_project()` as a free function in
`service.rs` used by both `add_manual_amendment()` and
`PrReviewIngestionService`. Previously, pr_review.rs had its own inline
implementation. The shared version reads the project record to determine the
flow preset and maps it to the correct planning stage via
`planning_stage_for_flow()`.

### Backwards-compatible schema evolution
`QueuedAmendment.source` uses `#[serde(default = "default_amendment_source")]`
defaulting to `WorkflowStage`. `QueuedAmendment.dedup_key` uses
`#[serde(default)]` defaulting to empty string. This ensures existing
persisted amendment files deserialize without error.

### Journal event for manual amendments
Created `amendment_queued_manual_event` as a separate builder from
`amendment_queued_event` because manual amendments have no `run_id` or
`source_stage`. Both emit `JournalEventType::AmendmentQueued` so consumers
see a uniform event type.

### AmendmentClearPartial error design
The `clear_amendments` service attempts removal of each amendment individually
and tracks successes and failures. If any removal fails, it returns
`AppError::AmendmentClearPartial` with vectors of removed and remaining
amendment IDs plus counts. The `#[error()]` format string uses `removed_count`
(a `usize`) since `Vec<String>` doesn't implement `Display`.

## Spec Deviations

### No `--from-file` piping through stdin
The `--file` argument reads from a filesystem path. Stdin piping was not
specified in the acceptance criteria and was not implemented. The `--text` and
`--file` arguments are mutually exclusive via clap's `ArgGroup`.

### Conformance scenario count
The spec mentions coverage for "restart persistence" and "completion blocking"
scenarios. These are implicitly covered by the `completed_project_reopen` and
`journal_records_manual_event` scenarios rather than being separate scenarios.
8 conformance scenarios were registered instead of the potentially higher count
implied by the full list of suggested topics.

### No separate lease-conflict conformance scenario
The lease-conflict scenario requires a running project which cannot be easily
set up in the test-stub conformance harness (bootstrap --start completes
immediately). This is covered by the CLI integration test
`project_amend_add_lease_conflict_rejects` instead.

## Testing

### Unit tests (20 new)
- `dedup_key_is_deterministic_for_same_input`
- `dedup_key_normalizes_whitespace`
- `dedup_key_differs_by_source`
- `dedup_key_differs_by_body`
- `dedup_key_is_sha256_hex`
- `amendment_source_serializes_to_snake_case`
- `amendment_source_round_trips`
- `amendment_source_display_matches_as_str`
- `queued_amendment_defaults_source_to_workflow_stage_on_missing`
- `add_manual_amendment_creates_and_returns_id`
- `add_manual_amendment_rejects_running_project`
- `add_manual_amendment_deduplicates`
- `add_manual_amendment_dedup_normalizes_whitespace`
- `list_amendments_empty_returns_empty`
- `list_amendments_returns_all_pending`
- `remove_amendment_succeeds_for_existing`
- `remove_amendment_fails_for_missing`
- `clear_amendments_empty_returns_empty`
- `clear_amendments_removes_all`
- `clear_amendments_partial_failure_reports_remaining`

### CLI integration tests (12 new)
- `project_amend_add_text_succeeds_and_prints_id`
- `project_amend_add_file_succeeds`
- `project_amend_add_rejects_empty_body`
- `project_amend_list_empty`
- `project_amend_add_then_list_shows_amendment`
- `project_amend_remove_existing`
- `project_amend_remove_missing_fails`
- `project_amend_clear_removes_all`
- `project_amend_duplicate_manual_add_is_noop`
- `project_amend_add_reopens_completed_project`
- `project_amend_add_journal_records_event`
- `project_amend_add_lease_conflict_rejects`

### Conformance scenarios (8 new)
- `parity_slice3_manual_add`
- `parity_slice3_manual_list_empty`
- `parity_slice3_manual_remove`
- `parity_slice3_manual_clear`
- `parity_slice3_duplicate_manual_add`
- `parity_slice3_completed_project_reopen`
- `parity_slice3_journal_records_manual_event`
- `parity_slice3_remove_missing_fails`

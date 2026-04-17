#![forbid(unsafe_code)]

//! Failure reconciliation handler for failed bead tasks.
//!
//! Unlike success reconciliation, this path never mutates bead state in `br`.
//! It records the failed attempt in milestone lineage/journal state, counts the
//! bead's failed attempts, and either keeps the controller retryable or
//! escalates to operator intervention after too many failures.

use std::cmp::Ordering;
use std::path::Path;

use chrono::{DateTime, Utc};
use tracing::Instrument;

use crate::contexts::milestone_record::bead_refs::milestone_bead_refs_match;
use crate::contexts::milestone_record::controller::{
    self as milestone_controller, MilestoneControllerPort, MilestoneControllerState,
};
use crate::contexts::milestone_record::model::{
    CompletionJournalDetails, MilestoneEventType, MilestoneId, MilestoneJournalEvent,
    StartJournalDetails, TaskRunEntry, TaskRunOutcome,
};
use crate::contexts::milestone_record::service::{
    self as milestone_service, CompletionMilestoneDisposition, MilestoneJournalPort,
    MilestoneSnapshotPort, TaskRunLineagePort,
};

pub const MAX_FAILURE_RETRIES: u32 = 3;
const SUPERSEDED_BY_RETRY_ERROR_PREFIX: &str = "superseded by retry started at ";
const CLI_SYNC_TASK_ID_PREFIX: &str = "cli-sync:";

#[derive(Clone)]
struct FailureAttemptEvent {
    details: CompletionJournalDetails,
    failed_at: DateTime<Utc>,
    journal_index: usize,
}

#[derive(Clone)]
struct AttemptStartEvent {
    details: StartJournalDetails,
    started_at: DateTime<Utc>,
    journal_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttemptIdentity {
    project_id: String,
    run_id: Option<String>,
    started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
struct RecordedFailureProvenance {
    task_id: Option<String>,
    outcome_detail: Option<String>,
    plan_hash: Option<String>,
    failed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default)]
struct ParsedFailureOutcomeDetail {
    task_id: Option<String>,
    attempt_number: Option<u32>,
    error_summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureReconciliationOutcome {
    Retryable {
        attempt_number: u32,
        max_retries: u32,
    },
    EscalatedToOperator {
        attempt_number: u32,
        reason: String,
    },
}

#[derive(Debug)]
pub enum FailureReconciliationError {
    MilestoneUpdateFailed {
        bead_id: String,
        task_id: String,
        details: String,
    },
}

impl std::fmt::Display for FailureReconciliationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MilestoneUpdateFailed {
                bead_id,
                task_id,
                details,
            } => write!(
                f,
                "milestone update failed for bead={bead_id} task={task_id}: {details}"
            ),
        }
    }
}

impl std::error::Error for FailureReconciliationError {}

#[allow(clippy::too_many_arguments)]
pub async fn reconcile_failure<S, J, L, C>(
    snapshot_store: &S,
    journal_store: &J,
    lineage_store: &L,
    controller_store: &C,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    milestone_id_str: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    failed_at: DateTime<Utc>,
    error_summary: &str,
) -> Result<FailureReconciliationOutcome, FailureReconciliationError>
where
    S: MilestoneSnapshotPort,
    J: MilestoneJournalPort,
    L: TaskRunLineagePort,
    C: MilestoneControllerPort,
{
    async move {
        reconcile_failure_sync(
            snapshot_store,
            journal_store,
            lineage_store,
            controller_store,
            base_dir,
            bead_id,
            task_id,
            project_id,
            milestone_id_str,
            run_id,
            plan_hash,
            started_at,
            failed_at,
            error_summary,
        )
    }
    .instrument(tracing::warn_span!(
        "reconcile_failure",
        milestone_id = milestone_id_str,
        bead_id = bead_id,
        task_id = task_id,
        run_id = run_id
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn reconcile_failure_sync<S, J, L, C>(
    snapshot_store: &S,
    journal_store: &J,
    lineage_store: &L,
    controller_store: &C,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    milestone_id_str: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    failed_at: DateTime<Utc>,
    error_summary: &str,
) -> Result<FailureReconciliationOutcome, FailureReconciliationError>
where
    S: MilestoneSnapshotPort,
    J: MilestoneJournalPort,
    L: TaskRunLineagePort,
    C: MilestoneControllerPort,
{
    let milestone_id = MilestoneId::new(milestone_id_str).map_err(|error| {
        FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("invalid milestone id: {error}"),
        }
    })?;
    let error_summary = normalize_error_summary(error_summary);
    let existing_journal = milestone_service::read_journal(journal_store, base_dir, &milestone_id)
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!(
                "failed to load milestone journal for failure reconciliation: {error}"
            ),
        })?;

    let existing_runs =
        milestone_service::find_runs_for_bead(lineage_store, base_dir, &milestone_id, bead_id)
            .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!("failed to load task runs for failure reconciliation: {error}"),
            })?;
    let existing_failed_events = failure_attempt_events(&existing_journal, &milestone_id, bead_id);
    let existing_started_events = attempt_start_events(&existing_journal, &milestone_id, bead_id);
    let existing_attempts = known_bead_attempts(
        &existing_runs,
        &existing_started_events,
        &existing_failed_events,
    );

    let (already_recorded, predicted_attempt_number) = failure_attempt_number(
        &existing_runs,
        &existing_journal,
        &milestone_id,
        bead_id,
        task_id,
        project_id,
        run_id,
        started_at,
    )?;
    let recorded_provenance = recorded_failure_provenance(
        &existing_runs,
        &existing_journal,
        &milestone_id,
        bead_id,
        project_id,
        run_id,
        started_at,
    );
    let preferred_task_id =
        preferred_failure_task_id(recorded_provenance.task_id.as_deref(), task_id);
    let outcome_detail = normalized_failure_outcome_detail(
        recorded_provenance.outcome_detail.as_deref(),
        preferred_task_id,
        predicted_attempt_number,
        &error_summary,
    );
    let recorded_task_id =
        extract_task_id_from_outcome_detail(&outcome_detail).unwrap_or(preferred_task_id);
    let predicted_outcome = failure_outcome(bead_id, predicted_attempt_number, &error_summary);

    if !already_recorded
        && !exact_attempt_exists(&existing_runs, project_id, run_id, started_at)
        && newer_attempt_exists(&existing_attempts, project_id, run_id, started_at)
    {
        let repaired_historical_failure = repair_historical_failure_state(
            snapshot_store,
            base_dir,
            &milestone_id,
            bead_id,
            recorded_task_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            &outcome_detail,
            failed_at,
            journal_store,
            lineage_store,
        )?;
        if repaired_historical_failure {
            repair_recorded_failure_attempt_ordinals(
                snapshot_store,
                journal_store,
                lineage_store,
                base_dir,
                &milestone_id,
                bead_id,
                project_id,
                plan_hash,
            )?;
        }
        tracing::info!(
            bead_id = bead_id,
            task_id = task_id,
            run_id = run_id,
            attempt_number = predicted_attempt_number,
            repaired_historical_failure,
            newer_same_run_attempt =
                newer_same_run_attempt_exists(&existing_attempts, run_id, started_at),
            "skipping stale failure replay because a newer bead attempt is already active"
        );
        log_failure_reconciliation(
            bead_id,
            task_id,
            predicted_attempt_number,
            &error_summary,
            true,
            true,
            repaired_historical_failure,
            &predicted_outcome,
        );
        transition_controller_after_failure(
            controller_store,
            base_dir,
            &milestone_id,
            bead_id,
            task_id,
            project_id,
            run_id,
            started_at,
            failed_at,
            true,
            &existing_attempts,
            &predicted_outcome,
        )?;
        return Ok(predicted_outcome);
    }

    if already_recorded && newer_attempt_exists(&existing_attempts, project_id, run_id, started_at)
    {
        let repaired_historical_failure = repair_historical_failure_state(
            snapshot_store,
            base_dir,
            &milestone_id,
            bead_id,
            recorded_task_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            &outcome_detail,
            failed_at,
            journal_store,
            lineage_store,
        )?;
        if repaired_historical_failure {
            repair_recorded_failure_attempt_ordinals(
                snapshot_store,
                journal_store,
                lineage_store,
                base_dir,
                &milestone_id,
                bead_id,
                project_id,
                plan_hash,
            )?;
        }
        tracing::info!(
            bead_id = bead_id,
            task_id = task_id,
            run_id = run_id,
            attempt_number = predicted_attempt_number,
            repaired_historical_failure,
            "skipping stale failure replay because a newer bead attempt already exists"
        );
        log_failure_reconciliation(
            bead_id,
            task_id,
            predicted_attempt_number,
            &error_summary,
            true,
            true,
            repaired_historical_failure,
            &predicted_outcome,
        );
        transition_controller_after_failure(
            controller_store,
            base_dir,
            &milestone_id,
            bead_id,
            task_id,
            project_id,
            run_id,
            started_at,
            failed_at,
            true,
            &existing_attempts,
            &predicted_outcome,
        )?;
        return Ok(predicted_outcome);
    }

    if already_recorded {
        milestone_service::repair_task_run_with_disposition(
            snapshot_store,
            journal_store,
            lineage_store,
            base_dir,
            &milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            TaskRunOutcome::Failed,
            Some(outcome_detail.clone()),
            failed_at,
            CompletionMilestoneDisposition::ReconcileFromLineage,
        )
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: error.to_string(),
        })?;
    } else {
        milestone_service::record_bead_completion_with_disposition(
            snapshot_store,
            journal_store,
            lineage_store,
            base_dir,
            &milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            TaskRunOutcome::Failed,
            Some(&outcome_detail),
            started_at,
            failed_at,
            CompletionMilestoneDisposition::ReconcileFromLineage,
        )
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: error.to_string(),
        })?;
    }

    let recorded_runs =
        milestone_service::find_runs_for_bead(lineage_store, base_dir, &milestone_id, bead_id)
            .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to reload task runs after failure reconciliation: {error}"
                ),
            })?;
    let recorded_journal = milestone_service::read_journal(journal_store, base_dir, &milestone_id)
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!(
                "failed to reload milestone journal after failure reconciliation: {error}"
            ),
        })?;
    let recorded_failed_events = failure_attempt_events(&recorded_journal, &milestone_id, bead_id);
    let recorded_started_events = attempt_start_events(&recorded_journal, &milestone_id, bead_id);
    let recorded_attempts = known_bead_attempts(
        &recorded_runs,
        &recorded_started_events,
        &recorded_failed_events,
    );
    let (_, attempt_number) = failure_attempt_number(
        &recorded_runs,
        &recorded_journal,
        &milestone_id,
        bead_id,
        task_id,
        project_id,
        run_id,
        started_at,
    )?;

    if !recorded_runs.iter().any(|entry| {
        entry.project_id == project_id
            && entry.run_id.as_deref() == Some(run_id)
            && entry.outcome == TaskRunOutcome::Failed
    }) {
        return Err(FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("failed run was not present after reconciliation for run_id={run_id}"),
        });
    }

    let outcome = failure_outcome(bead_id, attempt_number, &error_summary);
    log_failure_reconciliation(
        bead_id,
        task_id,
        attempt_number,
        &error_summary,
        already_recorded,
        false,
        false,
        &outcome,
    );
    transition_controller_after_failure(
        controller_store,
        base_dir,
        &milestone_id,
        bead_id,
        task_id,
        project_id,
        run_id,
        started_at,
        failed_at,
        already_recorded,
        &recorded_attempts,
        &outcome,
    )?;

    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn log_failure_reconciliation(
    bead_id: &str,
    task_id: &str,
    attempt_number: u32,
    error_summary: &str,
    already_recorded: bool,
    stale_replay: bool,
    repaired_historical_failure: bool,
    outcome: &FailureReconciliationOutcome,
) {
    tracing::warn!(
        bead_id = bead_id,
        task_id = task_id,
        attempt_number = attempt_number,
        max_retries = MAX_FAILURE_RETRIES,
        error_summary = error_summary,
        already_recorded,
        stale_replay,
        repaired_historical_failure,
        "reconciled failed bead attempt"
    );

    if let FailureReconciliationOutcome::EscalatedToOperator {
        attempt_number,
        reason,
    } = outcome
    {
        tracing::error!(
            bead_id = bead_id,
            task_id = task_id,
            attempt_number = *attempt_number,
            max_retries = MAX_FAILURE_RETRIES,
            error_summary = error_summary,
            already_recorded,
            stale_replay,
            repaired_historical_failure,
            reason = reason.as_str(),
            "failed bead escalated to operator"
        );
    }
}

fn normalize_error_summary(error_summary: &str) -> String {
    let normalized = error_summary
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" | ");
    if normalized.is_empty() {
        "task failed without an error summary".to_owned()
    } else {
        normalized
    }
}

fn task_id_is_cli_sync_placeholder(task_id: &str) -> bool {
    task_id.starts_with(CLI_SYNC_TASK_ID_PREFIX)
}

fn preferred_failure_task_id<'a>(
    recorded_task_id: Option<&'a str>,
    fallback_task_id: &'a str,
) -> &'a str {
    match recorded_task_id {
        Some(recorded_task_id)
            if task_id_is_cli_sync_placeholder(recorded_task_id)
                && !task_id_is_cli_sync_placeholder(fallback_task_id) =>
        {
            fallback_task_id
        }
        Some(recorded_task_id) => recorded_task_id,
        None => fallback_task_id,
    }
}

fn is_superseded_retry_error_summary(error_summary: &str) -> bool {
    normalize_error_summary(error_summary).starts_with(SUPERSEDED_BY_RETRY_ERROR_PREFIX)
}

fn format_failure_outcome_detail(
    task_id: &str,
    attempt_number: u32,
    error_summary: &str,
) -> String {
    format!("task_id={task_id}\nattempt={attempt_number}\nerror={error_summary}")
}

fn parse_failure_outcome_detail_structured(detail: &str) -> ParsedFailureOutcomeDetail {
    let mut parsed = ParsedFailureOutcomeDetail::default();
    let mut lines = detail.lines();

    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(task_id) = line.strip_prefix("task_id=") {
            let task_id = task_id.trim();
            if !task_id.is_empty() {
                parsed.task_id = Some(task_id.to_owned());
            }
            continue;
        }
        if let Some(attempt_number) = line.strip_prefix("attempt=") {
            if let Ok(attempt_number) = attempt_number.trim().parse::<u32>() {
                parsed.attempt_number = Some(attempt_number);
            }
            continue;
        }
        if let Some(error_summary) = line.strip_prefix("error=") {
            let mut error_lines = vec![error_summary];
            error_lines.extend(lines);
            let error_summary = normalize_error_summary(&error_lines.join("\n"));
            if !error_summary.is_empty() {
                parsed.error_summary = Some(error_summary);
            }
            break;
        }
    }

    parsed
}

fn consume_legacy_failure_field<'a>(detail: &'a str, prefix: &str) -> Option<(&'a str, &'a str)> {
    let detail = detail.trim_start();
    let value = detail.strip_prefix(prefix)?;
    match value.split_once(';') {
        Some((field, remainder)) => Some((field.trim(), remainder)),
        None => Some((value.trim(), "")),
    }
}

fn parse_failure_outcome_detail_legacy(detail: &str) -> ParsedFailureOutcomeDetail {
    let mut parsed = ParsedFailureOutcomeDetail::default();
    let mut remainder = detail.trim();

    if let Some((task_id, next)) = consume_legacy_failure_field(remainder, "task_id=") {
        if !task_id.is_empty() {
            parsed.task_id = Some(task_id.to_owned());
        }
        remainder = next;
    }

    if let Some((attempt_number, next)) = consume_legacy_failure_field(remainder, "attempt=") {
        if let Ok(attempt_number) = attempt_number.parse::<u32>() {
            parsed.attempt_number = Some(attempt_number);
        }
        remainder = next;
    }

    let remainder = remainder.trim_start_matches(';').trim_start();
    if let Some(error_summary) = remainder.strip_prefix("error=") {
        let error_summary = normalize_error_summary(error_summary);
        if !error_summary.is_empty() {
            parsed.error_summary = Some(error_summary);
        }
    }

    parsed
}

fn parse_failure_outcome_detail(detail: &str) -> ParsedFailureOutcomeDetail {
    let parsed = parse_failure_outcome_detail_structured(detail);
    if parsed.task_id.is_some() || parsed.attempt_number.is_some() || parsed.error_summary.is_some()
    {
        return parsed;
    }

    let parsed = parse_failure_outcome_detail_legacy(detail);
    if parsed.task_id.is_some() || parsed.attempt_number.is_some() || parsed.error_summary.is_some()
    {
        return parsed;
    }

    let mut parsed = ParsedFailureOutcomeDetail::default();
    let error_summary = normalize_error_summary(detail);
    if !error_summary.is_empty() {
        parsed.error_summary = Some(error_summary);
    }
    parsed
}

fn normalized_failure_outcome_detail(
    recorded_outcome_detail: Option<&str>,
    fallback_task_id: &str,
    attempt_number: u32,
    fallback_error_summary: &str,
) -> String {
    normalized_failure_outcome_detail_with_attempt_strategy(
        recorded_outcome_detail,
        fallback_task_id,
        attempt_number,
        fallback_error_summary,
        true,
    )
}

fn normalized_failure_outcome_detail_with_attempt_strategy(
    recorded_outcome_detail: Option<&str>,
    fallback_task_id: &str,
    attempt_number: u32,
    fallback_error_summary: &str,
    preserve_recorded_attempt_number: bool,
) -> String {
    let parsed = recorded_outcome_detail
        .map(parse_failure_outcome_detail)
        .unwrap_or_default();
    let task_id = preferred_failure_task_id(parsed.task_id.as_deref(), fallback_task_id);
    let attempt_number = if preserve_recorded_attempt_number {
        parsed.attempt_number.unwrap_or(attempt_number)
    } else {
        attempt_number
    };
    let error_summary = parsed
        .error_summary
        .as_deref()
        .filter(|summary| !is_superseded_retry_error_summary(summary))
        .unwrap_or(fallback_error_summary);
    format_failure_outcome_detail(task_id, attempt_number, error_summary)
}

fn canonical_failure_error_summary(detail: Option<&str>) -> String {
    detail
        .map(parse_failure_outcome_detail)
        .and_then(|parsed| parsed.error_summary)
        .filter(|summary| !is_superseded_retry_error_summary(summary))
        .unwrap_or_else(|| normalize_error_summary(detail.unwrap_or_default()))
}

fn extract_task_id_from_outcome_detail(detail: &str) -> Option<&str> {
    let task_id = detail.lines().next()?.trim().strip_prefix("task_id=")?;
    let task_id = task_id.split(';').next().map(str::trim).unwrap_or(task_id);
    (!task_id.is_empty()).then_some(task_id)
}

#[allow(clippy::too_many_arguments)]
fn repair_historical_failure_state<S, J, L>(
    snapshot_store: &S,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    outcome_detail: &str,
    failed_at: DateTime<Utc>,
    journal_store: &J,
    lineage_store: &L,
) -> Result<bool, FailureReconciliationError>
where
    S: MilestoneSnapshotPort,
    J: MilestoneJournalPort,
    L: TaskRunLineagePort,
{
    let runs =
        milestone_service::find_runs_for_bead(lineage_store, base_dir, milestone_id, bead_id)
            .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to load task runs before historical failure repair: {error}"
                ),
            })?;
    let journal = milestone_service::read_journal(journal_store, base_dir, milestone_id).map_err(
        |error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!(
                "failed to load milestone journal before historical failure repair: {error}"
            ),
        },
    )?;
    if failure_state_matches_expected(
        &runs,
        &journal,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        started_at,
        task_id,
        outcome_detail,
    ) {
        return Ok(false);
    }

    milestone_service::repair_task_run_with_disposition(
        snapshot_store,
        journal_store,
        lineage_store,
        base_dir,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        plan_hash,
        started_at,
        TaskRunOutcome::Failed,
        Some(outcome_detail.to_owned()),
        failed_at,
        CompletionMilestoneDisposition::ReconcileFromLineage,
    )
    .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: format!(
            "failed to repair historical failed attempt during stale replay reconciliation: {error}"
        ),
    })?;

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn repair_recorded_failure_attempt_ordinals<S, J, L>(
    snapshot_store: &S,
    journal_store: &J,
    lineage_store: &L,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    default_plan_hash: Option<&str>,
) -> Result<(), FailureReconciliationError>
where
    S: MilestoneSnapshotPort,
    J: MilestoneJournalPort,
    L: TaskRunLineagePort,
{
    let mut runs =
        milestone_service::find_runs_for_bead(lineage_store, base_dir, milestone_id, bead_id)
            .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: project_id.to_owned(),
                details: format!(
                    "failed to reload task runs while repairing failure attempt ordinals: {error}"
                ),
            })?;
    let mut journal =
        milestone_service::read_journal(journal_store, base_dir, milestone_id).map_err(|error| {
            FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: project_id.to_owned(),
                details: format!(
                    "failed to reload milestone journal while repairing failure attempt ordinals: {error}"
                ),
            }
        })?;

    let failed_events = failure_attempt_events(&journal, milestone_id, bead_id);
    let attempts = recorded_failed_attempts(&runs, &failed_events);

    for (index, attempt) in attempts.iter().enumerate() {
        let Some(run_id) = attempt.run_id.as_deref() else {
            continue;
        };
        let provenance = recorded_failure_provenance(
            &runs,
            &journal,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            attempt.started_at,
        );
        let Some(task_id) = provenance.task_id.as_deref() else {
            continue;
        };
        let attempt_number = index as u32 + 1;
        let canonical_outcome_detail = normalized_failure_outcome_detail_with_attempt_strategy(
            provenance.outcome_detail.as_deref(),
            task_id,
            attempt_number,
            &canonical_failure_error_summary(provenance.outcome_detail.as_deref()),
            false,
        );
        if failure_state_matches_expected(
            &runs,
            &journal,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            attempt.started_at,
            task_id,
            &canonical_outcome_detail,
        ) {
            continue;
        }

        milestone_service::repair_task_run_with_disposition(
            snapshot_store,
            journal_store,
            lineage_store,
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            provenance.plan_hash.as_deref().or(default_plan_hash),
            attempt.started_at,
            TaskRunOutcome::Failed,
            Some(canonical_outcome_detail),
            provenance.failed_at.unwrap_or(attempt.started_at),
            CompletionMilestoneDisposition::ReconcileFromLineage,
        )
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!(
                "failed to repair stale failure attempt markers during historical backfill: {error}"
            ),
        })?;

        runs =
            milestone_service::find_runs_for_bead(lineage_store, base_dir, milestone_id, bead_id)
                .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to reload task runs after repairing failure attempt markers: {error}"
                ),
            })?;
        journal = milestone_service::read_journal(journal_store, base_dir, milestone_id).map_err(
            |error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to reload milestone journal after repairing failure attempt markers: {error}"
                ),
            },
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn failure_state_matches_expected(
    runs: &[TaskRunEntry],
    journal: &[MilestoneJournalEvent],
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
    task_id: &str,
    outcome_detail: &str,
) -> bool {
    let run_matches = runs.iter().any(|entry| {
        attempt_identity_matches(entry, project_id, run_id, started_at)
            && entry.outcome == TaskRunOutcome::Failed
            && entry.task_id.as_deref() == Some(task_id)
            && entry.outcome_detail.as_deref() == Some(outcome_detail)
    });
    let journal_matches = failure_attempt_events(journal, milestone_id, bead_id)
        .into_iter()
        .find(|event| {
            event.details.project_id == project_id
                && event.details.run_id.as_deref() == Some(run_id)
                && event.details.started_at == started_at
        })
        .is_some_and(|event| {
            event.details.task_id.as_deref() == Some(task_id)
                && event.details.outcome_detail.as_deref() == Some(outcome_detail)
        });

    run_matches && journal_matches
}

fn attempt_identity_cmp(left: &AttemptIdentity, right: &AttemptIdentity) -> Ordering {
    left.started_at
        .cmp(&right.started_at)
        .then_with(|| left.project_id.cmp(&right.project_id))
        .then_with(|| left.run_id.cmp(&right.run_id))
}

fn sort_attempt_identities(attempts: &mut [AttemptIdentity]) {
    attempts.sort_by(attempt_identity_cmp);
}

fn recorded_failed_attempts(
    runs: &[TaskRunEntry],
    failed_events: &[FailureAttemptEvent],
) -> Vec<AttemptIdentity> {
    let mut attempts = Vec::new();

    for event in failed_events {
        push_attempt_identity(
            &mut attempts,
            &event.details.project_id,
            event.details.run_id.as_deref(),
            event.details.started_at,
        );
    }
    for entry in runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
    {
        push_attempt_identity(
            &mut attempts,
            &entry.project_id,
            entry.run_id.as_deref(),
            entry.started_at,
        );
    }

    sort_attempt_identities(&mut attempts);
    attempts
}

fn attempt_identity_matches(
    entry: &TaskRunEntry,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> bool {
    entry.project_id == project_id
        && entry.run_id.as_deref() == Some(run_id)
        && entry.started_at == started_at
}

fn newer_attempt_exists(
    attempts: &[AttemptIdentity],
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> bool {
    attempts.iter().any(|attempt| {
        !(attempt.project_id == project_id
            && attempt.run_id.as_deref() == Some(run_id)
            && attempt.started_at == started_at)
            && attempt.started_at > started_at
    })
}

fn exact_attempt_exists(
    runs: &[TaskRunEntry],
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> bool {
    runs.iter()
        .any(|entry| attempt_identity_matches(entry, project_id, run_id, started_at))
}

fn newer_same_run_attempt_exists(
    attempts: &[AttemptIdentity],
    run_id: &str,
    started_at: DateTime<Utc>,
) -> bool {
    attempts
        .iter()
        .any(|attempt| attempt.run_id.as_deref() == Some(run_id) && attempt.started_at > started_at)
}

fn recorded_failure_provenance(
    runs: &[TaskRunEntry],
    journal: &[MilestoneJournalEvent],
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> RecordedFailureProvenance {
    let run_provenance = runs.iter().find(|entry| {
        attempt_identity_matches(entry, project_id, run_id, started_at)
            && entry.outcome == TaskRunOutcome::Failed
    });
    let journal_provenance = failure_attempt_events(journal, milestone_id, bead_id)
        .into_iter()
        .find(|event| {
            event.details.project_id == project_id
                && event.details.run_id.as_deref() == Some(run_id)
                && event.details.started_at == started_at
        });

    let authoritative_outcome_detail = |detail: Option<String>| {
        detail.filter(|detail| {
            !parse_failure_outcome_detail(detail)
                .error_summary
                .as_deref()
                .is_some_and(is_superseded_retry_error_summary)
        })
    };
    let run_outcome_detail = run_provenance.and_then(|entry| entry.outcome_detail.clone());
    let journal_outcome_detail = journal_provenance
        .as_ref()
        .and_then(|event| event.details.outcome_detail.clone());

    let outcome_detail = authoritative_outcome_detail(run_outcome_detail)
        .or_else(|| authoritative_outcome_detail(journal_outcome_detail));
    let task_id = run_provenance
        .and_then(|entry| entry.task_id.clone())
        .or_else(|| {
            journal_provenance
                .as_ref()
                .and_then(|event| event.details.task_id.clone())
        })
        .or_else(|| {
            outcome_detail
                .as_deref()
                .and_then(extract_task_id_from_outcome_detail)
                .map(str::to_owned)
        });

    RecordedFailureProvenance {
        task_id,
        outcome_detail,
        plan_hash: run_provenance
            .and_then(|entry| entry.plan_hash.clone())
            .or_else(|| {
                journal_provenance
                    .as_ref()
                    .and_then(|event| event.details.plan_hash.clone())
            }),
        failed_at: run_provenance
            .and_then(|entry| entry.finished_at)
            .or_else(|| journal_provenance.as_ref().map(|event| event.failed_at)),
    }
}

fn controller_requires_failure_sync(
    controller: &milestone_controller::MilestoneControllerRecord,
    bead_id: &str,
    project_id: &str,
) -> bool {
    match controller.state {
        MilestoneControllerState::Idle => true,
        MilestoneControllerState::Selecting | MilestoneControllerState::Completed => false,
        MilestoneControllerState::Claimed
        | MilestoneControllerState::Running
        | MilestoneControllerState::Reconciling
        | MilestoneControllerState::Blocked
        | MilestoneControllerState::NeedsOperator => {
            controller
                .active_bead_id
                .as_deref()
                .is_none_or(|active_bead_id| active_bead_id == bead_id)
                && controller
                    .active_task_id
                    .as_deref()
                    .is_none_or(|active_task_id| active_task_id == project_id)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn failure_attempt_number(
    runs: &[TaskRunEntry],
    journal: &[MilestoneJournalEvent],
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> Result<(bool, u32), FailureReconciliationError> {
    let failed_events = failure_attempt_events(journal, milestone_id, bead_id);
    let started_events = attempt_start_events(journal, milestone_id, bead_id);

    let exact_run = runs
        .iter()
        .find(|entry| {
            entry.project_id == project_id
                && entry.run_id.as_deref() == Some(run_id)
                && entry.started_at == started_at
        })
        .or_else(|| {
            runs.iter().find(|entry| {
                entry.project_id == project_id && entry.run_id.as_deref() == Some(run_id)
            })
        });

    let exact_attempt = AttemptIdentity {
        project_id: project_id.to_owned(),
        run_id: Some(run_id.to_owned()),
        started_at,
    };
    let already_recorded = failed_events.iter().any(|event| {
        event.details.project_id == project_id
            && event.details.run_id.as_deref() == Some(run_id)
            && event.details.started_at == started_at
    }) || runs.iter().any(|entry| {
        attempt_identity_matches(entry, project_id, run_id, started_at)
            && entry.outcome == TaskRunOutcome::Failed
    });

    if let Some(entry) =
        exact_run.filter(|entry| attempt_identity_matches(entry, project_id, run_id, started_at))
    {
        if entry.outcome.is_terminal() && entry.outcome != TaskRunOutcome::Failed {
            return Err(FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "run_id={run_id} is already finalized with non-failed outcome '{}'",
                    entry.outcome
                ),
            });
        }
    }

    let failed_attempts = known_failed_attempts(runs, &started_events, &failed_events);
    if let Some(attempt_index) = failed_attempts
        .iter()
        .position(|attempt| attempt == &exact_attempt)
    {
        return Ok((already_recorded, attempt_index as u32 + 1));
    }

    if let Some(entry) = exact_run {
        if entry.outcome.is_terminal() && entry.outcome != TaskRunOutcome::Failed {
            return Err(FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "run_id={run_id} is already finalized with non-failed outcome '{}'",
                    entry.outcome
                ),
            });
        }
    }

    let insertion_index = failed_attempts
        .iter()
        .filter(|attempt| attempt_identity_cmp(attempt, &exact_attempt).is_lt())
        .count();
    Ok((already_recorded, insertion_index as u32 + 1))
}

fn failure_outcome(
    bead_id: &str,
    attempt_number: u32,
    error_summary: &str,
) -> FailureReconciliationOutcome {
    if attempt_number < MAX_FAILURE_RETRIES {
        FailureReconciliationOutcome::Retryable {
            attempt_number,
            max_retries: MAX_FAILURE_RETRIES,
        }
    } else {
        FailureReconciliationOutcome::EscalatedToOperator {
            attempt_number,
            reason: format!("bead {bead_id} failed {attempt_number} times: {error_summary}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn transition_controller_after_failure<C: MilestoneControllerPort>(
    controller_store: &C,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
    replay_already_settled: bool,
    recorded_attempts: &[AttemptIdentity],
    outcome: &FailureReconciliationOutcome,
) -> Result<(), FailureReconciliationError> {
    let (desired_state, desired_reason, request) = match outcome {
        FailureReconciliationOutcome::Retryable {
            attempt_number,
            max_retries,
        } => {
            let reason = format!(
                "bead {bead_id} failed attempt {attempt_number}/{max_retries}; retry remains available"
            );
            let request = milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::Blocked,
                reason.clone(),
            )
            .with_bead(bead_id)
            .with_task(project_id);
            (MilestoneControllerState::Blocked, reason, request)
        }
        FailureReconciliationOutcome::EscalatedToOperator {
            attempt_number: _,
            reason,
        } => {
            let request = milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::NeedsOperator,
                reason.clone(),
            )
            .with_bead(bead_id)
            .with_task(project_id);
            (
                MilestoneControllerState::NeedsOperator,
                reason.clone(),
                request,
            )
        }
    };

    let current = milestone_controller::load_controller(controller_store, base_dir, milestone_id)
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: format!("failed to load controller during failure reconciliation: {error}"),
    })?;
    let transition_at = current
        .as_ref()
        .map(|controller| {
            now.max(controller.last_transition_at)
                .max(controller.updated_at)
        })
        .unwrap_or(now);

    if newer_attempt_exists(recorded_attempts, project_id, run_id, started_at) {
        tracing::info!(
            bead_id = bead_id,
            task_id = task_id,
            run_id = run_id,
            "skipping failure controller transition because a newer bead attempt is already recorded"
        );
        return Ok(());
    }

    if replay_already_settled
        && current.as_ref().is_some_and(|controller| {
            !controller_requires_failure_sync(controller, bead_id, project_id)
        })
    {
        return Ok(());
    }

    if replay_already_settled
        && current.as_ref().is_some_and(|controller| {
            controller.state == desired_state
                && controller.last_transition_reason.as_deref() == Some(desired_reason.as_str())
        })
    {
        return Ok(());
    }

    if !current
        .as_ref()
        .is_some_and(|controller| controller.state == MilestoneControllerState::Reconciling)
    {
        milestone_controller::sync_controller_task_reconciling(
            controller_store,
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            "workflow execution failed; reconciling milestone state",
            transition_at,
        )
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: error.to_string(),
        })?;
    }

    let refreshed = milestone_controller::load_controller(controller_store, base_dir, milestone_id)
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("failed to reload controller during failure reconciliation: {error}"),
        })?;

    if replay_already_settled
        && refreshed.as_ref().is_some_and(|controller| {
            controller.state == desired_state
                && controller.last_transition_reason.as_deref() == Some(desired_reason.as_str())
        })
    {
        return Ok(());
    }

    milestone_controller::sync_controller_state(
        controller_store,
        base_dir,
        milestone_id,
        request,
        transition_at,
    )
    .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: error.to_string(),
    })?;

    Ok(())
}

fn failure_attempt_events(
    journal: &[MilestoneJournalEvent],
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> Vec<FailureAttemptEvent> {
    let mut events = journal
        .iter()
        .enumerate()
        .filter(|(_, event)| event.event_type == MilestoneEventType::BeadFailed)
        .filter(|(_, event)| {
            event.bead_id.as_deref().is_some_and(|event_bead_id| {
                milestone_bead_refs_match(milestone_id, event_bead_id, bead_id)
            })
        })
        .filter_map(|(journal_index, event)| {
            let details =
                serde_json::from_str::<CompletionJournalDetails>(event.details.as_deref()?).ok()?;
            Some(FailureAttemptEvent {
                details,
                failed_at: event.timestamp,
                journal_index,
            })
        })
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        left.details
            .started_at
            .cmp(&right.details.started_at)
            .then_with(|| left.failed_at.cmp(&right.failed_at))
            .then_with(|| left.details.run_id.cmp(&right.details.run_id))
            .then_with(|| left.details.task_id.cmp(&right.details.task_id))
            .then_with(|| left.journal_index.cmp(&right.journal_index))
    });
    events
}

fn attempt_start_events(
    journal: &[MilestoneJournalEvent],
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> Vec<AttemptStartEvent> {
    let mut events = journal
        .iter()
        .enumerate()
        .filter(|(_, event)| event.event_type == MilestoneEventType::BeadStarted)
        .filter(|(_, event)| {
            event.bead_id.as_deref().is_some_and(|event_bead_id| {
                milestone_bead_refs_match(milestone_id, event_bead_id, bead_id)
            })
        })
        .filter_map(|(journal_index, event)| {
            let details =
                serde_json::from_str::<StartJournalDetails>(event.details.as_deref()?).ok()?;
            Some(AttemptStartEvent {
                details,
                started_at: event.timestamp,
                journal_index,
            })
        })
        .collect::<Vec<_>>();
    events.sort_by(|left, right| {
        left.started_at
            .cmp(&right.started_at)
            .then_with(|| left.details.run_id.cmp(&right.details.run_id))
            .then_with(|| left.journal_index.cmp(&right.journal_index))
    });
    events
}

fn known_bead_attempts(
    runs: &[TaskRunEntry],
    started_events: &[AttemptStartEvent],
    failed_events: &[FailureAttemptEvent],
) -> Vec<AttemptIdentity> {
    let mut attempts = Vec::new();

    for event in failed_events {
        push_attempt_identity(
            &mut attempts,
            &event.details.project_id,
            event.details.run_id.as_deref(),
            event.details.started_at,
        );
    }
    for event in started_events {
        push_attempt_identity(
            &mut attempts,
            &event.details.project_id,
            event.details.run_id.as_deref(),
            event.started_at,
        );
    }
    for entry in runs {
        push_attempt_identity(
            &mut attempts,
            &entry.project_id,
            entry.run_id.as_deref(),
            entry.started_at,
        );
    }

    sort_attempt_identities(&mut attempts);
    attempts
}

fn known_failed_attempts(
    runs: &[TaskRunEntry],
    started_events: &[AttemptStartEvent],
    failed_events: &[FailureAttemptEvent],
) -> Vec<AttemptIdentity> {
    let mut attempts = Vec::new();

    for event in failed_events {
        push_attempt_identity(
            &mut attempts,
            &event.details.project_id,
            event.details.run_id.as_deref(),
            event.details.started_at,
        );
    }
    for entry in runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
    {
        push_attempt_identity(
            &mut attempts,
            &entry.project_id,
            entry.run_id.as_deref(),
            entry.started_at,
        );
    }
    for event in started_events {
        let Some(run_id) = event.details.run_id.as_deref() else {
            continue;
        };
        if same_run_attempt_has_newer_start(run_id, event.started_at, started_events, runs) {
            push_attempt_identity(
                &mut attempts,
                &event.details.project_id,
                Some(run_id),
                event.started_at,
            );
        }
    }

    sort_attempt_identities(&mut attempts);
    attempts
}

fn same_run_attempt_has_newer_start(
    run_id: &str,
    started_at: DateTime<Utc>,
    started_events: &[AttemptStartEvent],
    runs: &[TaskRunEntry],
) -> bool {
    started_events.iter().any(|event| {
        event.details.run_id.as_deref() == Some(run_id) && event.started_at > started_at
    }) || runs
        .iter()
        .any(|entry| entry.run_id.as_deref() == Some(run_id) && entry.started_at > started_at)
}

fn push_attempt_identity(
    attempts: &mut Vec<AttemptIdentity>,
    project_id: &str,
    run_id: Option<&str>,
    started_at: DateTime<Utc>,
) {
    let attempt = AttemptIdentity {
        project_id: project_id.to_owned(),
        run_id: run_id.map(str::to_owned),
        started_at,
    };
    if !attempts.contains(&attempt) {
        attempts.push(attempt);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        failure_attempt_number, format_failure_outcome_detail, parse_failure_outcome_detail,
        reconcile_failure, FailureReconciliationOutcome, MAX_FAILURE_RETRIES,
    };
    use crate::adapters::fs::{
        FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
        FsMilestoneSnapshotStore, FsMilestoneStore, FsTaskRunLineageStore,
    };
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::controller as milestone_controller;
    use crate::contexts::milestone_record::model::{
        MilestoneEventType, MilestoneId, MilestoneJournalEvent, TaskRunEntry, TaskRunOutcome,
    };
    use crate::contexts::milestone_record::service::{
        self as milestone_service, create_milestone, persist_plan, read_journal, read_task_runs,
        CreateMilestoneInput, MilestoneJournalPort, TaskRunLineagePort,
    };
    use chrono::{Duration, Utc};
    use std::path::Path;

    fn write_beads_export(base_dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
        let contents = "{\"id\":\"bead-failure\",\"status\":\"open\"}\n".to_owned();
        std::fs::create_dir_all(base_dir.join(".beads"))?;
        std::fs::write(base_dir.join(".beads/issues.jsonl"), &contents)?;
        Ok(contents)
    }

    fn create_test_milestone(
        base_dir: &Path,
    ) -> Result<crate::contexts::milestone_record::model::MilestoneId, Box<dyn std::error::Error>>
    {
        let now = Utc::now();
        let record = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-failure-reconcile".to_owned(),
                name: "Failure reconcile milestone".to_owned(),
                description: "Exercises failure reconciliation".to_owned(),
            },
            now,
        )?;
        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-failure-reconcile".to_owned(),
                name: "Failure reconcile milestone".to_owned(),
            },
            executive_summary: "Single-bead failure reconciliation test plan".to_owned(),
            goals: vec!["Track failed attempts".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "The bead is represented in the plan".to_owned(),
                covered_by: vec!["bead-failure".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-failure".to_owned()),
                    explicit_id: Some(true),
                    title: "Failure bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
            agents_guidance: None,
        };
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &record.id,
            &bundle,
            now + Duration::milliseconds(1),
        )?;
        Ok(record.id)
    }

    fn remove_bead_failed_events(
        base_dir: &Path,
        milestone_id: &crate::contexts::milestone_record::model::MilestoneId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let retained = read_journal(&FsMilestoneJournalStore, base_dir, milestone_id)?
            .into_iter()
            .filter(|event| event.event_type != MilestoneEventType::BeadFailed)
            .map(|event| serde_json::to_string(&event))
            .collect::<Result<Vec<_>, _>>()?;
        let contents = if retained.is_empty() {
            String::new()
        } else {
            format!("{}\n", retained.join("\n"))
        };
        std::fs::write(
            base_dir.join(format!(
                ".ralph-burning/milestones/{milestone_id}/journal.ndjson"
            )),
            contents,
        )?;
        Ok(())
    }

    fn start_attempt_with_project(
        base_dir: &Path,
        milestone_id: &crate::contexts::milestone_record::model::MilestoneId,
        _task_id: &str,
        project_id: &str,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        milestone_service::record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            milestone_id,
            "bead-failure",
            project_id,
            run_id,
            "plan-v1",
            started_at,
        )?;
        if milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            milestone_id,
        )?
        .as_ref()
        .is_some_and(|controller| {
            controller.state == milestone_controller::MilestoneControllerState::Blocked
        }) {
            milestone_controller::sync_controller_task_claimed(
                &FsMilestoneControllerStore,
                base_dir,
                milestone_id,
                "bead-failure",
                project_id,
                "retrying blocked bead task",
                started_at,
            )?;
        }
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base_dir,
            milestone_id,
            "bead-failure",
            project_id,
            "workflow execution started",
            started_at,
        )?;
        Ok(())
    }

    fn start_attempt(
        base_dir: &Path,
        milestone_id: &crate::contexts::milestone_record::model::MilestoneId,
        task_id: &str,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        start_attempt_with_project(
            base_dir,
            milestone_id,
            task_id,
            "proj-failure",
            run_id,
            started_at,
        )
    }

    #[test]
    fn failure_attempt_number_ignores_start_only_attempts_when_counting_retries() {
        let milestone_id = MilestoneId::new("ms-failure-reconcile").expect("milestone id");
        let orphaned_started_at = Utc::now() + Duration::seconds(1);
        let failed_started_at = orphaned_started_at + Duration::seconds(30);
        let runs = vec![
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-failure".to_owned(),
                run_id: Some("run-orphan".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: orphaned_started_at,
                finished_at: None,
                task_id: Some("task-orphan".to_owned()),
            },
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-failure".to_owned(),
                run_id: Some("run-actual".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: failed_started_at,
                finished_at: None,
                task_id: Some("task-actual".to_owned()),
            },
        ];
        let journal = vec![
            MilestoneJournalEvent::new(MilestoneEventType::BeadStarted, orphaned_started_at)
                .with_bead("bead-failure")
                .with_details(
                    TaskRunEntry {
                        milestone_id: milestone_id.to_string(),
                        bead_id: "bead-failure".to_owned(),
                        project_id: "proj-failure".to_owned(),
                        run_id: Some("run-orphan".to_owned()),
                        plan_hash: Some("plan-v1".to_owned()),
                        outcome: TaskRunOutcome::Running,
                        outcome_detail: None,
                        started_at: orphaned_started_at,
                        finished_at: None,
                        task_id: Some("task-orphan".to_owned()),
                    }
                    .start_journal_details(),
                ),
            MilestoneJournalEvent::new(MilestoneEventType::BeadStarted, failed_started_at)
                .with_bead("bead-failure")
                .with_details(
                    TaskRunEntry {
                        milestone_id: milestone_id.to_string(),
                        bead_id: "bead-failure".to_owned(),
                        project_id: "proj-failure".to_owned(),
                        run_id: Some("run-actual".to_owned()),
                        plan_hash: Some("plan-v1".to_owned()),
                        outcome: TaskRunOutcome::Running,
                        outcome_detail: None,
                        started_at: failed_started_at,
                        finished_at: None,
                        task_id: Some("task-actual".to_owned()),
                    }
                    .start_journal_details(),
                ),
        ];

        let (already_recorded, attempt_number) = failure_attempt_number(
            &runs,
            &journal,
            &milestone_id,
            "bead-failure",
            "task-actual",
            "proj-failure",
            "run-actual",
            failed_started_at,
        )
        .expect("attempt number");

        assert!(!already_recorded);
        assert_eq!(
            attempt_number, 1,
            "start-only orphaned attempts must not consume retry budget"
        );
    }

    #[test]
    fn failure_attempt_number_orders_retries_by_started_at_instead_of_run_id() {
        let milestone_id = MilestoneId::new("ms-failure-reconcile").expect("milestone id");
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);
        let runs = vec![
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-failure".to_owned(),
                run_id: Some("zzz-older-run".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("task_id=task-1\nattempt=1\nerror=first failure".to_owned()),
                started_at: first_started_at,
                finished_at: Some(first_started_at + Duration::seconds(5)),
                task_id: Some("task-1".to_owned()),
            },
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-failure".to_owned(),
                run_id: Some("aaa-newer-run".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: second_started_at,
                finished_at: None,
                task_id: Some("task-1".to_owned()),
            },
        ];

        let (already_recorded, attempt_number) = failure_attempt_number(
            &runs,
            &[],
            &milestone_id,
            "bead-failure",
            "task-1",
            "proj-failure",
            "aaa-newer-run",
            second_started_at,
        )
        .expect("attempt number");

        assert!(!already_recorded);
        assert_eq!(
            attempt_number, 2,
            "retry numbering must follow attempt start time, not run_id lexicographic order"
        );
    }

    #[test]
    fn failure_attempt_number_counts_failed_attempts_across_replacement_projects() {
        let milestone_id = MilestoneId::new("ms-failure-reconcile").expect("milestone id");
        let other_project_started_at = Utc::now() + Duration::seconds(1);
        let target_started_at = other_project_started_at + Duration::seconds(30);
        let runs = vec![
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-other".to_owned(),
                run_id: Some("run-other".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some(
                    "task_id=task-other\nattempt=1\nerror=other project failure".to_owned(),
                ),
                started_at: other_project_started_at,
                finished_at: Some(other_project_started_at + Duration::seconds(5)),
                task_id: Some("task-other".to_owned()),
            },
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-failure".to_owned(),
                run_id: Some("run-target".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: target_started_at,
                finished_at: None,
                task_id: Some("task-target".to_owned()),
            },
        ];

        let (already_recorded, attempt_number) = failure_attempt_number(
            &runs,
            &[],
            &milestone_id,
            "bead-failure",
            "task-target",
            "proj-failure",
            "run-target",
            target_started_at,
        )
        .expect("attempt number");

        assert!(!already_recorded);
        assert_eq!(
            attempt_number, 2,
            "replacement-project retries must inherit prior bead failures"
        );
    }

    #[test]
    fn parse_failure_outcome_detail_preserves_semicolons_inside_error_payload() {
        let detail = format_failure_outcome_detail(
            "task-1",
            2,
            "first clause; attempt=99; task_id=shadow; final clause",
        );

        let parsed = parse_failure_outcome_detail(&detail);

        assert_eq!(parsed.task_id.as_deref(), Some("task-1"));
        assert_eq!(parsed.attempt_number, Some(2));
        assert_eq!(
            parsed.error_summary.as_deref(),
            Some("first clause; attempt=99; task_id=shadow; final clause")
        );
    }

    #[tokio::test]
    async fn reconcile_failure_first_failure_records_retryable_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;

        let outcome = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "agent crashed while applying patch",
        )
        .await?;

        assert_eq!(
            outcome,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert_eq!(controller.active_bead_id.as_deref(), Some("bead-failure"));
        assert_eq!(controller.active_task_id.as_deref(), Some("proj-failure"));
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("failed attempt 1/3")));

        let snapshot =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base, &milestone_id)?;
        assert_eq!(snapshot.progress.failed_beads, 1);

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(
            runs[0].task_id.as_deref(),
            Some("task-1"),
            "task_id should be backfilled from failure outcome detail"
        );

        let journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?;
        let failed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::BeadFailed
                    && event.bead_id.as_deref() == Some("bead-failure")
            })
            .expect("failed event should be present");
        let details = failed_event
            .details
            .as_deref()
            .expect("failed event should carry details");
        assert!(details.contains("\"task_id\":\"task-1\""));
        assert!(details.contains("attempt=1"));
        assert!(details.contains("agent crashed while applying patch"));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_counts_second_failure_and_escalates_on_third(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let base_time = Utc::now();

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-1",
            base_time + Duration::seconds(1),
        )?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            base_time + Duration::seconds(1),
            base_time + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-2",
            base_time + Duration::seconds(11),
        )?;
        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-2",
            Some("plan-v1"),
            base_time + Duration::seconds(11),
            base_time + Duration::seconds(15),
            "second failure",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let runs_after_second = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(
            runs_after_second
                .iter()
                .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
                .count(),
            2
        );

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-3",
            base_time + Duration::seconds(21),
        )?;
        let third = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-3",
            Some("plan-v1"),
            base_time + Duration::seconds(21),
            base_time + Duration::seconds(25),
            "third failure",
        )
        .await?;
        match third {
            FailureReconciliationOutcome::EscalatedToOperator {
                attempt_number,
                reason,
            } => {
                assert_eq!(attempt_number, 3);
                assert!(reason.contains("failed 3 times"));
                assert!(reason.contains("third failure"));
            }
            other => panic!("expected escalation on third failure, got {other:?}"),
        }

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("failed 3 times")));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_is_idempotent_for_replayed_run(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "replayed failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let controller_journal_len =
            milestone_controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base,
                &milestone_id,
            )?
            .len();
        let failed_events_before = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .count();

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "replayed failure",
        )
        .await?;
        assert_eq!(replay, first);

        let failed_events_after = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .count();
        assert_eq!(failed_events_before, failed_events_after);

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(
            runs.iter()
                .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
                .count(),
            1
        );

        let controller_journal_after =
            milestone_controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base,
                &milestone_id,
            )?
            .len();
        assert_eq!(controller_journal_len, controller_journal_after);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_with_semicolons_in_error_remains_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);
        let failed_at = started_at + Duration::seconds(10);
        let error_summary =
            "compile failed; attempt=99; task_id=shadow-task; still the same root cause";

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            error_summary,
        )
        .await?;

        let recorded_before = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?
        .into_iter()
        .find(|entry| entry.outcome == TaskRunOutcome::Failed)
        .and_then(|entry| entry.outcome_detail)
        .expect("failed run should record outcome detail");

        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            error_summary,
        )
        .await?;

        let recorded_after = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?
        .into_iter()
        .find(|entry| entry.outcome == TaskRunOutcome::Failed)
        .and_then(|entry| entry.outcome_detail)
        .expect("failed run should still record outcome detail");

        assert_eq!(recorded_after, recorded_before);
        assert!(recorded_after.contains(error_summary));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_preserves_original_task_provenance(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);
        let failed_at = started_at + Duration::seconds(10);

        start_attempt(base, &milestone_id, "task-daemon", "run-1", started_at)?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-daemon",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "daemon failure",
        )
        .await?;

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "cli-sync:proj-failure",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "cli replay should not replace daemon provenance",
        )
        .await?;
        assert_eq!(replay, first);

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].task_id.as_deref(), Some("task-daemon"));
        assert!(runs[0].outcome_detail.as_deref().is_some_and(|detail| {
            detail.contains("task_id=task-daemon")
                && detail.contains("error=daemon failure")
                && !detail.contains("cli-sync:proj-failure")
        }));

        let failure_events: Vec<_> = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect();
        assert_eq!(failure_events.len(), 1);
        let details: crate::contexts::milestone_record::model::CompletionJournalDetails =
            serde_json::from_str(
                failure_events[0]
                    .details
                    .as_deref()
                    .expect("bead_failed details should be present"),
            )?;
        assert_eq!(details.task_id.as_deref(), Some("task-daemon"));
        assert!(details
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| !detail.contains("cli-sync:proj-failure")));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_replaces_cli_sync_placeholder_with_real_task_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);
        let failed_at = started_at + Duration::seconds(10);

        start_attempt(base, &milestone_id, "task-daemon", "run-1", started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "cli-sync:proj-failure",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "cli placeholder failure",
        )
        .await?;

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-daemon",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "daemon replay should repair placeholder provenance",
        )
        .await?;
        assert_eq!(
            replay,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let runs = read_task_runs(&FsTaskRunLineageStore, base, &milestone_id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].task_id.as_deref(), Some("task-daemon"));
        assert!(runs[0].outcome_detail.as_deref().is_some_and(|detail| {
            detail.contains("task_id=task-daemon")
                && !detail.contains("cli-sync:proj-failure")
                && detail.contains("error=cli placeholder failure")
        }));

        let failed_event = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .find(|event| event.event_type == MilestoneEventType::BeadFailed)
            .expect("bead_failed event should exist");
        let details: crate::contexts::milestone_record::model::CompletionJournalDetails =
            serde_json::from_str(
                failed_event
                    .details
                    .as_deref()
                    .expect("bead_failed details should be present"),
            )?;
        assert_eq!(details.task_id.as_deref(), Some("task-daemon"));
        assert!(details.outcome_detail.as_deref().is_some_and(|detail| {
            detail.contains("task_id=task-daemon") && !detail.contains("cli-sync:proj-failure")
        }));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_repairs_missing_snapshot_and_journal_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);
        let failed_at = started_at + Duration::seconds(10);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        FsTaskRunLineageStore.repair_task_run_terminal(
            base,
            &milestone_id,
            "bead-failure",
            "proj-failure",
            "run-1",
            Some("plan-v1"),
            started_at,
            TaskRunOutcome::Failed,
            Some("task_id=task-1\nattempt=1\nerror=partial write".to_owned()),
            failed_at,
        )?;

        let snapshot_before =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base, &milestone_id)?;
        assert_eq!(snapshot_before.progress.failed_beads, 0);
        let failed_events_before = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .count();
        assert_eq!(failed_events_before, 0);

        let outcome = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "partial write",
        )
        .await?;

        assert_eq!(
            outcome,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let snapshot_after =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base, &milestone_id)?;
        assert_eq!(snapshot_after.progress.failed_beads, 1);
        let failed_event = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .find(|event| event.event_type == MilestoneEventType::BeadFailed)
            .expect("failure replay should repair bead_failed journal state");
        assert_eq!(failed_event.timestamp, failed_at);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_does_not_clobber_newer_running_retry(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let base_time = Utc::now();

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-1",
            base_time + Duration::seconds(1),
        )?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            base_time + Duration::seconds(1),
            base_time + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-2",
            base_time + Duration::seconds(11),
        )?;
        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            base_time + Duration::seconds(1),
            base_time + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(replay, first);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Running
        );
        assert_eq!(controller.active_task_id.as_deref(), Some("proj-failure"));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_skips_stale_replay_when_newer_retry_uses_different_run_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let stale_started_at = Utc::now() + Duration::seconds(1);
        let newer_started_at = stale_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-2", newer_started_at)?;

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            stale_started_at,
            stale_started_at + Duration::seconds(5),
            "stale failure replay",
        )
        .await?;
        assert_eq!(
            replay,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Running
        );
        assert_eq!(controller.active_task_id.as_deref(), Some("proj-failure"));

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-1")
                && entry.outcome == TaskRunOutcome::Failed
                && entry.started_at == stale_started_at
        }));
        assert!(runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-2")
                && entry.outcome == TaskRunOutcome::Running
                && entry.started_at == newer_started_at
        }));

        let failed_events = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect::<Vec<_>>();
        assert_eq!(failed_events.len(), 1);
        let repaired_details: crate::contexts::milestone_record::model::CompletionJournalDetails =
            serde_json::from_str(
                failed_events[0]
                    .details
                    .as_deref()
                    .expect("bead_failed details"),
            )?;
        assert_eq!(repaired_details.run_id.as_deref(), Some("run-1"));
        assert_eq!(repaired_details.started_at, stale_started_at);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_counts_replacement_project_retries_and_skips_stale_old_project_replays(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt_with_project(
            base,
            &milestone_id,
            "task-original",
            "proj-original",
            "run-1",
            first_started_at,
        )?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-original",
            "proj-original",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "original project failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );
        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            milestone_controller::ControllerTransitionRequest::new(
                milestone_controller::MilestoneControllerState::Selecting,
                "replacement project selected for retry",
            ),
            second_started_at,
        )?;

        start_attempt_with_project(
            base,
            &milestone_id,
            "task-replacement",
            "proj-replacement",
            "run-2",
            second_started_at,
        )?;
        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-original",
            "proj-original",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "original project failure",
        )
        .await?;
        assert_eq!(replay, first);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Running
        );
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some("proj-replacement")
        );

        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-replacement",
            "proj-replacement",
            milestone_id.as_str(),
            "run-2",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "replacement project failure",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_counts_resumed_retries_with_same_run_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(base, &milestone_id, "task-1", "run-1", second_started_at)?;
        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure after resume",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(
            runs.len(),
            1,
            "same run_id retry should reuse the lineage row"
        );
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[0].started_at, second_started_at);

        let failed_events = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect::<Vec<_>>();
        assert_eq!(failed_events.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_does_not_clobber_newer_retry_with_same_run_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;

        start_attempt(base, &milestone_id, "task-1", "run-1", second_started_at)?;
        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(replay, first);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Running
        );
        assert_eq!(controller.active_task_id.as_deref(), Some("proj-failure"));

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Failed && entry.started_at == first_started_at
        }));
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Running && entry.started_at == second_started_at
        }));

        let raw_runs = read_task_runs(&FsTaskRunLineageStore, base, &milestone_id)?;
        assert_eq!(raw_runs.len(), 2);
        assert!(raw_runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-1")
                && entry.outcome == TaskRunOutcome::Failed
                && entry.started_at == first_started_at
        }));
        assert!(raw_runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-1")
                && entry.outcome == TaskRunOutcome::Running
                && entry.started_at == second_started_at
        }));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_repairs_missing_historical_event_for_stale_same_run_id_replay(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(base, &milestone_id, "task-1", "run-1", second_started_at)?;
        remove_bead_failed_events(base, &milestone_id)?;

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            replay,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Running
        );

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Failed && entry.started_at == first_started_at
        }));
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Running && entry.started_at == second_started_at
        }));

        let failed_events = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect::<Vec<_>>();
        assert_eq!(failed_events.len(), 1);
        let repaired_details: crate::contexts::milestone_record::model::CompletionJournalDetails =
            serde_json::from_str(
                failed_events[0]
                    .details
                    .as_deref()
                    .expect("bead_failed details"),
            )?;
        assert_eq!(repaired_details.run_id.as_deref(), Some("run-1"));
        assert_eq!(repaired_details.started_at, first_started_at);

        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure after repaired replay",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_repairs_missing_historical_event_when_newer_same_run_attempt_already_failed(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;

        start_attempt(base, &milestone_id, "task-1", "run-1", second_started_at)?;
        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let retained = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| {
                if event.event_type != MilestoneEventType::BeadFailed {
                    return true;
                }
                let Ok(details) = serde_json::from_str::<
                    crate::contexts::milestone_record::model::CompletionJournalDetails,
                >(
                    event
                        .details
                        .as_deref()
                        .expect("bead_failed details should be present"),
                ) else {
                    return true;
                };
                details.started_at != first_started_at
            })
            .map(|event| serde_json::to_string(&event))
            .collect::<Result<Vec<_>, _>>()?;
        std::fs::write(
            base.join(format!(
                ".ralph-burning/milestones/{milestone_id}/journal.ndjson"
            )),
            format!("{}\n", retained.join("\n")),
        )?;

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            replay,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let failed_events = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect::<Vec<_>>();
        assert_eq!(failed_events.len(), 2);
        assert!(failed_events.iter().any(|event| {
            serde_json::from_str::<
                    crate::contexts::milestone_record::model::CompletionJournalDetails,
                >(event.details.as_deref().unwrap_or_default())
                .ok()
                .is_some_and(|details| details.started_at == first_started_at)
        }));

        let raw_runs = read_task_runs(&FsTaskRunLineageStore, base, &milestone_id)?;
        assert_eq!(raw_runs.len(), 2);
        assert!(raw_runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Failed && entry.started_at == first_started_at
        }));
        assert!(raw_runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Failed && entry.started_at == second_started_at
        }));

        let third_started_at = second_started_at + Duration::seconds(30);
        start_attempt(base, &milestone_id, "task-1", "run-1", third_started_at)?;
        let third = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            third_started_at,
            third_started_at + Duration::seconds(5),
            "third failure",
        )
        .await?;
        assert_eq!(
            third,
            FailureReconciliationOutcome::EscalatedToOperator {
                attempt_number: 3,
                reason: "bead bead-failure failed 3 times: third failure".to_owned(),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_keeps_attempt_number_after_historical_backfill(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;

        start_attempt(base, &milestone_id, "task-1", "run-1", second_started_at)?;
        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let retained = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| {
                if event.event_type != MilestoneEventType::BeadFailed {
                    return true;
                }
                let Ok(details) = serde_json::from_str::<
                    crate::contexts::milestone_record::model::CompletionJournalDetails,
                >(
                    event
                        .details
                        .as_deref()
                        .expect("bead_failed details should be present"),
                ) else {
                    return true;
                };
                details.started_at != first_started_at
            })
            .map(|event| serde_json::to_string(&event))
            .collect::<Result<Vec<_>, _>>()?;
        std::fs::write(
            base.join(format!(
                ".ralph-burning/milestones/{milestone_id}/journal.ndjson"
            )),
            format!("{}\n", retained.join("\n")),
        )?;

        let repaired_first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            repaired_first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let replayed_second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure replay",
        )
        .await?;
        assert_eq!(
            replayed_second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let failure_events: Vec<_> = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect();
        assert_eq!(failure_events.len(), 2);
        let second_details = failure_events
            .iter()
            .filter_map(|event| {
                serde_json::from_str::<
                    crate::contexts::milestone_record::model::CompletionJournalDetails,
                >(event.details.as_deref().unwrap_or_default())
                .ok()
            })
            .find(|details| details.started_at == second_started_at)
            .expect("second attempt failure details should exist");
        assert_eq!(second_details.task_id.as_deref(), Some("task-1"));
        assert!(second_details
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("attempt=2")));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_backfill_repairs_later_attempt_markers_without_replay(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;

        start_attempt(base, &milestone_id, "task-1", "run-2", second_started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-2",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure",
        )
        .await?;

        remove_bead_failed_events(base, &milestone_id)?;
        FsTaskRunLineageStore.repair_task_run_terminal(
            base,
            &milestone_id,
            "bead-failure",
            "proj-failure",
            "run-2",
            Some("plan-v1"),
            second_started_at,
            TaskRunOutcome::Failed,
            Some("task_id=task-1\nattempt=1\nerror=second failure".to_owned()),
            second_started_at + Duration::seconds(5),
        )?;

        let stale_second_event = MilestoneJournalEvent::new(
            MilestoneEventType::BeadFailed,
            second_started_at + Duration::seconds(5),
        )
        .with_bead("bead-failure")
        .with_details(
            TaskRunEntry {
                milestone_id: milestone_id.to_string(),
                bead_id: "bead-failure".to_owned(),
                project_id: "proj-failure".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("task_id=task-1\nattempt=1\nerror=second failure".to_owned()),
                started_at: second_started_at,
                finished_at: Some(second_started_at + Duration::seconds(5)),
                task_id: Some("task-1".to_owned()),
            }
            .completion_journal_details(),
        );
        FsMilestoneJournalStore.append_event_if_missing(
            base,
            &milestone_id,
            &stale_second_event,
        )?;

        let repaired_first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            repaired_first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let repaired_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        let second_run = repaired_runs
            .iter()
            .find(|entry| {
                entry.project_id == "proj-failure"
                    && entry.run_id.as_deref() == Some("run-2")
                    && entry.started_at == second_started_at
            })
            .expect("second failed run should remain present");
        assert_eq!(
            second_run.outcome_detail.as_deref(),
            Some("task_id=task-1\nattempt=2\nerror=second failure")
        );

        let second_event = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .filter_map(|event| {
                serde_json::from_str::<
                    crate::contexts::milestone_record::model::CompletionJournalDetails,
                >(event.details.as_deref().unwrap_or_default())
                .ok()
            })
            .find(|details| {
                details.project_id == "proj-failure"
                    && details.run_id.as_deref() == Some("run-2")
                    && details.started_at == second_started_at
            })
            .expect("second failed journal event should remain present");
        assert_eq!(
            second_event.outcome_detail.as_deref(),
            Some("task_id=task-1\nattempt=2\nerror=second failure")
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_stale_replay_does_not_repair_other_attempt_ordinals_when_history_is_already_consistent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);
        let third_started_at = second_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;

        start_attempt(base, &milestone_id, "task-1", "run-2", second_started_at)?;
        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-2",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure",
        )
        .await?;

        FsTaskRunLineageStore.repair_task_run_terminal(
            base,
            &milestone_id,
            "bead-failure",
            "proj-failure",
            "run-2",
            Some("plan-v1"),
            second_started_at,
            TaskRunOutcome::Failed,
            Some("task_id=task-1\nattempt=99\nerror=second failure".to_owned()),
            second_started_at + Duration::seconds(5),
        )?;

        start_attempt(base, &milestone_id, "task-1", "run-3", third_started_at)?;
        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            replay,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let repaired_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        let second_run = repaired_runs
            .iter()
            .find(|entry| {
                entry.run_id.as_deref() == Some("run-2") && entry.started_at == second_started_at
            })
            .expect("second failed attempt should remain present");
        assert_eq!(
            second_run.outcome_detail.as_deref(),
            Some("task_id=task-1\nattempt=99\nerror=second failure"),
            "no-op stale replay repair should not rewrite unrelated later attempts"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_counts_same_run_partial_write_attempts_toward_escalation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);
        let third_started_at = second_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        FsTaskRunLineageStore.update_task_run(
            base,
            &milestone_id,
            "bead-failure",
            "proj-failure",
            "run-1",
            Some("plan-v1"),
            first_started_at,
            TaskRunOutcome::Failed,
            Some("task_id=task-1\nattempt=1\nerror=partial first failure".to_owned()),
            first_started_at + Duration::seconds(5),
        )?;

        start_attempt(base, &milestone_id, "task-1", "run-1", second_started_at)?;
        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            second_started_at,
            second_started_at + Duration::seconds(5),
            "second failure",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(base, &milestone_id, "task-1", "run-1", third_started_at)?;
        let third = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            third_started_at,
            third_started_at + Duration::seconds(5),
            "third failure",
        )
        .await?;
        assert_eq!(
            third,
            FailureReconciliationOutcome::EscalatedToOperator {
                attempt_number: 3,
                reason: "bead bead-failure failed 3 times: third failure".to_owned(),
            }
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_upgrades_plain_failure_detail_with_task_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);
        let failed_at = started_at + Duration::seconds(5);

        start_attempt(base, &milestone_id, "task-structured", "run-1", started_at)?;
        FsTaskRunLineageStore.update_task_run(
            base,
            &milestone_id,
            "bead-failure",
            "proj-failure",
            "run-1",
            Some("plan-v1"),
            started_at,
            TaskRunOutcome::Failed,
            Some("stale failure".to_owned()),
            failed_at,
        )?;

        let outcome = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-structured",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "stale failure",
        )
        .await?;
        assert_eq!(
            outcome,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].task_id.as_deref(), Some("task-structured"));
        assert_eq!(
            runs[0].outcome_detail.as_deref(),
            Some("task_id=task-structured\nattempt=1\nerror=stale failure")
        );

        let failure_event = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .find(|event| event.event_type == MilestoneEventType::BeadFailed)
            .expect("failure replay should repair bead_failed journal state");
        let details: crate::contexts::milestone_record::model::CompletionJournalDetails =
            serde_json::from_str(
                failure_event
                    .details
                    .as_deref()
                    .expect("bead_failed details should be present"),
            )?;
        assert_eq!(details.task_id.as_deref(), Some("task-structured"));
        assert_eq!(
            details.outcome_detail.as_deref(),
            Some("task_id=task-structured\nattempt=1\nerror=stale failure")
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_stale_replay_prefers_real_error_over_superseded_retry_placeholder(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let first_started_at = Utc::now() + Duration::seconds(1);
        let second_started_at = first_started_at + Duration::seconds(30);

        start_attempt(base, &milestone_id, "task-1", "run-1", first_started_at)?;
        start_attempt(base, &milestone_id, "task-1", "run-2", second_started_at)?;

        let outcome = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            first_started_at,
            first_started_at + Duration::seconds(5),
            "actual first failure summary",
        )
        .await?;
        assert_eq!(
            outcome,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let failure_event = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .find(|event| {
                event.event_type == MilestoneEventType::BeadFailed
                    && serde_json::from_str::<
                        crate::contexts::milestone_record::model::CompletionJournalDetails,
                    >(event.details.as_deref().unwrap_or_default())
                    .ok()
                    .is_some_and(|details| details.run_id.as_deref() == Some("run-1"))
            })
            .expect("stale replay should repair the first failure event");
        let details: crate::contexts::milestone_record::model::CompletionJournalDetails =
            serde_json::from_str(
                failure_event
                    .details
                    .as_deref()
                    .expect("bead_failed details should be present"),
            )?;
        let outcome_detail = details
            .outcome_detail
            .as_deref()
            .expect("repaired historical failure should include outcome detail");
        assert!(outcome_detail.contains("error=actual first failure summary"));
        assert!(!outcome_detail.contains("superseded by retry"));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_does_not_mutate_beads_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        let export_before = write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        let _ = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "no bead mutation expected",
        )
        .await?;

        let export_after = std::fs::read_to_string(base.join(".beads/issues.jsonl"))?;
        assert_eq!(export_before, export_after);
        assert!(
            !base.join(".beads/.br-unsynced-mutations.d").exists(),
            "failure reconciliation should not prepare any br mutation records"
        );

        Ok(())
    }
}

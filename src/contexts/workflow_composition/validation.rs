#![forbid(unsafe_code)]

//! Local validation orchestration for docs/CI flows, standard-flow evidence
//! injection, and post-review pre-commit gating.
//!
//! Docs and CI validation stages run configured commands locally instead of
//! invoking an agent backend. Standard flow injects local validation evidence
//! into the review context. Pre-commit checks gate review approval.

use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::adapters::validation_runner::{
    render_validation_group, run_command_group, run_pre_commit_checks, ValidationGroupResult,
    DEFAULT_VALIDATION_COMMAND_TIMEOUT_SECS,
};
use crate::contexts::project_run_record::model::{
    ArtifactRecord, LogLevel, PayloadRecord, RuntimeLogEntry,
};
use crate::contexts::project_run_record::service::{PayloadArtifactWritePort, RuntimeLogWritePort};
use crate::contexts::workflow_composition::panel_contracts::{RecordKind, RecordProducer};
use crate::contexts::workflow_composition::payloads::{ReviewOutcome, ValidationPayload};
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::{ProjectId, StageCursor, StageId};
use crate::shared::error::AppResult;

/// Per-command timeout for validation commands.
fn validation_timeout() -> Duration {
    Duration::from_secs(DEFAULT_VALIDATION_COMMAND_TIMEOUT_SECS)
}

// ── Local validation for docs/CI stages ─────────────────────────────────────

/// Run local validation commands for docs or CI stages.
///
/// Returns a `ValidationPayload` with deterministic outcome: all commands pass
/// => `Approved`, any failure => `RequestChanges`.
pub async fn run_local_validation(
    stage_id: StageId,
    commands: &[String],
    repo_root: &Path,
) -> (ValidationPayload, ValidationGroupResult) {
    let group_name = match stage_id {
        StageId::DocsValidation => "docs_validation",
        StageId::CiValidation => "ci_validation",
        _ => "local_validation",
    };

    let group_result =
        run_command_group(group_name, commands, repo_root, validation_timeout()).await;

    let outcome = if group_result.passed {
        ReviewOutcome::Approved
    } else {
        ReviewOutcome::RequestChanges
    };

    let evidence = group_result.evidence_summary();
    let findings_or_gaps = if group_result.passed {
        vec![]
    } else {
        group_result.failing_excerpts()
    };
    let follow_up_or_amendments = if group_result.passed {
        vec![]
    } else {
        group_result
            .commands
            .iter()
            .filter(|cmd| !cmd.passed)
            .map(|cmd| format!("Fix failing command: `{}`", cmd.command))
            .collect()
    };

    let payload = ValidationPayload {
        outcome,
        evidence,
        findings_or_gaps,
        follow_up_or_amendments,
    };

    (payload, group_result)
}

/// Persist local validation evidence as supporting history records.
pub fn persist_local_validation_evidence(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    stage_id: StageId,
    cursor: &StageCursor,
    group_result: &ValidationGroupResult,
    record_id_base: &str,
) -> AppResult<()> {
    let now = Utc::now();
    let payload_id = format!("{}-local-validation", record_id_base);
    let artifact_id = format!("{}-artifact", payload_id);

    let producer = RecordProducer::LocalValidation {
        command: group_result.group_name.clone(),
    };

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: serde_json::to_value(group_result)?,
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };

    let artifact_record = ArtifactRecord {
        artifact_id,
        payload_id,
        stage_id,
        created_at: now,
        content: render_validation_group(group_result),
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer),
        completion_round: cursor.completion_round,
    };

    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )
}

// ── Standard flow: local validation evidence for review context ─────────────

/// Run standard_commands and return evidence for injection into review context.
///
/// Returns `None` if no standard commands are configured.
pub async fn run_standard_validation_evidence(
    commands: &[String],
    repo_root: &Path,
) -> Option<ValidationGroupResult> {
    if commands.is_empty() {
        return None;
    }
    let result = run_command_group(
        "standard_validation",
        commands,
        repo_root,
        validation_timeout(),
    )
    .await;
    Some(result)
}

/// Build a JSON value representing local validation evidence for injection
/// into the review stage prompt context.
pub fn build_local_validation_context(group_result: &ValidationGroupResult) -> serde_json::Value {
    json!({
        "local_validation": {
            "group": group_result.group_name,
            "passed": group_result.passed,
            "summary": group_result.evidence_summary(),
            "failing_excerpts": group_result.failing_excerpts(),
        }
    })
}

// ── Pre-commit gating ───────────────────────────────────────────────────────

/// Run pre-commit checks and return the result.
///
/// This is called after review approval in the standard flow. If any check
/// fails, the caller must invalidate reviewer approval, persist evidence,
/// and return to implementation remediation.
pub async fn run_pre_commit(
    repo_root: &Path,
    effective_config: &EffectiveConfig,
) -> ValidationGroupResult {
    let policy = effective_config.validation_policy();

    run_pre_commit_checks(
        repo_root,
        policy.pre_commit_fmt,
        policy.pre_commit_clippy,
        policy.pre_commit_nix_build,
        policy.pre_commit_fmt_auto_fix,
        validation_timeout(),
    )
    .await
}

/// Check whether all pre-commit checks are disabled (nothing to run).
pub fn pre_commit_checks_disabled(effective_config: &EffectiveConfig) -> bool {
    let policy = effective_config.validation_policy();
    !policy.pre_commit_fmt && !policy.pre_commit_clippy && !policy.pre_commit_nix_build
}

/// Build remediation context from a failed pre-commit result.
pub fn pre_commit_remediation_context(group_result: &ValidationGroupResult) -> serde_json::Value {
    json!({
        "source_stage": "pre_commit",
        "cycle": 0,
        "follow_up_or_amendments": group_result
            .commands
            .iter()
            .filter(|cmd| !cmd.passed)
            .map(|cmd| format!("Fix pre-commit failure: `{}`", cmd.command))
            .collect::<Vec<_>>(),
        "findings_or_gaps": group_result.failing_excerpts(),
    })
}

/// Persist pre-commit validation evidence as supporting records and emit a
/// runtime log entry.
pub fn persist_pre_commit_evidence(
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    stage_id: StageId,
    cursor: &StageCursor,
    group_result: &ValidationGroupResult,
    record_id_base: &str,
) -> AppResult<()> {
    // Persist supporting evidence.
    persist_local_validation_evidence(
        artifact_write,
        base_dir,
        project_id,
        stage_id,
        cursor,
        group_result,
        &format!("{}-precommit", record_id_base),
    )?;

    // Emit runtime log entry.
    let status = if group_result.passed {
        "passed"
    } else {
        "failed"
    };
    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: if group_result.passed {
                LogLevel::Info
            } else {
                LogLevel::Warn
            },
            source: "pre_commit".to_owned(),
            message: format!(
                "pre-commit checks {}: {}",
                status,
                group_result.evidence_summary().join("; ")
            ),
        },
    );

    Ok(())
}

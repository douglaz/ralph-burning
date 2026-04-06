#![forbid(unsafe_code)]

//! Completion panel orchestration, consensus math, and aggregate persistence.
//!
//! Implements the completion panel workflow:
//! 1. Resolve completer panel backends.
//! 2. Invoke every resolved completer.
//! 3. Persist each completer response as `StageSupporting`.
//! 4. Compute aggregate verdict: `complete` only when
//!    `complete_votes >= min_completers` AND
//!    `complete_votes / total_voters >= consensus_threshold`.
//! 5. Persist the aggregate as canonical `StageAggregate`.
//! 6. If `complete`, advance to acceptance_qa.
//! 7. Otherwise reopen work (advance completion_round, return to planning).

use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use crate::adapters::process_backend::processed_contract_schema_value;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::ResolvedPanelMember;
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::model::{
    ArtifactRecord, LogLevel, PayloadRecord, RuntimeLogEntry,
};
use crate::contexts::project_run_record::service::{PayloadArtifactWritePort, RuntimeLogWritePort};
use crate::contexts::project_run_record::task_prompt_contract;
use crate::contexts::workflow_composition::panel_contracts::{
    CompletionAggregatePayload, CompletionVerdict, CompletionVotePayload, RecordKind,
    RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::contexts::workspace_governance::template_catalog;
use crate::shared::domain::{
    BackendFamily, BackendRole, FailureClass, ProjectId, ResolvedBackendTarget, RunId,
    SessionPolicy, StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

/// Result of a completion panel execution.
pub struct CompletionResult {
    /// The aggregate payload for journal persistence.
    pub aggregate_payload: Value,
    /// The aggregate artifact text.
    pub aggregate_artifact: String,
    /// The computed verdict.
    pub verdict: CompletionVerdict,
}

/// Compute the completion verdict from vote counts and thresholds.
pub fn compute_completion_verdict(
    complete_votes: usize,
    total_voters: usize,
    min_completers: usize,
    consensus_threshold: f64,
) -> CompletionVerdict {
    if total_voters == 0 {
        return CompletionVerdict::ContinueWork;
    }

    let ratio = complete_votes as f64 / total_voters as f64;
    if complete_votes >= min_completers && ratio >= consensus_threshold {
        CompletionVerdict::Complete
    } else {
        CompletionVerdict::ContinueWork
    }
}

fn build_completer_prompt(
    base_dir: &Path,
    project_id: Option<&ProjectId>,
    prompt_text: &str,
    schema_str: &str,
) -> AppResult<String> {
    let task_prompt_contract_block =
        task_prompt_contract::stage_consumer_guidance_for_prompt(prompt_text);
    template_catalog::resolve_and_render(
        "completion_panel_completer",
        base_dir,
        project_id,
        &[
            ("task_prompt_contract", task_prompt_contract_block.as_str()),
            ("prompt_text", prompt_text),
            ("json_schema", schema_str),
        ],
    )
}

/// Execute the completion panel workflow.
///
/// Invokes all completers, persists supporting records, computes the aggregate,
/// and returns the result. The caller is responsible for persisting the aggregate
/// record and transitioning the run state.
#[allow(clippy::too_many_arguments)]
pub async fn execute_completion_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    completers: &[ResolvedPanelMember],
    min_completers: usize,
    consensus_threshold: f64,
    prompt_reference: &str,
    rollback_count: u32,
    timeout_for_backend: &dyn Fn(BackendFamily) -> Duration,
    cancellation_token: CancellationToken,
) -> AppResult<CompletionResult>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::CompletionPanel;
    let prompt_path = project_root.join(prompt_reference);
    let project_prompt =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for completion: {e}"),
        })?;

    // ── Invoke completers and persist supporting records ──────────────────
    // Availability filtering is done by the engine before snapshot
    // persistence; all completers in the panel are available and expected
    // to execute. Non-exhausted invocation errors are propagated directly.
    // BackendExhausted errors degrade gracefully: the completer is skipped
    // and execution continues with remaining members.
    let mut complete_votes = 0usize;
    let mut continue_votes = 0usize;
    let mut executed_voters: Vec<String> = Vec::new();
    let mut total_exhausted_count: usize = 0;
    let mut last_exhaustion_error: Option<AppError> = None;

    for (i, member) in completers.iter().enumerate() {
        let completer_target = &member.target;
        let completer_timeout = timeout_for_backend(completer_target.backend.family);
        let invocation_result = invoke_completer(
            agent_service,
            base_dir,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            completer_target,
            &project_prompt,
            i,
            completer_timeout,
            cancellation_token.clone(),
            Some(project_id),
        )
        .await;

        let (vote_payload, producer) = match invocation_result {
            Ok(result) => result,
            Err(error) => {
                let is_exhausted = error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted);
                if is_exhausted {
                    total_exhausted_count += 1;
                    tracing::warn!(
                        completer = i,
                        backend = %completer_target.backend.family,
                        "completer unavailable (backend exhausted), proceeding with remaining completers"
                    );
                    let _ = log_write.append_runtime_log(
                        base_dir,
                        project_id,
                        &RuntimeLogEntry {
                            timestamp: Utc::now(),
                            level: LogLevel::Warn,
                            source: "completion_panel".to_owned(),
                            message: format!(
                                "completer {i} ({}) unavailable (backend exhausted), skipping",
                                completer_target.backend.family
                            ),
                        },
                    );
                    // Early-exit quorum check: if the executed voters plus
                    // the remaining members cannot reach the effective
                    // minimum, fail immediately instead of continuing.
                    let effective_min = min_completers.saturating_sub(total_exhausted_count).max(1);
                    if executed_voters.len() + completers.len().saturating_sub(i + 1)
                        < effective_min
                    {
                        last_exhaustion_error = Some(error);
                        break;
                    }
                    continue;
                }
                return Err(error);
            }
        };

        let vote: CompletionVotePayload =
            serde_json::from_value(vote_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: completer_target.backend.family.to_string(),
                    contract_id: "completion:completer".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("completer output schema validation failed: {e}"),
                }
            })?;

        let (backend_family, model_id) = super::require_agent_record_producer(
            &producer,
            completer_target.backend.family.as_str(),
            "completion:completer",
            "completion panel invocations must produce agent metadata",
        )?;
        let voter_id = format!("{backend_family}:{model_id}");
        let vote_artifact =
            renderers::render_completion_vote(stage_id, &vote, &producer.to_string());

        persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &vote_payload,
            &vote_artifact,
            producer,
            &format!("completer-{i}"),
        )?;

        executed_voters.push(voter_id);
        if vote.vote_complete {
            complete_votes += 1;
        } else {
            continue_votes += 1;
        }
    }

    // ── Check min_completers after execution ──────────────────────────────
    // Reduce effective quorum for exhausted backends: allow proceeding with
    // at least 1 completer if available, rather than requiring the original
    // min_completers which may be unachievable.
    let total_voters = executed_voters.len();
    let effective_min_completers = min_completers.saturating_sub(total_exhausted_count).max(1);
    if total_voters < effective_min_completers {
        // When the shortfall is entirely due to backend exhaustion,
        // propagate the last BackendExhausted error so the engine's
        // failure-class-aware handling can apply (e.g., non-retryable).
        if let Some(exhaustion_error) = last_exhaustion_error {
            return Err(exhaustion_error);
        }
        return Err(AppError::InsufficientPanelMembers {
            panel: "completion".to_owned(),
            resolved: total_voters,
            minimum: effective_min_completers,
        });
    }

    // ── Compute aggregate verdict ─────────────────────────────────────────
    // Use effective quorum so that a degraded panel (some backends exhausted)
    // can still reach a Complete verdict with fewer voters.
    let verdict = compute_completion_verdict(
        complete_votes,
        total_voters,
        effective_min_completers,
        consensus_threshold,
    );

    let aggregate = CompletionAggregatePayload {
        verdict,
        complete_votes,
        continue_votes,
        total_voters,
        consensus_threshold,
        min_completers,
        effective_min_completers,
        exhausted_count: total_exhausted_count,
        executed_voters,
    };

    let aggregate_payload = serde_json::to_value(&aggregate)?;
    let aggregate_artifact = renderers::render_completion_aggregate(stage_id, &aggregate);

    Ok(CompletionResult {
        aggregate_payload,
        aggregate_artifact,
        verdict,
    })
}

/// Invoke a single completer and return the raw parsed payload plus producer.
#[allow(clippy::too_many_arguments)]
async fn invoke_completer<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    prompt_text: &str,
    index: usize,
    timeout: Duration,
    cancellation_token: CancellationToken,
    project_id: Option<&ProjectId>,
) -> AppResult<(Value, RecordProducer)>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id = format!(
        "{}-{}-completer-{index}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );

    let schema_str = serde_json::to_string_pretty(&processed_contract_schema_value(
        &InvocationContract::Panel {
            stage_id,
            role: "completer".to_owned(),
        },
        target.backend.family,
    ))?;

    let prompt = build_completer_prompt(base_dir, project_id, prompt_text, &schema_str)?;

    let request = InvocationRequest {
        invocation_id,
        project_root: project_root.to_path_buf(),
        working_dir: backend_working_dir.to_path_buf(),
        contract: InvocationContract::Panel {
            stage_id,
            role: "completer".to_owned(),
        },
        role: BackendRole::Planner,
        resolved_target: target.clone(),
        payload: InvocationPayload {
            prompt,
            context: Value::Null,
        },
        timeout,
        cancellation_token,
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: cursor.attempt,
    };

    let envelope = agent_service.invoke(request).await?;
    let producer = super::agent_record_producer(&envelope.metadata);
    Ok((envelope.parsed_payload, producer))
}

/// Persist a completion supporting record.
#[allow(clippy::too_many_arguments)]
fn persist_supporting_record(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    stage_id: StageId,
    rollback_count: u32,
    payload: &Value,
    artifact_content: &str,
    producer: RecordProducer,
    suffix: &str,
) -> AppResult<()> {
    let now = Utc::now();
    let base_id = format!(
        "{}-{}-{suffix}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let payload_id = if rollback_count == 0 {
        format!("{base_id}-payload")
    } else {
        format!("{base_id}-rb{rollback_count}-payload")
    };
    let artifact_id = if rollback_count == 0 {
        format!("{base_id}-artifact")
    } else {
        format!("{base_id}-rb{rollback_count}-artifact")
    };

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: payload.clone(),
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };

    let artifact_record = ArtifactRecord {
        artifact_id,
        payload_id,
        stage_id,
        created_at: now,
        content: artifact_content.to_owned(),
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::*;
    use crate::adapters::process_backend::{
        processed_contract_schema_value, ProcessBackendAdapter,
    };
    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationPayload, InvocationRequest,
    };
    use crate::shared::domain::{ResolvedBackendTarget, SessionPolicy};

    fn extract_prompt_schema(prompt: &str) -> serde_json::Value {
        let (_, after_heading) = prompt
            .split_once("## Authoritative JSON Schema")
            .expect("prompt should contain authoritative schema heading");
        let (_, after_open) = after_heading
            .split_once("```json\n")
            .expect("prompt should contain schema code fence");
        let (schema_text, _) = after_open
            .split_once("\n```")
            .expect("prompt schema fence should close");
        serde_json::from_str(schema_text).expect("prompt schema should parse")
    }

    fn extract_transport_schema(args: &[String]) -> serde_json::Value {
        let schema_idx = args
            .iter()
            .position(|arg| arg == "--json-schema")
            .expect("claude transport should include --json-schema");
        serde_json::from_str(&args[schema_idx + 1]).expect("transport schema should parse")
    }

    fn extract_stdin_schema(stdin_payload: &str) -> serde_json::Value {
        let (_, schema_text) = stdin_payload
            .split_once("Return ONLY valid JSON matching the following schema:\n")
            .expect("stdin should contain schema marker");
        serde_json::from_str(schema_text.trim()).expect("stdin schema should parse")
    }

    #[test]
    fn build_completer_prompt_surfaces_task_prompt_contract_guidance() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_completer_prompt(
            tmp.path(),
            None,
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            "{}",
        )
        .expect("render prompt");

        assert!(prompt.contains("## Task Prompt Contract"));
        assert!(prompt.contains("## Must-Do Scope"));
    }

    #[test]
    fn build_completer_prompt_omits_task_prompt_contract_guidance_for_generic_prompts() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_completer_prompt(tmp.path(), None, "# Prompt\n\nGeneric.", "{}")
            .expect("render prompt");

        assert!(!prompt.contains("## Task Prompt Contract"));
    }

    #[test]
    fn build_completer_prompt_allows_legacy_override_without_task_prompt_contract_placeholder() {
        let tmp = tempdir().expect("tempdir");
        let template_dir = tmp.path().join(".ralph-burning").join("templates");
        fs::create_dir_all(&template_dir).expect("template dir");
        fs::write(
            template_dir.join("completion_panel_completer.md"),
            "LEGACY COMPLETER\n\n{{prompt_text}}\n\n{{json_schema}}",
        )
        .expect("template override");

        let prompt = build_completer_prompt(tmp.path(), None, "# Prompt\n\nGeneric.", "{}")
            .expect("render prompt");

        assert!(prompt.starts_with("LEGACY COMPLETER"));
        assert!(prompt.contains("# Prompt"));
    }

    #[tokio::test]
    async fn build_completer_prompt_keeps_claude_prompt_schema_in_sync_with_transport_schema() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_completer_prompt(
            tmp.path(),
            None,
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            &serde_json::to_string_pretty(&processed_contract_schema_value(
                &InvocationContract::Panel {
                    stage_id: StageId::CompletionPanel,
                    role: "completer".to_owned(),
                },
                BackendFamily::Claude,
            ))
            .expect("schema"),
        )
        .expect("render prompt");

        let request = InvocationRequest {
            invocation_id: "completion-schema-sync".to_owned(),
            project_root: tmp.path().to_path_buf(),
            working_dir: tmp.path().to_path_buf(),
            contract: InvocationContract::Panel {
                stage_id: StageId::CompletionPanel,
                role: "completer".to_owned(),
            },
            role: BackendRole::Planner,
            resolved_target: ResolvedBackendTarget::new(BackendFamily::Claude, "claude-test"),
            payload: InvocationPayload {
                prompt: prompt.clone(),
                context: Value::Null,
            },
            timeout: Duration::from_secs(30),
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::NewSession,
            prior_session: None,
            attempt_number: 1,
        };

        let adapter = ProcessBackendAdapter::new();
        let prepared = adapter
            .build_command(&request)
            .await
            .expect("prepare command");
        let prompt_schema = extract_prompt_schema(&prompt);
        let transport_schema = extract_transport_schema(prepared.args());
        let stdin_schema = extract_stdin_schema(prepared.stdin_payload());

        assert_eq!(prompt_schema, transport_schema);
        assert_eq!(stdin_schema, transport_schema);
    }

    // ── Panel degradation tests ──────────────────────────────────────────

    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::json;

    use crate::adapters::fs::{
        FileSystem, FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore,
        FsRuntimeLogWriteStore, FsSessionStore,
    };
    use crate::contexts::agent_execution::model::{
        InvocationEnvelope, InvocationMetadata, RawOutputReference, TokenCounts,
    };
    use crate::contexts::agent_execution::policy::ResolvedPanelMember;
    use crate::contexts::agent_execution::service::AgentExecutionPort;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::project_run_record::service::{self, CreateProjectInput};
    use crate::contexts::workspace_governance;
    use crate::shared::domain::{
        BackendFamily, FailureClass, FlowPreset, RunId, StageCursor, StageId,
    };
    use crate::shared::error::AppError;

    #[derive(Clone, Default)]
    struct RecordingCompletionAdapter {
        requests: Arc<Mutex<Vec<String>>>,
        exhausted_indices: Arc<HashSet<usize>>,
    }

    impl RecordingCompletionAdapter {
        fn with_exhausted(indices: &[usize]) -> Self {
            Self {
                exhausted_indices: Arc::new(indices.iter().copied().collect()),
                ..Default::default()
            }
        }
    }

    impl AgentExecutionPort for RecordingCompletionAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &crate::contexts::agent_execution::model::InvocationContract,
        ) -> crate::shared::error::AppResult<()> {
            Ok(())
        }

        async fn check_availability(
            &self,
            _backend: &ResolvedBackendTarget,
        ) -> crate::shared::error::AppResult<()> {
            Ok(())
        }

        async fn invoke(
            &self,
            request: crate::contexts::agent_execution::model::InvocationRequest,
        ) -> crate::shared::error::AppResult<InvocationEnvelope> {
            self.requests
                .lock()
                .expect("lock")
                .push(request.invocation_id.clone());

            for &idx in self.exhausted_indices.iter() {
                let fragment = format!("completer-{idx}");
                if request.invocation_id.contains(&fragment) {
                    return Err(AppError::InvocationFailed {
                        backend: request.resolved_target.backend.family.to_string(),
                        contract_id: "completion_panel:completer".to_owned(),
                        failure_class: FailureClass::BackendExhausted,
                        details: "completer backend exhausted".to_owned(),
                    });
                }
            }

            let payload = json!({
                "vote_complete": true,
                "evidence": ["looks good"],
                "remaining_work": []
            });

            Ok(InvocationEnvelope {
                raw_output_reference: RawOutputReference::Inline("{}".to_owned()),
                parsed_payload: payload,
                metadata: InvocationMetadata {
                    invocation_id: request.invocation_id,
                    duration: Duration::from_millis(1),
                    token_counts: TokenCounts::default(),
                    backend_used: request.resolved_target.backend.clone(),
                    model_used: request.resolved_target.model.clone(),
                    adapter_reported_backend: None,
                    adapter_reported_model: None,
                    attempt_number: request.attempt_number,
                    session_id: None,
                    session_reused: false,
                },
                timestamp: Utc::now(),
            })
        }

        async fn cancel(&self, _invocation_id: &str) -> crate::shared::error::AppResult<()> {
            Ok(())
        }
    }

    fn setup_completion_project(
        base_dir: &Path,
        project_name: &str,
    ) -> (crate::shared::domain::ProjectId, PathBuf) {
        workspace_governance::initialize_workspace(base_dir, Utc::now()).expect("workspace init");
        let project_id = crate::shared::domain::ProjectId::new(project_name).expect("project id");
        service::create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: project_name.to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt\n\nTest prompt.\n".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt\n\nTest prompt.\n"),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("project creation");
        let project_root = base_dir
            .join(".ralph-burning")
            .join("projects")
            .join(project_id.as_str());
        (project_id, project_root)
    }

    #[tokio::test]
    async fn completion_degrades_when_one_completer_exhausted() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, project_root) = setup_completion_project(base_dir, "cp-one-exhausted");
        let adapter = RecordingCompletionAdapter::with_exhausted(&[0]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-cp-one-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::CompletionPanel, 1, 1, 1).expect("cursor");
        let completers = vec![
            ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "completer-0-model"),
                required: true,
                configured_index: 0,
            },
            ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "completer-1-model"),
                required: true,
                configured_index: 1,
            },
        ];

        let result = execute_completion_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_root,
            base_dir,
            &project_id,
            &run_id,
            &cursor,
            &completers,
            2,    // min_completers
            0.66, // consensus_threshold
            "prompt.md",
            0, // rollback_count
            &|_| Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await;

        assert!(
            result.is_ok(),
            "panel should succeed with one exhausted completer: {}",
            result.err().map_or("ok".to_owned(), |e| e.to_string())
        );
        let completion = result.unwrap();
        assert_eq!(completion.verdict, super::CompletionVerdict::Complete);
    }

    #[tokio::test]
    async fn completion_fails_when_all_completers_exhausted() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, project_root) = setup_completion_project(base_dir, "cp-all-exhausted");
        let adapter = RecordingCompletionAdapter::with_exhausted(&[0, 1]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-cp-all-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::CompletionPanel, 1, 1, 1).expect("cursor");
        let completers = vec![
            ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "completer-0-model"),
                required: true,
                configured_index: 0,
            },
            ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "completer-1-model"),
                required: true,
                configured_index: 1,
            },
        ];

        let error = execute_completion_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_root,
            base_dir,
            &project_id,
            &run_id,
            &cursor,
            &completers,
            2,    // min_completers
            0.66, // consensus_threshold
            "prompt.md",
            0, // rollback_count
            &|_| Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("panel should fail when all completers are exhausted");

        // When all completers are exhausted, the early-exit quorum check
        // propagates the BackendExhausted error so the engine's
        // failure-class-aware handling applies (non-retryable).
        assert!(
            error
                .failure_class()
                .is_some_and(|fc| fc == FailureClass::BackendExhausted),
            "error should carry BackendExhausted failure class: {error:?}"
        );
    }
}

/// Persist the aggregate record for the completion panel.
#[allow(clippy::too_many_arguments)]
pub fn persist_aggregate_record(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    stage_id: StageId,
    rollback_count: u32,
    payload: &Value,
    artifact_content: &str,
) -> AppResult<()> {
    let now = Utc::now();
    let base_id = format!(
        "{}-{}-aggregate-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let payload_id = if rollback_count == 0 {
        format!("{base_id}-payload")
    } else {
        format!("{base_id}-rb{rollback_count}-payload")
    };
    let artifact_id = if rollback_count == 0 {
        format!("{base_id}-artifact")
    } else {
        format!("{base_id}-rb{rollback_count}-artifact")
    };

    let producer = RecordProducer::System {
        component: "completion_aggregator".to_owned(),
    };

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: payload.clone(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };

    let artifact_record = ArtifactRecord {
        artifact_id,
        payload_id,
        stage_id,
        created_at: now,
        content: artifact_content.to_owned(),
        record_kind: RecordKind::StageAggregate,
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

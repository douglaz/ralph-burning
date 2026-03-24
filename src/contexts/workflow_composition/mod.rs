pub mod checkpoints;
pub mod completion;
pub mod contracts;
pub mod drift;
pub mod engine;
pub mod final_review;
pub mod panel_contracts;
pub mod payloads;
pub mod prompt_review;
pub mod renderers;
pub mod retry_policy;
pub mod validation;

use crate::contexts::agent_execution::model::InvocationMetadata;
use crate::contexts::workflow_composition::panel_contracts::RecordProducer;
use crate::shared::domain::{FailureClass, FlowPreset, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidationProfile {
    pub name: &'static str,
    pub summary: &'static str,
    pub final_review_enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowDefinition {
    pub preset: FlowPreset,
    pub description: &'static str,
    pub stages: &'static [StageId],
    pub validation_profile: ValidationProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowSemantics {
    pub planning_stage: StageId,
    pub execution_stage: StageId,
    pub remediation_trigger_stages: &'static [StageId],
    pub late_stages: &'static [StageId],
    pub prompt_review_stage: Option<StageId>,
}

const STANDARD_STAGES: [StageId; 8] = [
    StageId::PromptReview,
    StageId::Planning,
    StageId::Implementation,
    StageId::Qa,
    StageId::Review,
    StageId::CompletionPanel,
    StageId::AcceptanceQa,
    StageId::FinalReview,
];

const QUICK_DEV_STAGES: [StageId; 4] = [
    StageId::PlanAndImplement,
    StageId::Review,
    StageId::ApplyFixes,
    StageId::FinalReview,
];

const DOCS_CHANGE_STAGES: [StageId; 4] = [
    StageId::DocsPlan,
    StageId::DocsUpdate,
    StageId::DocsValidation,
    StageId::Review,
];

const CI_IMPROVEMENT_STAGES: [StageId; 4] = [
    StageId::CiPlan,
    StageId::CiUpdate,
    StageId::CiValidation,
    StageId::Review,
];

const STANDARD_REMEDIATION_TRIGGER_STAGES: [StageId; 2] = [StageId::Qa, StageId::Review];
const STANDARD_LATE_STAGES: [StageId; 3] = [
    StageId::CompletionPanel,
    StageId::AcceptanceQa,
    StageId::FinalReview,
];
const QUICK_DEV_REMEDIATION_TRIGGER_STAGES: [StageId; 1] = [StageId::Review];
const QUICK_DEV_LATE_STAGES: [StageId; 1] = [StageId::FinalReview];
const DOCS_CHANGE_REMEDIATION_TRIGGER_STAGES: [StageId; 2] =
    [StageId::DocsValidation, StageId::Review];
const CI_IMPROVEMENT_REMEDIATION_TRIGGER_STAGES: [StageId; 2] =
    [StageId::CiValidation, StageId::Review];

const FLOW_DEFINITIONS: [FlowDefinition; 4] = [
    FlowDefinition {
        preset: FlowPreset::Standard,
        description:
            "Full delivery flow with planning, implementation, QA, review, and acceptance.",
        stages: &STANDARD_STAGES,
        validation_profile: ValidationProfile {
            name: "standard-default",
            summary: "Full validation suite with completion and acceptance checks.",
            final_review_enabled: true,
        },
    },
    FlowDefinition {
        preset: FlowPreset::QuickDev,
        description: "Fast delivery flow for small code changes with lightweight review.",
        stages: &QUICK_DEV_STAGES,
        validation_profile: ValidationProfile {
            name: "quick-dev-default",
            summary: "Lightweight panel with final review enabled by default.",
            final_review_enabled: true,
        },
    },
    FlowDefinition {
        preset: FlowPreset::DocsChange,
        description: "Documentation-focused flow for planning, content updates, and validation.",
        stages: &DOCS_CHANGE_STAGES,
        validation_profile: ValidationProfile {
            name: "docs-default",
            summary: "Documentation validation with final review disabled by default.",
            final_review_enabled: false,
        },
    },
    FlowDefinition {
        preset: FlowPreset::CiImprovement,
        description: "CI improvement flow for automation planning, updates, and validation.",
        stages: &CI_IMPROVEMENT_STAGES,
        validation_profile: ValidationProfile {
            name: "ci-default",
            summary: "Automation validation with final review disabled by default.",
            final_review_enabled: false,
        },
    },
];

pub fn built_in_flows() -> &'static [FlowDefinition] {
    &FLOW_DEFINITIONS
}

pub fn flow_definition(preset: FlowPreset) -> &'static FlowDefinition {
    match preset {
        FlowPreset::Standard => &FLOW_DEFINITIONS[0],
        FlowPreset::QuickDev => &FLOW_DEFINITIONS[1],
        FlowPreset::DocsChange => &FLOW_DEFINITIONS[2],
        FlowPreset::CiImprovement => &FLOW_DEFINITIONS[3],
    }
}

pub fn flow_definition_by_id(flow_id: &str) -> AppResult<&'static FlowDefinition> {
    let preset = flow_id.parse::<FlowPreset>()?;
    Ok(flow_definition(preset))
}

pub fn flow_semantics(preset: FlowPreset) -> FlowSemantics {
    match preset {
        FlowPreset::Standard => FlowSemantics {
            planning_stage: StageId::Planning,
            execution_stage: StageId::Implementation,
            remediation_trigger_stages: &STANDARD_REMEDIATION_TRIGGER_STAGES,
            late_stages: &STANDARD_LATE_STAGES,
            prompt_review_stage: Some(StageId::PromptReview),
        },
        FlowPreset::QuickDev => FlowSemantics {
            planning_stage: StageId::PlanAndImplement,
            execution_stage: StageId::ApplyFixes,
            remediation_trigger_stages: &QUICK_DEV_REMEDIATION_TRIGGER_STAGES,
            late_stages: &QUICK_DEV_LATE_STAGES,
            prompt_review_stage: None,
        },
        FlowPreset::DocsChange => FlowSemantics {
            planning_stage: StageId::DocsPlan,
            execution_stage: StageId::DocsUpdate,
            remediation_trigger_stages: &DOCS_CHANGE_REMEDIATION_TRIGGER_STAGES,
            late_stages: &[],
            prompt_review_stage: None,
        },
        FlowPreset::CiImprovement => FlowSemantics {
            planning_stage: StageId::CiPlan,
            execution_stage: StageId::CiUpdate,
            remediation_trigger_stages: &CI_IMPROVEMENT_REMEDIATION_TRIGGER_STAGES,
            late_stages: &[],
            prompt_review_stage: None,
        },
    }
}

pub fn stage_plan_for_flow(preset: FlowPreset, prompt_review_enabled: bool) -> Vec<StageId> {
    let flow_def = flow_definition(preset);
    let semantics = flow_semantics(preset);

    flow_def
        .stages
        .iter()
        .copied()
        .filter(|stage_id| {
            prompt_review_enabled || Some(*stage_id) != semantics.prompt_review_stage
        })
        .collect()
}

pub fn agent_record_producer(metadata: &InvocationMetadata) -> RecordProducer {
    RecordProducer::Agent {
        backend_family: metadata.backend_used.family.to_string(),
        model_id: metadata.model_used.model_id.clone(),
        // Defensive: service.rs already sets these to None when they match the
        // resolved target, so the .filter() should be a no-op in practice.
        // We re-check here to guard against callers that construct metadata
        // directly (e.g. in tests) without enforcing that invariant.
        adapter_reported_backend_family: metadata
            .adapter_reported_backend
            .as_ref()
            .filter(|backend| backend.family != metadata.backend_used.family)
            .map(|backend| backend.family.to_string()),
        adapter_reported_model_id: metadata
            .adapter_reported_model
            .as_ref()
            .filter(|model| {
                model.backend_family != metadata.model_used.backend_family
                    || model.model_id != metadata.model_used.model_id
            })
            .map(|model| model.model_id.clone()),
    }
}

pub fn require_agent_record_producer<'a>(
    producer: &'a RecordProducer,
    backend: &str,
    contract_id: &str,
    details: &str,
) -> AppResult<(&'a str, &'a str)> {
    match producer {
        RecordProducer::Agent {
            backend_family,
            model_id,
            ..
        } => Ok((backend_family.as_str(), model_id.as_str())),
        _ => Err(AppError::InvocationFailed {
            backend: backend.to_owned(),
            contract_id: contract_id.to_owned(),
            failure_class: FailureClass::DomainValidationFailure,
            details: details.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::contexts::agent_execution::model::{InvocationMetadata, TokenCounts};
    use crate::shared::domain::{BackendFamily, BackendSpec, ModelSpec};
    use crate::shared::error::AppError;

    use super::*;

    #[test]
    fn agent_record_producer_uses_invocation_metadata_fields() {
        let metadata = InvocationMetadata {
            invocation_id: "invocation-1".to_owned(),
            duration: Duration::from_millis(1),
            token_counts: TokenCounts::default(),
            backend_used: BackendSpec::from_family(BackendFamily::Claude),
            model_used: ModelSpec::new(BackendFamily::Claude, "claude-opus-4-6"),
            adapter_reported_backend: None,
            adapter_reported_model: None,
            attempt_number: 1,
            session_id: None,
            session_reused: false,
        };

        assert_eq!(
            agent_record_producer(&metadata),
            RecordProducer::Agent {
                backend_family: "claude".to_owned(),
                model_id: "claude-opus-4-6".to_owned(),
                adapter_reported_backend_family: None,
                adapter_reported_model_id: None,
            }
        );
    }

    #[test]
    fn agent_record_producer_includes_adapter_reported_values_when_they_differ() {
        let metadata = InvocationMetadata {
            invocation_id: "invocation-1".to_owned(),
            duration: Duration::from_millis(1),
            token_counts: TokenCounts::default(),
            backend_used: BackendSpec::from_family(BackendFamily::Claude),
            model_used: ModelSpec::new(BackendFamily::Claude, "claude-opus-4-6"),
            adapter_reported_backend: Some(BackendSpec::from_family(BackendFamily::OpenRouter)),
            adapter_reported_model: Some(ModelSpec::new(
                BackendFamily::OpenRouter,
                "openai/gpt-4.1",
            )),
            attempt_number: 1,
            session_id: None,
            session_reused: false,
        };

        assert_eq!(
            agent_record_producer(&metadata),
            RecordProducer::Agent {
                backend_family: "claude".to_owned(),
                model_id: "claude-opus-4-6".to_owned(),
                adapter_reported_backend_family: Some("openrouter".to_owned()),
                adapter_reported_model_id: Some("openai/gpt-4.1".to_owned()),
            }
        );
    }

    #[test]
    fn require_agent_record_producer_returns_backend_and_model() {
        let producer = RecordProducer::Agent {
            backend_family: "claude".to_owned(),
            model_id: "claude-opus-4-6".to_owned(),
            adapter_reported_backend_family: None,
            adapter_reported_model_id: None,
        };

        assert_eq!(
            require_agent_record_producer(
                &producer,
                "claude",
                "completion:completer",
                "completion panel invocations must produce agent metadata",
            )
            .expect("agent producer"),
            ("claude", "claude-opus-4-6")
        );
    }

    #[test]
    fn require_agent_record_producer_rejects_non_agent_producer() {
        let error = require_agent_record_producer(
            &RecordProducer::System {
                component: "completion_aggregator".to_owned(),
            },
            "claude",
            "completion:completer",
            "completion panel invocations must produce agent metadata",
        )
        .expect_err("non-agent producers should be rejected");

        assert!(matches!(
            error,
            AppError::InvocationFailed {
                backend,
                contract_id,
                failure_class: FailureClass::DomainValidationFailure,
                details,
            } if backend == "claude"
                && contract_id == "completion:completer"
                && details == "completion panel invocations must produce agent metadata"
        ));
    }
}

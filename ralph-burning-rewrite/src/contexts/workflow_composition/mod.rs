pub mod contracts;
pub mod payloads;
pub mod renderers;

use crate::shared::domain::{FlowPreset, StageId};
use crate::shared::error::AppResult;

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

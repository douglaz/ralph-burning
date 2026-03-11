use ralph_burning::contexts::workflow_composition::{built_in_flows, flow_definition};
use ralph_burning::shared::domain::{FlowPreset, StageId};

#[test]
fn built_in_flow_registry_exposes_all_four_presets_in_order() {
    let presets: Vec<_> = built_in_flows()
        .iter()
        .map(|definition| definition.preset)
        .collect();

    assert_eq!(
        vec![
            FlowPreset::Standard,
            FlowPreset::QuickDev,
            FlowPreset::DocsChange,
            FlowPreset::CiImprovement
        ],
        presets
    );
}

#[test]
fn standard_flow_stage_order_matches_spec() {
    assert_eq!(
        &[
            StageId::PromptReview,
            StageId::Planning,
            StageId::Implementation,
            StageId::Qa,
            StageId::Review,
            StageId::CompletionPanel,
            StageId::AcceptanceQa,
            StageId::FinalReview,
        ],
        flow_definition(FlowPreset::Standard).stages
    );
}

#[test]
fn quick_dev_flow_stage_order_matches_spec() {
    assert_eq!(
        &[
            StageId::PlanAndImplement,
            StageId::Review,
            StageId::ApplyFixes,
            StageId::FinalReview,
        ],
        flow_definition(FlowPreset::QuickDev).stages
    );
}

#[test]
fn docs_change_flow_stage_order_matches_spec() {
    assert_eq!(
        &[
            StageId::DocsPlan,
            StageId::DocsUpdate,
            StageId::DocsValidation,
            StageId::Review,
        ],
        flow_definition(FlowPreset::DocsChange).stages
    );
}

#[test]
fn ci_improvement_flow_stage_order_matches_spec() {
    assert_eq!(
        &[
            StageId::CiPlan,
            StageId::CiUpdate,
            StageId::CiValidation,
            StageId::Review,
        ],
        flow_definition(FlowPreset::CiImprovement).stages
    );
}

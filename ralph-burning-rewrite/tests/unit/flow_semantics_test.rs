use ralph_burning::contexts::workflow_composition::{flow_semantics, stage_plan_for_flow};
use ralph_burning::shared::domain::{FlowPreset, StageId};

#[test]
fn standard_flow_semantics_match_spec() {
    let semantics = flow_semantics(FlowPreset::Standard);

    assert_eq!(semantics.planning_stage, StageId::Planning);
    assert_eq!(semantics.execution_stage, StageId::Implementation);
    assert_eq!(
        semantics.remediation_trigger_stages,
        &[StageId::Qa, StageId::Review]
    );
    assert_eq!(
        semantics.late_stages,
        &[
            StageId::CompletionPanel,
            StageId::AcceptanceQa,
            StageId::FinalReview,
        ]
    );
    assert_eq!(semantics.prompt_review_stage, Some(StageId::PromptReview));
}

#[test]
fn quick_dev_flow_semantics_are_defined() {
    let semantics = flow_semantics(FlowPreset::QuickDev);

    assert_eq!(semantics.planning_stage, StageId::PlanAndImplement);
    assert_eq!(semantics.execution_stage, StageId::ApplyFixes);
    assert_eq!(semantics.remediation_trigger_stages, &[StageId::Review]);
    assert_eq!(semantics.late_stages, &[StageId::FinalReview]);
    assert_eq!(semantics.prompt_review_stage, None);
}

#[test]
fn docs_change_flow_semantics_match_spec() {
    let semantics = flow_semantics(FlowPreset::DocsChange);

    assert_eq!(semantics.planning_stage, StageId::DocsPlan);
    assert_eq!(semantics.execution_stage, StageId::DocsUpdate);
    assert_eq!(
        semantics.remediation_trigger_stages,
        &[StageId::DocsValidation, StageId::Review]
    );
    assert!(semantics.late_stages.is_empty());
    assert_eq!(semantics.prompt_review_stage, None);
}

#[test]
fn ci_improvement_flow_semantics_match_spec() {
    let semantics = flow_semantics(FlowPreset::CiImprovement);

    assert_eq!(semantics.planning_stage, StageId::CiPlan);
    assert_eq!(semantics.execution_stage, StageId::CiUpdate);
    assert_eq!(
        semantics.remediation_trigger_stages,
        &[StageId::CiValidation, StageId::Review]
    );
    assert!(semantics.late_stages.is_empty());
    assert_eq!(semantics.prompt_review_stage, None);
}

#[test]
fn standard_stage_plan_for_flow_honors_prompt_review_toggle() {
    assert_eq!(
        stage_plan_for_flow(FlowPreset::Standard, true),
        vec![
            StageId::PromptReview,
            StageId::Planning,
            StageId::Implementation,
            StageId::Qa,
            StageId::Review,
            StageId::CompletionPanel,
            StageId::AcceptanceQa,
            StageId::FinalReview,
        ]
    );
    assert_eq!(
        stage_plan_for_flow(FlowPreset::Standard, false),
        vec![
            StageId::Planning,
            StageId::Implementation,
            StageId::Qa,
            StageId::Review,
            StageId::CompletionPanel,
            StageId::AcceptanceQa,
            StageId::FinalReview,
        ]
    );
}

#[test]
fn non_standard_stage_plans_ignore_prompt_review_toggle() {
    assert_eq!(
        stage_plan_for_flow(FlowPreset::DocsChange, true),
        stage_plan_for_flow(FlowPreset::DocsChange, false)
    );
    assert_eq!(
        stage_plan_for_flow(FlowPreset::CiImprovement, true),
        stage_plan_for_flow(FlowPreset::CiImprovement, false)
    );
}

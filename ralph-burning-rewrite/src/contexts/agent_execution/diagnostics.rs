//! Read-only backend diagnostics service and stable DTOs for
//! `backend list`, `backend check`, `backend show-effective`, and `backend probe`.
//!
//! This module reuses the existing `BackendPolicyService`, `BackendResolver`,
//! and `EffectiveConfig` primitives instead of introducing parallel resolution
//! logic.

use std::fmt;

use serde::Serialize;

use crate::contexts::agent_execution::policy::{BackendPolicyService, stage_to_policy_role};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::workspace_governance::config::{
    EffectiveConfig, DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
};
use crate::contexts::workflow_composition::{flow_definition, FlowDefinition};
use crate::shared::domain::{
    BackendFamily, BackendPolicyRole, FlowPreset, PanelBackendSpec, ResolvedBackendTarget, StageId,
};
use crate::shared::error::{AppError, AppResult};

// ── Stable DTOs ─────────────────────────────────────────────────────────────

/// One row in `backend list` output.
#[derive(Debug, Clone, Serialize)]
pub struct BackendListEntry {
    pub family: String,
    pub display_name: String,
    pub enabled: bool,
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compile_only: Option<bool>,
}

/// Aggregate result of `backend check`.
#[derive(Debug, Clone, Serialize)]
pub struct BackendCheckResult {
    pub passed: bool,
    pub failures: Vec<BackendCheckFailure>,
}

/// A single readiness failure from `backend check`.
#[derive(Debug, Clone, Serialize)]
pub struct BackendCheckFailure {
    pub role: String,
    pub backend_family: String,
    pub failure_kind: BackendCheckFailureKind,
    pub details: String,
    pub config_source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendCheckFailureKind {
    BackendDisabled,
    PanelMinimumViolation,
    RequiredMemberUnavailable,
    AvailabilityFailure,
}

impl fmt::Display for BackendCheckFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackendDisabled => f.write_str("backend_disabled"),
            Self::PanelMinimumViolation => f.write_str("panel_minimum_violation"),
            Self::RequiredMemberUnavailable => f.write_str("required_member_unavailable"),
            Self::AvailabilityFailure => f.write_str("availability_failure"),
        }
    }
}

/// Stable DTO for `backend show-effective`.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveBackendView {
    pub base_backend: EffectiveFieldView,
    pub default_model: EffectiveFieldView,
    pub roles: Vec<RoleEffectiveView>,
    pub default_session_policy: String,
    pub default_timeout_seconds: u64,
}

/// A resolved field with its source precedence.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveFieldView {
    pub value: String,
    pub source: String,
}

/// Per-role effective resolution for `show-effective`.
#[derive(Debug, Clone, Serialize)]
pub struct RoleEffectiveView {
    pub role: String,
    pub backend_family: String,
    pub model_id: String,
    pub timeout_seconds: u64,
    pub session_policy: String,
    pub override_source: String,
}

/// Result of `backend probe`.
#[derive(Debug, Clone, Serialize)]
pub struct BackendProbeResult {
    pub role: String,
    pub flow: String,
    pub cycle: u32,
    pub target: ProbeTargetView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub panel: Option<PanelProbeView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeTargetView {
    pub backend_family: String,
    pub model_id: String,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PanelProbeView {
    pub panel_type: String,
    pub minimum: usize,
    pub resolved_count: usize,
    pub members: Vec<PanelMemberView>,
    pub omitted: Vec<PanelOmittedView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arbiter: Option<PanelMemberView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PanelMemberView {
    pub backend_family: String,
    pub model_id: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PanelOmittedView {
    pub backend_family: String,
    pub reason: String,
    pub was_optional: bool,
}

// ── Diagnostics Service ─────────────────────────────────────────────────────

/// Read-only diagnostics service that wraps `EffectiveConfig` and
/// `BackendPolicyService` to produce stable DTOs for CLI rendering.
pub struct BackendDiagnosticsService<'a> {
    config: &'a EffectiveConfig,
    policy: BackendPolicyService<'a>,
}

impl<'a> BackendDiagnosticsService<'a> {
    pub fn new(config: &'a EffectiveConfig) -> Self {
        Self {
            config,
            policy: BackendPolicyService::new(config),
        }
    }

    // ── list ────────────────────────────────────────────────────────────

    pub fn list_backends(&self) -> Vec<BackendListEntry> {
        let families = [
            BackendFamily::Claude,
            BackendFamily::Codex,
            BackendFamily::OpenRouter,
            BackendFamily::Stub,
        ];

        families
            .iter()
            .map(|family| {
                let enabled = self.policy.backend_enabled_public(*family);
                let transport = transport_mechanism(*family);
                let compile_only = if *family == BackendFamily::Stub {
                    Some(true)
                } else {
                    None
                };

                BackendListEntry {
                    family: family.as_str().to_owned(),
                    display_name: family.display_name().to_owned(),
                    enabled,
                    transport: transport.to_owned(),
                    compile_only,
                }
            })
            .collect()
    }

    // ── check ───────────────────────────────────────────────────────────

    pub fn check_backends(&self, flow: FlowPreset) -> BackendCheckResult {
        let mut failures = Vec::new();
        let flow_def = flow_definition(flow);

        // Check base backend
        let base = &self.config.backend_policy().base_backend;
        if !self.policy.backend_enabled_public(base.family) {
            failures.push(BackendCheckFailure {
                role: "base".to_owned(),
                backend_family: base.family.as_str().to_owned(),
                failure_kind: BackendCheckFailureKind::BackendDisabled,
                details: format!("base backend '{}' is disabled", base.family),
                config_source: "default_backend".to_owned(),
            });
        }

        // Check only roles that are relevant to the given flow's stages
        let flow_roles = roles_for_flow(flow_def);
        for role in &flow_roles {
            if let Err(error) = self.policy.resolve_role_target(*role, 1) {
                failures.push(BackendCheckFailure {
                    role: role.as_str().to_owned(),
                    backend_family: self.family_for_role(*role),
                    failure_kind: BackendCheckFailureKind::BackendDisabled,
                    details: error.to_string(),
                    config_source: self.config_source_for_role(*role),
                });
            }
        }

        // Check completion panel only if the flow includes it
        if flow_def.stages.contains(&StageId::CompletionPanel) {
            self.check_panel_members(
                &mut failures,
                "completion_panel",
                || {
                    let res = self.policy.resolve_completion_panel(1)?;
                    Ok(res
                        .completers
                        .into_iter()
                        .map(|m| (m.target, m.required))
                        .collect::<Vec<_>>())
                },
                &self.config.completion_policy().backends,
                self.config.completion_policy().min_completers,
                "completion.backends",
            );
        }

        // Check final review panel only if the flow includes it and it's enabled
        if flow_def.stages.contains(&StageId::FinalReview)
            && self.config.final_review_policy().enabled
        {
            self.check_panel_members(
                &mut failures,
                "final_review_panel",
                || {
                    let res = self.policy.resolve_final_review_panel(1)?;
                    Ok(res
                        .reviewers
                        .into_iter()
                        .map(|m| (m.target, m.required))
                        .collect::<Vec<_>>())
                },
                &self.config.final_review_policy().backends,
                self.config.final_review_policy().min_reviewers,
                "final_review.backends",
            );
        }

        // Check prompt review panel only if the flow includes it and it's enabled
        if flow_def.stages.contains(&StageId::PromptReview)
            && self.config.prompt_review_policy().enabled
        {
            self.check_panel_members(
                &mut failures,
                "prompt_review_panel",
                || {
                    let res = self.policy.resolve_prompt_review_panel(1)?;
                    Ok(res
                        .validators
                        .into_iter()
                        .map(|m| (m.target, m.required))
                        .collect::<Vec<_>>())
                },
                &self.config.prompt_review_policy().validator_backends,
                self.config.prompt_review_policy().min_reviewers,
                "prompt_review.validator_backends",
            );
        }

        BackendCheckResult {
            passed: failures.is_empty(),
            failures,
        }
    }

    /// Check when the backend adapter could not be constructed at all.
    /// The adapter construction failure is itself a readiness error that should
    /// be surfaced alongside any config-level failures.
    pub fn check_backends_with_adapter_failure(
        &self,
        flow: FlowPreset,
        adapter_err: &AppError,
    ) -> BackendCheckResult {
        let mut result = self.check_backends(flow);

        // The adapter could not be constructed, so all resolved targets are
        // effectively unavailable. Report a single top-level availability failure.
        result.failures.push(BackendCheckFailure {
            role: "adapter".to_owned(),
            backend_family: "unknown".to_owned(),
            failure_kind: BackendCheckFailureKind::AvailabilityFailure,
            details: format!("backend adapter construction failed: {}", adapter_err),
            config_source: "RALPH_BURNING_BACKEND".to_owned(),
        });
        result.passed = false;
        result
    }

    /// Check with real adapter availability. This is the async variant that
    /// uses the production adapter to verify binary/API readiness.
    ///
    /// Unlike the previous implementation, this does NOT deduplicate targets by
    /// family:model — every role/member is checked and failures are reported
    /// with the exact role/member identity.
    pub async fn check_backends_with_availability<A: AgentExecutionPort>(
        &self,
        flow: FlowPreset,
        adapter: &A,
    ) -> BackendCheckResult {
        let mut result = self.check_backends(flow);

        // Collect ALL resolved targets with their role/member identity.
        // No deduplication — every role/member that resolves to a target gets
        // its own availability check so failures preserve exact identity.
        let mut targets_to_check: Vec<(ResolvedBackendTarget, String, String)> = Vec::new();
        let flow_def = flow_definition(flow);
        let flow_roles = roles_for_flow(flow_def);

        // Stage-role targets
        for role in &flow_roles {
            if let Ok(target) = self.policy.resolve_role_target(*role, 1) {
                targets_to_check.push((
                    target,
                    role.as_str().to_owned(),
                    self.config_source_for_role(*role),
                ));
            }
        }

        // Panel-specific targets: completion panel members
        if flow_def.stages.contains(&StageId::CompletionPanel) {
            if let Ok(res) = self.policy.resolve_completion_panel(1) {
                for (idx, member) in res.completers.iter().enumerate() {
                    let member_role = format!("completion_panel.member[{}]", idx);
                    targets_to_check.push((
                        member.target.clone(),
                        member_role,
                        "completion.backends".to_owned(),
                    ));
                }
            }
        }

        // Panel-specific targets: final review panel members + planner + arbiter
        if flow_def.stages.contains(&StageId::FinalReview)
            && self.config.final_review_policy().enabled
        {
            if let Ok(res) = self.policy.resolve_final_review_panel(1) {
                targets_to_check.push((
                    res.planner.clone(),
                    "final_review_panel.planner".to_owned(),
                    "workflow.planner_backend".to_owned(),
                ));
                for (idx, member) in res.reviewers.iter().enumerate() {
                    let member_role = format!("final_review_panel.reviewer[{}]", idx);
                    targets_to_check.push((
                        member.target.clone(),
                        member_role,
                        "final_review.backends".to_owned(),
                    ));
                }
                targets_to_check.push((
                    res.arbiter.clone(),
                    "final_review_panel.arbiter".to_owned(),
                    "final_review.arbiter_backend".to_owned(),
                ));
            }
        }

        // Panel-specific targets: prompt review panel refiner + validators
        if flow_def.stages.contains(&StageId::PromptReview)
            && self.config.prompt_review_policy().enabled
        {
            if let Ok(res) = self.policy.resolve_prompt_review_panel(1) {
                targets_to_check.push((
                    res.refiner.clone(),
                    "prompt_review_panel.refiner".to_owned(),
                    "prompt_review.refiner_backend".to_owned(),
                ));
                for (idx, member) in res.validators.iter().enumerate() {
                    let member_role = format!("prompt_review_panel.validator[{}]", idx);
                    targets_to_check.push((
                        member.target.clone(),
                        member_role,
                        "prompt_review.validator_backends".to_owned(),
                    ));
                }
            }
        }

        for (target, role, config_source) in &targets_to_check {
            if let Err(error) = adapter.check_availability(target).await {
                result.failures.push(BackendCheckFailure {
                    role: role.clone(),
                    backend_family: target.backend.family.as_str().to_owned(),
                    failure_kind: BackendCheckFailureKind::AvailabilityFailure,
                    details: error.to_string(),
                    config_source: config_source.clone(),
                });
            }
        }

        result.passed = result.failures.is_empty();
        result
    }

    fn check_panel_members<F>(
        &self,
        failures: &mut Vec<BackendCheckFailure>,
        panel_name: &str,
        resolve_fn: F,
        configured_specs: &[PanelBackendSpec],
        minimum: usize,
        config_field: &str,
    ) where
        F: FnOnce() -> AppResult<Vec<(ResolvedBackendTarget, bool)>>,
    {
        match resolve_fn() {
            Err(error) => {
                // Classify the panel error with structural detail
                let (kind, backend_family, details) =
                    classify_panel_error_structured(&error, configured_specs, minimum);
                failures.push(BackendCheckFailure {
                    role: panel_name.to_owned(),
                    backend_family,
                    failure_kind: kind,
                    details,
                    config_source: config_field.to_owned(),
                });
            }
            Ok(resolved) if resolved.len() < minimum => {
                failures.push(BackendCheckFailure {
                    role: panel_name.to_owned(),
                    backend_family: "mixed".to_owned(),
                    failure_kind: BackendCheckFailureKind::PanelMinimumViolation,
                    details: format!(
                        "resolved {} member(s) but minimum is {}",
                        resolved.len(),
                        minimum
                    ),
                    config_source: config_field.to_owned(),
                });
            }
            Ok(_) => {}
        }
    }

    // ── show-effective ──────────────────────────────────────────────────

    pub fn show_effective(&self) -> EffectiveBackendView {
        let bp = self.config.backend_policy();

        let base_backend = EffectiveFieldView {
            value: bp.base_backend.display_string(),
            source: self.source_for_base_backend(),
        };

        let default_model = EffectiveFieldView {
            value: bp
                .default_model
                .clone()
                .unwrap_or_else(|| bp.base_backend.family.default_model_id().to_owned()),
            source: self.source_for_default_model(),
        };

        let roles = BackendPolicyRole::ALL
            .iter()
            .filter_map(|role| {
                let target = self.policy.resolve_role_target(*role, 1).ok()?;
                let timeout = self.policy.timeout_for_role(target.backend.family, *role);
                let override_source = self.override_source_for_role(*role);
                let session_policy = session_policy_for_role(*role);
                Some(RoleEffectiveView {
                    role: role.as_str().to_owned(),
                    backend_family: target.backend.family.as_str().to_owned(),
                    model_id: target.model.model_id.clone(),
                    timeout_seconds: timeout.as_secs(),
                    session_policy,
                    override_source,
                })
            })
            .collect();

        EffectiveBackendView {
            base_backend,
            default_model,
            roles,
            default_session_policy: "reuse_if_allowed".to_owned(),
            default_timeout_seconds: DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
        }
    }

    // ── probe ───────────────────────────────────────────────────────────

    pub fn probe(
        &self,
        role_str: &str,
        flow: FlowPreset,
        cycle: u32,
    ) -> AppResult<BackendProbeResult> {
        let cycle = if cycle == 0 { 1 } else { cycle };
        let flow_def = flow_definition(flow);

        // Check if this is a panel target
        match role_str {
            "completion_panel" => {
                validate_flow_has_stage(flow_def, StageId::CompletionPanel)?;
                return self.probe_completion_panel(flow, cycle);
            }
            "final_review_panel" => {
                validate_flow_has_stage(flow_def, StageId::FinalReview)?;
                return self.probe_final_review_panel(flow, cycle);
            }
            "prompt_review_panel" => {
                validate_flow_has_stage(flow_def, StageId::PromptReview)?;
                return self.probe_prompt_review_panel(flow, cycle);
            }
            _ => {}
        }

        // Parse as a policy role
        let role: BackendPolicyRole = role_str.parse()?;
        let target = self.policy.resolve_role_target(role, cycle)?;
        let timeout = self.policy.timeout_for_role(target.backend.family, role);

        Ok(BackendProbeResult {
            role: role.as_str().to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: ProbeTargetView {
                backend_family: target.backend.family.as_str().to_owned(),
                model_id: target.model.model_id,
                timeout_seconds: timeout.as_secs(),
            },
            panel: None,
        })
    }

    fn probe_completion_panel(
        &self,
        flow: FlowPreset,
        cycle: u32,
    ) -> AppResult<BackendProbeResult> {
        let resolution = self.policy.resolve_completion_panel(cycle)?;
        let timeout = self.policy.timeout_for_role(
            resolution.planner.backend.family,
            BackendPolicyRole::Completer,
        );

        let configured_specs = &self.config.completion_policy().backends;
        let minimum = self.config.completion_policy().min_completers;

        let (members, omitted) = self.build_panel_member_views(
            &resolution
                .completers
                .iter()
                .map(|m| (m.target.backend.family, m.target.model.model_id.clone(), m.required))
                .collect::<Vec<_>>(),
            configured_specs,
        );

        Ok(BackendProbeResult {
            role: "completion_panel".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: ProbeTargetView {
                backend_family: resolution.planner.backend.family.as_str().to_owned(),
                model_id: resolution.planner.model.model_id,
                timeout_seconds: timeout.as_secs(),
            },
            panel: Some(PanelProbeView {
                panel_type: "completion".to_owned(),
                minimum,
                resolved_count: members.len(),
                members,
                omitted,
                arbiter: None,
            }),
        })
    }

    fn probe_final_review_panel(
        &self,
        flow: FlowPreset,
        cycle: u32,
    ) -> AppResult<BackendProbeResult> {
        let resolution = self.policy.resolve_final_review_panel(cycle)?;
        let timeout = self.policy.timeout_for_role(
            resolution.planner.backend.family,
            BackendPolicyRole::FinalReviewer,
        );

        let configured_specs = &self.config.final_review_policy().backends;
        let minimum = self.config.final_review_policy().min_reviewers;

        let (members, omitted) = self.build_panel_member_views(
            &resolution
                .reviewers
                .iter()
                .map(|m| (m.target.backend.family, m.target.model.model_id.clone(), m.required))
                .collect::<Vec<_>>(),
            configured_specs,
        );

        // Include the arbiter in the output
        let arbiter = PanelMemberView {
            backend_family: resolution.arbiter.backend.family.as_str().to_owned(),
            model_id: resolution.arbiter.model.model_id.clone(),
            required: true,
        };

        Ok(BackendProbeResult {
            role: "final_review_panel".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: ProbeTargetView {
                backend_family: resolution.planner.backend.family.as_str().to_owned(),
                model_id: resolution.planner.model.model_id,
                timeout_seconds: timeout.as_secs(),
            },
            panel: Some(PanelProbeView {
                panel_type: "final_review".to_owned(),
                minimum,
                resolved_count: members.len(),
                members,
                omitted,
                arbiter: Some(arbiter),
            }),
        })
    }

    fn probe_prompt_review_panel(
        &self,
        flow: FlowPreset,
        cycle: u32,
    ) -> AppResult<BackendProbeResult> {
        let resolution = self.policy.resolve_prompt_review_panel(cycle)?;
        let timeout = self.policy.timeout_for_role(
            resolution.refiner.backend.family,
            BackendPolicyRole::PromptValidator,
        );

        let configured_specs = &self.config.prompt_review_policy().validator_backends;
        let minimum = self.config.prompt_review_policy().min_reviewers;

        let (members, omitted) = self.build_panel_member_views(
            &resolution
                .validators
                .iter()
                .map(|m| (m.target.backend.family, m.target.model.model_id.clone(), m.required))
                .collect::<Vec<_>>(),
            configured_specs,
        );

        Ok(BackendProbeResult {
            role: "prompt_review_panel".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: ProbeTargetView {
                backend_family: resolution.refiner.backend.family.as_str().to_owned(),
                model_id: resolution.refiner.model.model_id,
                timeout_seconds: timeout.as_secs(),
            },
            panel: Some(PanelProbeView {
                panel_type: "prompt_review".to_owned(),
                minimum,
                resolved_count: members.len(),
                members,
                omitted,
                arbiter: None,
            }),
        })
    }

    /// Probe with real adapter availability. Optional panel members that are
    /// enabled but unavailable are moved to `omitted` instead of `members`.
    /// Required unavailable members, planners, arbiters, and refiners cause the
    /// probe to fail with exact member identity and source field.
    pub async fn probe_with_availability<A: AgentExecutionPort>(
        &self,
        role_str: &str,
        flow: FlowPreset,
        cycle: u32,
        adapter: &A,
    ) -> AppResult<BackendProbeResult> {
        let mut result = self.probe(role_str, flow, cycle)?;

        // Check the primary target (planner for panels, role target for singular)
        {
            let family: BackendFamily = result.target.backend_family.parse()?;
            let target = ResolvedBackendTarget::new(family, result.target.model_id.clone());
            if let Err(err) = adapter.check_availability(&target).await {
                return Err(AppError::BackendUnavailable {
                    backend: family.as_str().to_owned(),
                    details: format!(
                        "required target '{}' (planner/primary) unavailable: {}",
                        role_str, err
                    ),
                });
            }
        }

        if let Some(panel) = &mut result.panel {
            // Check arbiter availability (required for final_review_panel)
            if let Some(arbiter) = &panel.arbiter {
                let family: BackendFamily = arbiter.backend_family.parse()?;
                let target = ResolvedBackendTarget::new(family, arbiter.model_id.clone());
                if let Err(err) = adapter.check_availability(&target).await {
                    return Err(AppError::BackendUnavailable {
                        backend: family.as_str().to_owned(),
                        details: format!(
                            "required arbiter for '{}' unavailable: {}",
                            role_str, err
                        ),
                    });
                }
            }

            // Check panel members: required unavailable → fail; optional unavailable → omit
            let mut available_members = Vec::new();
            for member in panel.members.drain(..) {
                let family: BackendFamily = member.backend_family.parse()?;
                let target = ResolvedBackendTarget::new(family, member.model_id.clone());
                match adapter.check_availability(&target).await {
                    Ok(()) => available_members.push(member),
                    Err(err) if !member.required => {
                        panel.omitted.push(PanelOmittedView {
                            backend_family: member.backend_family,
                            reason: "backend unavailable".to_owned(),
                            was_optional: true,
                        });
                    }
                    Err(err) => {
                        return Err(AppError::BackendUnavailable {
                            backend: member.backend_family.clone(),
                            details: format!(
                                "required panel member '{}/{}' unavailable: {}",
                                member.backend_family, member.model_id, err
                            ),
                        });
                    }
                }
            }

            // Check minimum satisfaction after filtering
            let panel_min = panel.minimum;
            if available_members.len() < panel_min {
                return Err(AppError::InsufficientPanelMembers {
                    panel: panel.panel_type.clone(),
                    resolved: available_members.len(),
                    minimum: panel_min,
                });
            }

            panel.members = available_members;
            panel.resolved_count = panel.members.len();
        }

        Ok(result)
    }

    fn build_panel_member_views(
        &self,
        resolved: &[(BackendFamily, String, bool)],
        configured_specs: &[PanelBackendSpec],
    ) -> (Vec<PanelMemberView>, Vec<PanelOmittedView>) {
        let mut members = Vec::new();
        let mut omitted = Vec::new();

        for (family, model_id, required) in resolved {
            members.push(PanelMemberView {
                backend_family: family.as_str().to_owned(),
                model_id: model_id.clone(),
                required: *required,
            });
        }

        // Check for omitted optional members from configured specs.
        // A member is omitted if its backend is disabled (not enabled in config).
        for spec in configured_specs {
            let backend = spec.backend();
            let already_resolved = resolved.iter().any(|(f, _, _)| *f == backend);
            if spec.is_optional() && !already_resolved && !self.policy.backend_enabled_public(backend) {
                omitted.push(PanelOmittedView {
                    backend_family: backend.as_str().to_owned(),
                    reason: "backend disabled".to_owned(),
                    was_optional: true,
                });
            }
        }

        (members, omitted)
    }

    // ── Helper methods ──────────────────────────────────────────────────

    fn family_for_role(&self, role: BackendPolicyRole) -> String {
        let bp = self.config.backend_policy();
        match role {
            BackendPolicyRole::Planner => bp
                .planner_backend
                .as_ref()
                .map(|s| s.family.as_str())
                .unwrap_or(bp.base_backend.family.as_str()),
            BackendPolicyRole::Implementer => bp
                .implementer_backend
                .as_ref()
                .map(|s| s.family.as_str())
                .unwrap_or(bp.base_backend.family.as_str()),
            BackendPolicyRole::Reviewer => bp
                .reviewer_backend
                .as_ref()
                .map(|s| s.family.as_str())
                .unwrap_or(bp.base_backend.family.as_str()),
            BackendPolicyRole::Qa | BackendPolicyRole::AcceptanceQa => bp
                .qa_backend
                .as_ref()
                .map(|s| s.family.as_str())
                .unwrap_or(bp.base_backend.family.as_str()),
            BackendPolicyRole::PromptReviewer => bp
                .prompt_review_refiner_backend
                .as_ref()
                .map(|s| s.family.as_str())
                .unwrap_or(bp.base_backend.family.as_str()),
            BackendPolicyRole::Arbiter => bp
                .final_review_arbiter_backend
                .as_ref()
                .map(|s| s.family.as_str())
                .unwrap_or(bp.base_backend.family.as_str()),
            _ => bp.base_backend.family.as_str(),
        }
        .to_owned()
    }

    fn config_source_for_role(&self, role: BackendPolicyRole) -> String {
        match role {
            BackendPolicyRole::Planner => "workflow.planner_backend",
            BackendPolicyRole::Implementer => "workflow.implementer_backend",
            BackendPolicyRole::Reviewer => "workflow.reviewer_backend",
            BackendPolicyRole::Qa | BackendPolicyRole::AcceptanceQa => "workflow.qa_backend",
            BackendPolicyRole::PromptReviewer => "prompt_review.refiner_backend",
            BackendPolicyRole::Arbiter => "final_review.arbiter_backend",
            _ => "default_backend",
        }
        .to_owned()
    }

    fn source_for_base_backend(&self) -> String {
        let entry = self.config.get("default_backend");
        match entry {
            Ok(entry) => entry.source.to_string(),
            Err(_) => "default".to_owned(),
        }
    }

    fn source_for_default_model(&self) -> String {
        let entry = self.config.get("default_model");
        match entry {
            Ok(entry) => entry.source.to_string(),
            Err(_) => "default".to_owned(),
        }
    }

    fn override_source_for_role(&self, role: BackendPolicyRole) -> String {
        let key = match role {
            BackendPolicyRole::Planner => "workflow.planner_backend",
            BackendPolicyRole::Implementer => "workflow.implementer_backend",
            BackendPolicyRole::Reviewer => "workflow.reviewer_backend",
            BackendPolicyRole::Qa | BackendPolicyRole::AcceptanceQa => "workflow.qa_backend",
            BackendPolicyRole::PromptReviewer => "prompt_review.refiner_backend",
            BackendPolicyRole::Arbiter => "final_review.arbiter_backend",
            // Panel member roles inherit from the base backend. Report the
            // effective source of `default_backend` so operators see where the
            // resolution actually comes from.
            BackendPolicyRole::Completer
            | BackendPolicyRole::FinalReviewer
            | BackendPolicyRole::PromptValidator => {
                let base_source = self.source_for_base_backend();
                return format!("default_backend ({})", base_source);
            }
        };

        match self.config.get(key) {
            Ok(entry) => {
                if entry.source == crate::contexts::workspace_governance::config::ConfigValueSource::Default {
                    // No explicit override set — role inherits from default_backend
                    let base_source = self.source_for_base_backend();
                    format!("default_backend ({})", base_source)
                } else {
                    entry.source.to_string()
                }
            }
            Err(_) => {
                // Key not found means no explicit override — inherits from base
                let base_source = self.source_for_base_backend();
                format!("default_backend ({})", base_source)
            }
        }
    }
}

// ── Free functions ──────────────────────────────────────────────────────────

fn transport_mechanism(family: BackendFamily) -> &'static str {
    match family {
        BackendFamily::Claude => "process (claude CLI)",
        BackendFamily::Codex => "process (codex CLI)",
        BackendFamily::OpenRouter => "http (OpenRouter API)",
        BackendFamily::Stub => "in-process (test stub)",
    }
}

/// Return the session policy string for a given backend policy role,
/// matching the runtime behavior in the workflow engine.
fn session_policy_for_role(role: BackendPolicyRole) -> String {
    match role {
        // Panel work always uses NewSession
        BackendPolicyRole::Completer
        | BackendPolicyRole::FinalReviewer
        | BackendPolicyRole::PromptReviewer
        | BackendPolicyRole::PromptValidator
        | BackendPolicyRole::Arbiter => "new_session".to_owned(),
        // Main stage roles use ReuseIfAllowed
        BackendPolicyRole::Planner
        | BackendPolicyRole::Implementer
        | BackendPolicyRole::Reviewer
        | BackendPolicyRole::Qa
        | BackendPolicyRole::AcceptanceQa => "reuse_if_allowed".to_owned(),
    }
}

/// Given a flow definition, return the set of distinct `BackendPolicyRole`s
/// that the flow's stages will require.
fn roles_for_flow(flow_def: &FlowDefinition) -> Vec<BackendPolicyRole> {
    let mut roles = Vec::new();
    for stage_id in flow_def.stages {
        let role = stage_to_policy_role(*stage_id);
        if !roles.contains(&role) {
            roles.push(role);
        }
    }
    roles
}

/// Validate that a flow contains a given stage, returning a clear error if not.
fn validate_flow_has_stage(flow_def: &FlowDefinition, stage_id: StageId) -> AppResult<()> {
    if flow_def.stages.contains(&stage_id) {
        Ok(())
    } else {
        Err(AppError::InvalidConfigValue {
            key: "flow".to_owned(),
            value: flow_def.preset.as_str().to_owned(),
            reason: format!(
                "flow '{}' does not include stage '{}'",
                flow_def.preset.as_str(),
                stage_id.as_str()
            ),
        })
    }
}

/// Classify a panel resolution error with structural detail: returns the
/// failure kind, the specific backend family (not "mixed"), and a detail string.
fn classify_panel_error_structured(
    error: &AppError,
    configured_specs: &[PanelBackendSpec],
    minimum: usize,
) -> (BackendCheckFailureKind, String, String) {
    match error {
        AppError::BackendUnavailable { backend, details } => (
            BackendCheckFailureKind::RequiredMemberUnavailable,
            backend.clone(),
            format!("required backend '{backend}' unavailable: {details}"),
        ),
        AppError::InvalidConfigValue { reason, .. } if reason.contains("minimum") => {
            // Try to identify which backends were lost
            let disabled: Vec<String> = configured_specs
                .iter()
                .filter(|spec| !spec.is_optional())
                .map(|spec| spec.backend().as_str().to_owned())
                .collect();
            let family = if disabled.len() == 1 {
                disabled[0].clone()
            } else {
                "mixed".to_owned()
            };
            (
                BackendCheckFailureKind::PanelMinimumViolation,
                family,
                format!(
                    "resolved panel member count is below the required minimum of {minimum}"
                ),
            )
        }
        other => (
            BackendCheckFailureKind::RequiredMemberUnavailable,
            "mixed".to_owned(),
            other.to_string(),
        ),
    }
}

//! Read-only backend diagnostics service and stable DTOs for
//! `backend list`, `backend check`, `backend show-effective`, and `backend probe`.
//!
//! This module reuses the existing `BackendPolicyService`, `BackendResolver`,
//! and `EffectiveConfig` primitives instead of introducing parallel resolution
//! logic.

use std::fmt;

use serde::Serialize;

use crate::contexts::agent_execution::policy::BackendPolicyService;
use crate::contexts::workspace_governance::config::{
    EffectiveConfig, DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
};
use crate::shared::domain::{BackendFamily, BackendPolicyRole, FlowPreset, PanelBackendSpec};
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
}

impl fmt::Display for BackendCheckFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackendDisabled => f.write_str("backend_disabled"),
            Self::PanelMinimumViolation => f.write_str("panel_minimum_violation"),
            Self::RequiredMemberUnavailable => f.write_str("required_member_unavailable"),
        }
    }
}

/// Stable DTO for `backend show-effective`.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveBackendView {
    pub base_backend: EffectiveFieldView,
    pub default_model: EffectiveFieldView,
    pub roles: Vec<RoleEffectiveView>,
    pub session_policy: String,
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

    pub fn check_backends(&self, _flow: FlowPreset) -> BackendCheckResult {
        let mut failures = Vec::new();

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

        // Check all policy roles that are relevant
        for role in BackendPolicyRole::ALL {
            if let Err(error) = self.policy.resolve_role_target(role, 1) {
                failures.push(BackendCheckFailure {
                    role: role.as_str().to_owned(),
                    backend_family: self.family_for_role(role),
                    failure_kind: BackendCheckFailureKind::BackendDisabled,
                    details: error.to_string(),
                    config_source: self.config_source_for_role(role),
                });
            }
        }

        // Check completion panel
        self.check_panel_resolution(
            &mut failures,
            "completion_panel",
            || self.policy.resolve_completion_panel(1),
            self.config.completion_policy().min_completers,
        );

        // Check final review panel (only if enabled)
        if self.config.final_review_policy().enabled {
            self.check_final_review_panel(&mut failures);
        }

        // Check prompt review panel (only if enabled)
        if self.config.prompt_review_policy().enabled {
            self.check_prompt_review_panel(&mut failures);
        }

        BackendCheckResult {
            passed: failures.is_empty(),
            failures,
        }
    }

    fn check_panel_resolution<F, T>(
        &self,
        failures: &mut Vec<BackendCheckFailure>,
        panel_name: &str,
        resolve_fn: F,
        minimum: usize,
    ) where
        F: FnOnce() -> AppResult<T>,
    {
        if let Err(error) = resolve_fn() {
            let (kind, details) = classify_panel_error(&error, minimum);
            failures.push(BackendCheckFailure {
                role: panel_name.to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: kind,
                details,
                config_source: format!("{panel_name} config"),
            });
        }
    }

    fn check_final_review_panel(&self, failures: &mut Vec<BackendCheckFailure>) {
        if let Err(error) = self.policy.resolve_final_review_panel(1) {
            let (kind, details) = classify_panel_error(
                &error,
                self.config.final_review_policy().min_reviewers,
            );
            failures.push(BackendCheckFailure {
                role: "final_review_panel".to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: kind,
                details,
                config_source: "final_review config".to_owned(),
            });
        }
    }

    fn check_prompt_review_panel(&self, failures: &mut Vec<BackendCheckFailure>) {
        if let Err(error) = self.policy.resolve_prompt_review_panel(1) {
            let (kind, details) = classify_panel_error(
                &error,
                self.config.prompt_review_policy().min_reviewers,
            );
            failures.push(BackendCheckFailure {
                role: "prompt_review_panel".to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: kind,
                details,
                config_source: "prompt_review config".to_owned(),
            });
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
                Some(RoleEffectiveView {
                    role: role.as_str().to_owned(),
                    backend_family: target.backend.family.as_str().to_owned(),
                    model_id: target.model.model_id.clone(),
                    timeout_seconds: timeout.as_secs(),
                    override_source,
                })
            })
            .collect();

        EffectiveBackendView {
            base_backend,
            default_model,
            roles,
            session_policy: "reuse_if_allowed".to_owned(),
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

        // Check if this is a panel target
        match role_str {
            "completion_panel" => return self.probe_completion_panel(flow, cycle),
            "final_review_panel" => return self.probe_final_review_panel(flow, cycle),
            "prompt_review_panel" => return self.probe_prompt_review_panel(flow, cycle),
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
            }),
        })
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

        // Check for omitted optional members from configured specs
        for spec in configured_specs {
            if spec.is_optional() && !self.policy.backend_enabled_public(spec.backend()) {
                omitted.push(PanelOmittedView {
                    backend_family: spec.backend().as_str().to_owned(),
                    reason: "backend disabled or unavailable".to_owned(),
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
            _ => return "default".to_owned(),
        };

        match self.config.get(key) {
            Ok(entry) => entry.source.to_string(),
            Err(_) => "default".to_owned(),
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

fn classify_panel_error(
    error: &AppError,
    minimum: usize,
) -> (BackendCheckFailureKind, String) {
    match error {
        AppError::BackendUnavailable { backend, details } => (
            BackendCheckFailureKind::RequiredMemberUnavailable,
            format!("required backend '{backend}' unavailable: {details}"),
        ),
        AppError::InvalidConfigValue { reason, .. } if reason.contains("minimum") => (
            BackendCheckFailureKind::PanelMinimumViolation,
            format!(
                "resolved panel member count is below the required minimum of {minimum}"
            ),
        ),
        other => (
            BackendCheckFailureKind::RequiredMemberUnavailable,
            other.to_string(),
        ),
    }
}

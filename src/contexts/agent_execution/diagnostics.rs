//! Read-only backend diagnostics service and stable DTOs for
//! `backend list`, `backend check`, `backend show-effective`, and `backend probe`.
//!
//! This module reuses the existing `BackendPolicyService`, `BackendResolver`,
//! and `EffectiveConfig` primitives instead of introducing parallel resolution
//! logic.

use std::fmt;

use serde::Serialize;

use crate::adapters::tmux::TmuxAdapter;
use crate::contexts::agent_execution::policy::{stage_to_policy_role, BackendPolicyService};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::workflow_composition::{flow_definition, FlowDefinition};
use crate::contexts::workspace_governance::config::{
    EffectiveConfig, DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
};
use crate::shared::domain::{
    BackendFamily, BackendPolicyRole, ExecutionMode, FlowPreset, PanelBackendSpec,
    ResolvedBackendTarget, StageId,
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
    TmuxUnavailable,
}

impl fmt::Display for BackendCheckFailureKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackendDisabled => f.write_str("backend_disabled"),
            Self::PanelMinimumViolation => f.write_str("panel_minimum_violation"),
            Self::RequiredMemberUnavailable => f.write_str("required_member_unavailable"),
            Self::AvailabilityFailure => f.write_str("availability_failure"),
            Self::TmuxUnavailable => f.write_str("tmux_unavailable"),
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
    pub model_source: String,
    pub timeout_source: String,
    /// When present, indicates this role's configured backend could not resolve.
    /// The `backend_family` and `override_source` still reflect what was
    /// configured, so operators can see the broken selection and its source.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_error: Option<String>,
}

/// Result of `backend probe`.
#[derive(Debug, Clone, Serialize)]
pub struct BackendProbeResult {
    pub role: String,
    pub flow: String,
    pub cycle: u32,
    /// Primary target for single-role probes (planner, implementer, reviewer, etc.).
    /// `None` for panel probes — use `panel` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<ProbeTargetView>,
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
    /// The index of this member in the original configured spec list.
    /// Preserved through optional-member filtering so failure messages
    /// always reference the exact configured position.
    pub configured_index: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PanelOmittedView {
    pub backend_family: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    pub reason: String,
    pub was_optional: bool,
}

// ── Diagnostics Service ─────────────────────────────────────────────────────

/// Read-only diagnostics service that wraps `EffectiveConfig` and
/// `BackendPolicyService` to produce stable DTOs for CLI rendering.
pub struct BackendDiagnosticsService<'a> {
    config: &'a EffectiveConfig,
    policy: BackendPolicyService<'a>,
    tmux_search_paths: Option<Vec<std::path::PathBuf>>,
}

impl<'a> BackendDiagnosticsService<'a> {
    pub fn new(config: &'a EffectiveConfig) -> Self {
        Self {
            config,
            policy: BackendPolicyService::new(config),
            tmux_search_paths: None,
        }
    }

    /// Override the search paths used for the tmux availability check.
    pub fn with_tmux_search_paths(mut self, paths: Vec<std::path::PathBuf>) -> Self {
        self.tmux_search_paths = Some(paths);
        self
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
                    // Only mark stub as compile-time-only when the current
                    // binary was built without stub support (no test-stub feature).
                    #[cfg(feature = "test-stub")]
                    {
                        None
                    }
                    #[cfg(not(feature = "test-stub"))]
                    {
                        Some(true)
                    }
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

        if self.config.effective_execution_mode() == ExecutionMode::Tmux {
            let tmux_result = if let Some(ref paths) = self.tmux_search_paths {
                TmuxAdapter::check_tmux_available_in(paths)
            } else {
                TmuxAdapter::check_tmux_available()
            };
            if let Err(error) = tmux_result {
                let source = self
                    .config
                    .get("execution.mode")
                    .map(|entry| entry.source.to_string())
                    .unwrap_or_else(|_| "execution.mode".to_owned());
                failures.push(BackendCheckFailure {
                    role: "execution".to_owned(),
                    backend_family: "tmux".to_owned(),
                    failure_kind: BackendCheckFailureKind::TmuxUnavailable,
                    details: error.to_string(),
                    config_source: source,
                });
            }
        }

        // Get stage-derived roles, excluding panel-specific ones (Completer,
        // FinalReviewer) which are validated by the dedicated panel checks below.
        let flow_roles = effectively_required_stage_roles(flow_def);

        // Only check base backend if at least one effectively-required stage
        // role would actually fall through to it (no explicit override).
        let base_backend_needed = flow_roles
            .iter()
            .any(|role| !self.policy.has_explicit_override(*role));
        if base_backend_needed {
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
        }

        // Check only effectively-required stage roles (panel-specific roles
        // are handled below by dedicated panel checks).
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
            self.check_completion_panel_config(&mut failures);
        }

        // Check final review panel if the flow includes it.
        // Note: the engine's stage_plan_for_flow() does NOT filter FinalReview
        // based on final_review.enabled — it always includes FinalReview when
        // the flow definition contains it. Diagnostics must match this behavior
        // so `backend check` validates what the engine will actually execute.
        if flow_def.stages.contains(&StageId::FinalReview) {
            self.check_final_review_panel_config(&mut failures);
        }

        // Check prompt review panel only if the flow includes it and it's enabled
        if flow_def.stages.contains(&StageId::PromptReview)
            && self.config.prompt_review_policy().enabled
        {
            self.check_prompt_review_panel_config(&mut failures);
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
    ///
    /// Optional panel members that fail availability are omitted from failures
    /// unless their omission drops the panel below its configured minimum,
    /// in which case a `PanelMinimumViolation` is reported.
    pub async fn check_backends_with_availability<A: AgentExecutionPort>(
        &self,
        flow: FlowPreset,
        adapter: &A,
    ) -> BackendCheckResult {
        let mut result = self.check_backends(flow);
        if result
            .failures
            .iter()
            .any(|failure| failure.failure_kind == BackendCheckFailureKind::TmuxUnavailable)
        {
            result.passed = false;
            return result;
        }

        // Required targets: stage roles plus singleton panel targets.
        // These always fail on unavailability.
        let mut required_targets: Vec<(ResolvedBackendTarget, String, String)> = Vec::new();
        let flow_def = flow_definition(flow);
        let flow_roles = effectively_required_stage_roles(flow_def);

        // Stage-role targets (always required)
        for role in &flow_roles {
            if let Ok(target) = self.policy.resolve_role_target(*role, 1) {
                required_targets.push((
                    target,
                    role.as_str().to_owned(),
                    self.config_source_for_role(*role),
                ));
            }
        }

        // Panel-specific targets: completion panel members (required vs optional)
        if flow_def.stages.contains(&StageId::CompletionPanel) {
            if let Ok(res) = self.policy.resolve_completion_panel(1) {
                let minimum = self.config.completion_policy().min_completers;
                let completion_source = self.completion_panel_config_source();
                self.check_panel_availability(
                    adapter,
                    &res.completers
                        .iter()
                        .map(|m| (m.target.clone(), m.required, m.configured_index))
                        .collect::<Vec<_>>(),
                    "completion_panel",
                    &completion_source,
                    minimum,
                    &mut result,
                )
                .await;
            }
        }

        // Panel-specific targets: final review panel (arbiter required,
        // reviewers may be optional).
        // Arbiter is resolved independently so its availability is always
        // checked even when reviewer resolution fails.
        // Note: aligned with engine stage_plan_for_flow() which does NOT filter
        // FinalReview based on final_review.enabled.
        if flow_def.stages.contains(&StageId::FinalReview) {
            // Arbiter — always required, resolved independently of panel
            if let Ok(arbiter_target) = self
                .policy
                .resolve_role_target(BackendPolicyRole::Arbiter, 1)
            {
                required_targets.push((
                    arbiter_target,
                    "final_review_panel.arbiter".to_owned(),
                    self.config_source_for_role(BackendPolicyRole::Arbiter),
                ));
            }

            // Full panel resolution for reviewers
            if let Ok(res) = self.policy.resolve_final_review_panel(1) {
                let minimum = self.config.final_review_policy().min_reviewers;
                self.check_panel_availability(
                    adapter,
                    &res.reviewers
                        .iter()
                        .map(|m| (m.target.clone(), m.required, m.configured_index))
                        .collect::<Vec<_>>(),
                    "final_review_panel",
                    "final_review.backends",
                    minimum,
                    &mut result,
                )
                .await;
            }
        }

        // Panel-specific targets: prompt review panel (refiner required,
        // validators may be optional).
        // Refiner is resolved independently so its availability is always
        // checked even when validator resolution fails.
        if flow_def.stages.contains(&StageId::PromptReview)
            && self.config.prompt_review_policy().enabled
        {
            // Refiner — always required, resolved independently of panel
            if let Ok(refiner_target) = self
                .policy
                .resolve_role_target(BackendPolicyRole::PromptReviewer, 1)
            {
                required_targets.push((
                    refiner_target,
                    "prompt_review_panel.refiner".to_owned(),
                    self.config_source_for_role(BackendPolicyRole::PromptReviewer),
                ));
            }

            // Full panel resolution for validators
            if let Ok(res) = self.policy.resolve_prompt_review_panel(1) {
                let minimum = self.config.prompt_review_policy().min_reviewers;
                self.check_panel_availability(
                    adapter,
                    &res.validators
                        .iter()
                        .map(|m| (m.target.clone(), m.required, m.configured_index))
                        .collect::<Vec<_>>(),
                    "prompt_review_panel",
                    "prompt_review.validator_backends",
                    minimum,
                    &mut result,
                )
                .await;
            }
        }

        // Check all required targets — any failure is blocking
        for (target, role, config_source) in &required_targets {
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

    /// Check panel member availability with proper required/optional semantics.
    ///
    /// Required members that fail availability → `AvailabilityFailure`.
    /// Optional members that fail availability → omitted silently.
    /// If the remaining available count drops below `minimum` → `PanelMinimumViolation`.
    ///
    /// Each member tuple carries `(target, required, configured_index)` where
    /// `configured_index` is the member's position in the original configured
    /// spec list, preserved through optional-member filtering.
    async fn check_panel_availability<A: AgentExecutionPort>(
        &self,
        adapter: &A,
        members: &[(ResolvedBackendTarget, bool, usize)],
        panel_name: &str,
        config_source: &str,
        minimum: usize,
        result: &mut BackendCheckResult,
    ) {
        let mut available_count = 0;

        for (target, required, configured_index) in members {
            match adapter.check_availability(target).await {
                Ok(()) => {
                    available_count += 1;
                }
                Err(error) => {
                    if *required {
                        let member_role = format!("{}.member[{}]", panel_name, configured_index);
                        result.failures.push(BackendCheckFailure {
                            role: member_role,
                            backend_family: target.backend.family.as_str().to_owned(),
                            failure_kind: BackendCheckFailureKind::AvailabilityFailure,
                            details: error.to_string(),
                            config_source: config_source.to_owned(),
                        });
                    }
                    // Optional unavailable members are silently omitted
                }
            }
        }

        // Check if remaining available members satisfy the minimum
        if available_count < minimum {
            result.failures.push(BackendCheckFailure {
                role: panel_name.to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: BackendCheckFailureKind::PanelMinimumViolation,
                details: format!(
                    "only {} member(s) available but minimum is {}",
                    available_count, minimum
                ),
                config_source: config_source.to_owned(),
            });
        }
    }

    /// Check completion panel members individually, reporting exact member
    /// identity and config source field for each failure.
    ///
    /// When `completion.backends` is not explicitly configured, this uses the
    /// same implicit resolution path as `BackendPolicyService::resolve_completion_panel()`:
    /// `default_completion_targets()` resolves the Completer role target instead
    /// of iterating the built-in default backend list. This prevents false
    /// failures when the default list contains backends that runtime would never
    /// use.
    fn check_completion_panel_config(&self, failures: &mut Vec<BackendCheckFailure>) {
        if !self.config.completion_backends_are_explicit() {
            // Implicit completion backends: runtime uses default_completion_targets(),
            // which resolves the Completer role and replicates it min_completers times.
            // Validate that the Completer role can actually resolve.
            if let Err(e) = self
                .policy
                .resolve_role_target(BackendPolicyRole::Completer, 1)
            {
                failures.push(BackendCheckFailure {
                    role: "completion_panel".to_owned(),
                    backend_family: self.family_for_role(BackendPolicyRole::Completer),
                    failure_kind: BackendCheckFailureKind::BackendDisabled,
                    details: e.to_string(),
                    config_source: self.config_source_for_role(BackendPolicyRole::Completer),
                });
            }
            return;
        }

        let specs = &self.config.completion_policy().backends;
        let minimum = self.config.completion_policy().min_completers;
        let mut resolved_count = 0;

        for (idx, spec) in specs.iter().enumerate() {
            let backend = spec.backend();
            if !self.policy.backend_enabled_public(backend) {
                if spec.is_optional() {
                    continue;
                }
                failures.push(BackendCheckFailure {
                    role: format!("completion_panel.member[{}]", idx),
                    backend_family: backend.as_str().to_owned(),
                    failure_kind: BackendCheckFailureKind::RequiredMemberUnavailable,
                    details: format!("required backend '{}' is disabled", backend),
                    config_source: "completion.backends".to_owned(),
                });
                continue;
            }
            resolved_count += 1;
        }

        if resolved_count < minimum {
            failures.push(BackendCheckFailure {
                role: "completion_panel".to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: BackendCheckFailureKind::PanelMinimumViolation,
                details: format!(
                    "resolved {} member(s) but minimum is {}",
                    resolved_count, minimum
                ),
                config_source: "completion.backends".to_owned(),
            });
        }
    }

    /// Check final review panel members individually: each reviewer and the
    /// arbiter each get their own failure identity and config source.
    fn check_final_review_panel_config(&self, failures: &mut Vec<BackendCheckFailure>) {
        let policy = self.config.final_review_policy();

        // Check arbiter separately — this was previously collapsed into the panel
        if let Err(e) = self
            .policy
            .resolve_role_target(BackendPolicyRole::Arbiter, 1)
        {
            failures.push(BackendCheckFailure {
                role: "final_review_panel.arbiter".to_owned(),
                backend_family: self.family_for_role(BackendPolicyRole::Arbiter),
                failure_kind: BackendCheckFailureKind::RequiredMemberUnavailable,
                details: e.to_string(),
                config_source: self.config_source_for_role(BackendPolicyRole::Arbiter),
            });
        }

        // Check each reviewer spec individually
        let specs = &policy.backends;
        let minimum = policy.min_reviewers;
        let mut resolved_count = 0;

        for (idx, spec) in specs.iter().enumerate() {
            let backend = spec.backend();
            if !self.policy.backend_enabled_public(backend) {
                if spec.is_optional() {
                    continue;
                }
                failures.push(BackendCheckFailure {
                    role: format!("final_review_panel.reviewer[{}]", idx),
                    backend_family: backend.as_str().to_owned(),
                    failure_kind: BackendCheckFailureKind::RequiredMemberUnavailable,
                    details: format!("required backend '{}' is disabled", backend),
                    config_source: "final_review.backends".to_owned(),
                });
                continue;
            }
            resolved_count += 1;
        }

        if resolved_count < minimum {
            failures.push(BackendCheckFailure {
                role: "final_review_panel".to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: BackendCheckFailureKind::PanelMinimumViolation,
                details: format!(
                    "resolved {} reviewer(s) but minimum is {}",
                    resolved_count, minimum
                ),
                config_source: "final_review.backends".to_owned(),
            });
        }
    }

    /// Check prompt review panel members individually: refiner and each
    /// validator get their own failure identity and config source.
    fn check_prompt_review_panel_config(&self, failures: &mut Vec<BackendCheckFailure>) {
        let policy = self.config.prompt_review_policy();

        // Check refiner separately — this was previously collapsed into the panel
        if let Err(e) = self
            .policy
            .resolve_role_target(BackendPolicyRole::PromptReviewer, 1)
        {
            failures.push(BackendCheckFailure {
                role: "prompt_review_panel.refiner".to_owned(),
                backend_family: self.family_for_role(BackendPolicyRole::PromptReviewer),
                failure_kind: BackendCheckFailureKind::RequiredMemberUnavailable,
                details: e.to_string(),
                config_source: self.config_source_for_role(BackendPolicyRole::PromptReviewer),
            });
        }

        // Check each validator spec individually
        let specs = &policy.validator_backends;
        let minimum = policy.min_reviewers;
        let mut resolved_count = 0;

        for (idx, spec) in specs.iter().enumerate() {
            let backend = spec.backend();
            if !self.policy.backend_enabled_public(backend) {
                if spec.is_optional() {
                    continue;
                }
                failures.push(BackendCheckFailure {
                    role: format!("prompt_review_panel.validator[{}]", idx),
                    backend_family: backend.as_str().to_owned(),
                    failure_kind: BackendCheckFailureKind::RequiredMemberUnavailable,
                    details: format!("required backend '{}' is disabled", backend),
                    config_source: "prompt_review.validator_backends".to_owned(),
                });
                continue;
            }
            resolved_count += 1;
        }

        if resolved_count < minimum {
            failures.push(BackendCheckFailure {
                role: "prompt_review_panel".to_owned(),
                backend_family: "mixed".to_owned(),
                failure_kind: BackendCheckFailureKind::PanelMinimumViolation,
                details: format!(
                    "resolved {} validator(s) but minimum is {}",
                    resolved_count, minimum
                ),
                config_source: "prompt_review.validator_backends".to_owned(),
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

        // Effective default model follows the same resolution priority as
        // target_for_family(): base_backend.model (set from either
        // default_backend="family(model)" or merged from default_model)
        // takes precedence over the compile-time family default.
        //
        // Distinguish models embedded in default_backend from those merged
        // from settings.default_model: if bp.default_model matches
        // base_backend.model, the model came from settings.default_model
        // and should report that source; otherwise it was embedded in
        // default_backend.
        let default_model = if let Some(ref model) = bp.base_backend.model {
            let source = if bp.default_model.as_ref() == Some(model) {
                self.source_for_default_model()
            } else {
                self.source_for_base_backend()
            };
            EffectiveFieldView {
                value: model.clone(),
                source,
            }
        } else {
            EffectiveFieldView {
                value: bp.base_backend.family.default_model_id().to_owned(),
                source: "default".to_owned(),
            }
        };

        let roles = BackendPolicyRole::ALL
            .iter()
            .flat_map(|role| match role {
                BackendPolicyRole::FinalReviewer => self.final_review_reviewer_effective_views(),
                _ => vec![self.role_effective_view(*role)],
            })
            .collect::<Vec<_>>();

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

        // Check if this is a panel target.
        // Panel probes handle their own error wrapping internally for exact
        // per-component identity, so no blanket wrap_probe_config_error here.
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
            "final_reviewer" => {
                validate_flow_has_stage(flow_def, StageId::FinalReview)?;
                return self.probe_final_reviewer(flow, cycle);
            }
            _ => {}
        }

        // Parse as a policy role
        let role: BackendPolicyRole = role_str.parse()?;
        let target = self
            .policy
            .resolve_role_target(role, cycle)
            .map_err(|err| {
                self.make_probe_target_error(
                    role_str,
                    role_str,
                    &self.family_for_role(role),
                    &self.config_source_for_role(role),
                    &err,
                )
            })?;
        let timeout = self.policy.timeout_for_role(target.backend.family, role);

        Ok(BackendProbeResult {
            role: role.as_str().to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: Some(ProbeTargetView {
                backend_family: target.backend.family.as_str().to_owned(),
                model_id: target.model.model_id,
                timeout_seconds: timeout.as_secs(),
            }),
            panel: None,
        })
    }

    fn probe_completion_panel(
        &self,
        flow: FlowPreset,
        cycle: u32,
    ) -> AppResult<BackendProbeResult> {
        // Panel probes use target: None — members are in the panel view.

        let configured_specs = &self.config.completion_policy().backends;
        let minimum = self.config.completion_policy().min_completers;
        let completion_source = self.completion_panel_config_source();

        // Try the full panel resolution; if it fails, identify the exact
        // failing member directly.
        let resolution = self.policy.resolve_completion_panel(cycle).map_err(|err| {
            self.identify_failing_panel_member(
                "completion_panel",
                "member",
                &completion_source,
                configured_specs,
                minimum,
                &err,
            )
        })?;

        let (members, omitted) = self.build_panel_member_views(
            &resolution
                .completers
                .iter()
                .map(|m| {
                    (
                        m.target.backend.family,
                        m.target.model.model_id.clone(),
                        m.required,
                        m.configured_index,
                    )
                })
                .collect::<Vec<_>>(),
            configured_specs,
        );

        Ok(BackendProbeResult {
            role: "completion_panel".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: None,
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
        // Resolve arbiter separately for exact error identity
        let arbiter_target = self
            .policy
            .resolve_role_target(BackendPolicyRole::Arbiter, cycle)
            .map_err(|err| {
                self.make_probe_target_error(
                    "final_review_panel",
                    "arbiter",
                    &self.family_for_role(BackendPolicyRole::Arbiter),
                    &self.config_source_for_role(BackendPolicyRole::Arbiter),
                    &err,
                )
            })?;

        let configured_specs = &self.config.final_review_policy().backends;
        let minimum = self.config.final_review_policy().min_reviewers;

        // Try the full panel resolution; if it fails, identify the exact
        // failing reviewer (arbiter already succeeded above).
        let resolution = self
            .policy
            .resolve_final_review_panel(cycle)
            .map_err(|err| {
                self.identify_failing_panel_member(
                    "final_review_panel",
                    "reviewer",
                    "final_review.backends",
                    configured_specs,
                    minimum,
                    &err,
                )
            })?;

        let (members, omitted) = self.build_panel_member_views(
            &resolution
                .reviewers
                .iter()
                .map(|m| {
                    (
                        m.target.backend.family,
                        m.target.model.model_id.clone(),
                        m.required,
                        m.configured_index,
                    )
                })
                .collect::<Vec<_>>(),
            configured_specs,
        );

        let arbiter = PanelMemberView {
            backend_family: arbiter_target.backend.family.as_str().to_owned(),
            model_id: arbiter_target.model.model_id.clone(),
            required: true,
            configured_index: 0, // arbiter is a single component, not indexed
        };

        Ok(BackendProbeResult {
            role: "final_review_panel".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: None,
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

    fn probe_final_reviewer(&self, flow: FlowPreset, cycle: u32) -> AppResult<BackendProbeResult> {
        let configured_specs = &self.config.final_review_policy().backends;
        let selection = self
            .primary_final_review_reviewer_configured_index()
            .and_then(|idx| configured_specs.get(idx))
            .ok_or_else(|| AppError::InvalidConfigValue {
                key: "final_review.backends".to_owned(),
                value: "[]".to_owned(),
                reason: "must configure at least one final-review reviewer".to_owned(),
            })?;
        let selection = selection.selection();
        let target = self
            .policy
            .resolve_selection_target(BackendPolicyRole::FinalReviewer, selection)
            .map_err(|err| {
                self.make_probe_target_error(
                    "final_reviewer",
                    "reviewer[0]",
                    selection.family.as_str(),
                    "final_review.backends",
                    &err,
                )
            })?;
        let timeout = self
            .policy
            .timeout_for_role(target.backend.family, BackendPolicyRole::FinalReviewer);

        Ok(BackendProbeResult {
            role: "final_reviewer".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: Some(ProbeTargetView {
                backend_family: target.backend.family.as_str().to_owned(),
                model_id: target.model.model_id,
                timeout_seconds: timeout.as_secs(),
            }),
            panel: None,
        })
    }

    fn probe_prompt_review_panel(
        &self,
        flow: FlowPreset,
        cycle: u32,
    ) -> AppResult<BackendProbeResult> {
        // Resolve refiner separately for exact error identity
        self.policy
            .resolve_role_target(BackendPolicyRole::PromptReviewer, cycle)
            .map_err(|err| {
                self.make_probe_target_error(
                    "prompt_review_panel",
                    "refiner",
                    &self.family_for_role(BackendPolicyRole::PromptReviewer),
                    &self.config_source_for_role(BackendPolicyRole::PromptReviewer),
                    &err,
                )
            })?;

        let configured_specs = &self.config.prompt_review_policy().validator_backends;
        let minimum = self.config.prompt_review_policy().min_reviewers;

        // Try the full panel resolution; if it fails, identify the exact
        // failing validator (refiner already succeeded above).
        let resolution = self
            .policy
            .resolve_prompt_review_panel(cycle)
            .map_err(|err| {
                self.identify_failing_panel_member(
                    "prompt_review_panel",
                    "validator",
                    "prompt_review.validator_backends",
                    configured_specs,
                    minimum,
                    &err,
                )
            })?;

        let (members, omitted) = self.build_panel_member_views(
            &resolution
                .validators
                .iter()
                .map(|m| {
                    (
                        m.target.backend.family,
                        m.target.model.model_id.clone(),
                        m.required,
                        m.configured_index,
                    )
                })
                .collect::<Vec<_>>(),
            configured_specs,
        );

        Ok(BackendProbeResult {
            role: "prompt_review_panel".to_owned(),
            flow: flow.as_str().to_owned(),
            cycle,
            target: None,
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
    /// Required unavailable members and singleton panel targets (arbiter,
    /// refiner) cause the probe to fail with exact member identity, backend
    /// family, and effective config source field.
    pub async fn probe_with_availability<A: AgentExecutionPort>(
        &self,
        role_str: &str,
        flow: FlowPreset,
        cycle: u32,
        adapter: &A,
    ) -> AppResult<BackendProbeResult> {
        // Config-time probe failures are already wrapped with target identity
        // and source by `probe()` itself via `wrap_probe_config_error`.
        let mut result = self.probe(role_str, flow, cycle)?;

        // Resolve config source for the primary target role
        let primary_source = self.probe_primary_config_source(role_str);
        let primary_target = self.probe_primary_target_label(role_str);

        // Check the primary target (only for single-role probes; panels
        // check their members individually below).
        if let Some(ref target_view) = result.target {
            let family: BackendFamily = target_view.backend_family.parse()?;
            let target = ResolvedBackendTarget::new(family, target_view.model_id.clone());
            if let Err(err) = adapter.check_availability(&target).await {
                return Err(AppError::BackendUnavailable {
                    backend: family.as_str().to_owned(),
                    details: format!(
                        "required target '{}' ({}) unavailable: {} [source: {}]",
                        role_str, primary_target, err, primary_source
                    ),
                    failure_class: None,
                });
            }
        }

        if let Some(panel) = &mut result.panel {
            // Prompt-review still has a singleton refiner target in addition
            // to validator members, so availability is checked separately.
            if panel.panel_type == "prompt_review" {
                let refiner = self
                    .policy
                    .resolve_role_target(BackendPolicyRole::PromptReviewer, result.cycle);
                if let Ok(refiner) = refiner {
                    let refiner_source =
                        self.config_source_for_role(BackendPolicyRole::PromptReviewer);
                    if let Err(err) = adapter.check_availability(&refiner).await {
                        return Err(AppError::BackendUnavailable {
                            backend: refiner.backend.family.as_str().to_owned(),
                            details: format!(
                                "required primary '{}' for '{}' unavailable: {} [source: {}]",
                                "refiner", role_str, err, refiner_source
                            ),
                            failure_class: None,
                        });
                    }
                }
            }

            // Final-review still has a singleton arbiter target in addition to
            // reviewer members, so availability is checked separately.
            if let Some(arbiter) = &panel.arbiter {
                let arbiter_source = self.config_source_for_role(BackendPolicyRole::Arbiter);
                let family: BackendFamily = arbiter.backend_family.parse()?;
                let target = ResolvedBackendTarget::new(family, arbiter.model_id.clone());
                if let Err(err) = adapter.check_availability(&target).await {
                    return Err(AppError::BackendUnavailable {
                        backend: family.as_str().to_owned(),
                        details: format!(
                            "required arbiter for '{}' unavailable: {} [source: {}]",
                            role_str, err, arbiter_source
                        ),
                        failure_class: None,
                    });
                }
            }

            let members_source = self.panel_members_config_source(&panel.panel_type);

            // Check panel members: required unavailable -> fail; optional
            // unavailable -> omit.
            let member_label_prefix = Self::panel_member_label_prefix(&panel.panel_type);
            let mut available_members = Vec::new();
            for member in panel.members.drain(..) {
                let family: BackendFamily = member.backend_family.parse()?;
                let target = ResolvedBackendTarget::new(family, member.model_id.clone());
                match adapter.check_availability(&target).await {
                    Ok(()) => available_members.push(member),
                    Err(_) if !member.required => {
                        panel.omitted.push(PanelOmittedView {
                            backend_family: member.backend_family,
                            model_id: Some(member.model_id),
                            reason: "backend unavailable".to_owned(),
                            was_optional: true,
                        });
                    }
                    Err(err) => {
                        return Err(AppError::BackendUnavailable {
                            backend: member.backend_family.clone(),
                            details: format!(
                                "required '{}[{}]' ({}:{}) unavailable: {} [source: {}]",
                                member_label_prefix,
                                member.configured_index,
                                member.backend_family,
                                member.model_id,
                                err,
                                members_source
                            ),
                            failure_class: None,
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

    /// Build a probe error with exact target identity, backend family, and
    /// selecting config source field for a specific component (arbiter,
    /// refiner, or singular role).
    fn make_probe_target_error(
        &self,
        panel: &str,
        target_label: &str,
        family: &str,
        config_source: &str,
        inner: &AppError,
    ) -> AppError {
        AppError::BackendUnavailable {
            backend: family.to_owned(),
            details: format!(
                "required target '{panel}' ({target_label}) unavailable: {inner} [source: {config_source}]",
            ),
            failure_class: None,
        }
    }

    /// Identify which panel member spec failed resolution, and build an error
    /// with exact `panel.member_label[N]` identity and the panel's config
    /// source field. Used when the panel's singleton target (refiner/arbiter)
    /// succeeded but `resolve_*_panel()` still failed.
    ///
    /// When the failure is caused by optional-member omission dropping the
    /// panel below its configured minimum, returns `InsufficientPanelMembers`
    /// instead of a generic `BackendUnavailable` fallback.
    fn identify_failing_panel_member(
        &self,
        panel: &str,
        member_label: &str,
        config_source: &str,
        specs: &[PanelBackendSpec],
        minimum: usize,
        inner: &AppError,
    ) -> AppError {
        // Scan the specs to find the first required disabled backend — that
        // is the member whose failure caused the panel resolution to error.
        for (idx, spec) in specs.iter().enumerate() {
            let backend = spec.backend();
            if !spec.is_optional() && !self.policy.backend_enabled_public(backend) {
                return AppError::BackendUnavailable {
                    backend: backend.as_str().to_owned(),
                    details: format!(
                        "required target '{panel}.{member_label}[{idx}]' ({backend}) unavailable: \
                         backend disabled [source: {config_source}]",
                    ),
                    failure_class: None,
                };
            }
        }

        // Check if optional-member omission caused the panel to drop below
        // its configured minimum. Count how many specs have enabled backends.
        let enabled_count = specs
            .iter()
            .filter(|spec| self.policy.backend_enabled_public(spec.backend()))
            .count();
        if enabled_count < minimum {
            return AppError::InsufficientPanelMembers {
                panel: panel.to_owned(),
                resolved: enabled_count,
                minimum,
            };
        }

        // Fallback: cannot pinpoint the member — wrap with panel-level identity
        AppError::BackendUnavailable {
            backend: "unknown".to_owned(),
            details: format!(
                "required target '{panel}' ({member_label}) unavailable: {inner} [source: {config_source}]",
            ),
            failure_class: None,
        }
    }

    /// Return a human-readable label for a probe target, identifying the exact
    /// role/member used in availability reporting.
    fn probe_primary_target_label<'b>(&self, role_str: &'b str) -> &'b str {
        match role_str {
            "final_review_panel" => "arbiter",
            "final_reviewer" => "reviewer[0]",
            "prompt_review_panel" => "refiner",
            _ => role_str,
        }
    }

    /// Resolve the effective config source field for a probe target, based on
    /// the role string.
    fn probe_primary_config_source(&self, role_str: &str) -> String {
        match role_str {
            "final_review_panel" => self.config_source_for_role(BackendPolicyRole::Arbiter),
            "final_reviewer" => "final_review.backends".to_owned(),
            "prompt_review_panel" => self.config_source_for_role(BackendPolicyRole::PromptReviewer),
            // Singular role probe — parse to get the actual role's config source
            _ => match role_str.parse::<BackendPolicyRole>() {
                Ok(role) => self.config_source_for_role(role),
                Err(_) => "default_backend".to_owned(),
            },
        }
    }

    /// Return the qualified member label prefix for probe failure messages.
    fn panel_member_label_prefix(panel_type: &str) -> &'static str {
        match panel_type {
            "completion" => "completion_panel.member",
            "final_review" => "final_review_panel.reviewer",
            "prompt_review" => "prompt_review_panel.validator",
            _ => "panel.member",
        }
    }

    /// Return the config field that governs panel member selection.
    /// For completion panels, uses the actual source when backends are implicit.
    fn panel_members_config_source(&self, panel_type: &str) -> String {
        match panel_type {
            "completion" => self.completion_panel_config_source(),
            "final_review" => "final_review.backends".to_owned(),
            "prompt_review" => "prompt_review.validator_backends".to_owned(),
            _ => "default_backend".to_owned(),
        }
    }

    /// Return the effective config source for completion panel members.
    /// When `completion.backends` is explicitly configured, returns
    /// `"completion.backends"`. When implicit, returns the config source
    /// for the Completer role (matching runtime's `default_completion_targets()`).
    fn completion_panel_config_source(&self) -> String {
        if self.config.completion_backends_are_explicit() {
            "completion.backends".to_owned()
        } else {
            self.config_source_for_role(BackendPolicyRole::Completer)
        }
    }

    fn build_panel_member_views(
        &self,
        resolved: &[(BackendFamily, String, bool, usize)],
        configured_specs: &[PanelBackendSpec],
    ) -> (Vec<PanelMemberView>, Vec<PanelOmittedView>) {
        let mut members = Vec::new();
        let mut omitted = Vec::new();

        for (family, model_id, required, configured_index) in resolved {
            members.push(PanelMemberView {
                backend_family: family.as_str().to_owned(),
                model_id: model_id.clone(),
                required: *required,
                configured_index: *configured_index,
            });
        }

        // Check for omitted optional members from configured specs.
        // A member is omitted if its backend is disabled (not enabled in config).
        for spec in configured_specs {
            let backend = spec.backend();
            let already_resolved = resolved.iter().any(|(f, _, _, _)| *f == backend);
            if spec.is_optional()
                && !already_resolved
                && !self.policy.backend_enabled_public(backend)
            {
                omitted.push(PanelOmittedView {
                    backend_family: backend.as_str().to_owned(),
                    model_id: spec.selection().model.clone(),
                    reason: "backend disabled".to_owned(),
                    was_optional: true,
                });
            }
        }

        (members, omitted)
    }

    fn primary_final_review_reviewer_configured_index(&self) -> Option<usize> {
        self.config
            .final_review_policy()
            .backends
            .iter()
            .enumerate()
            .find_map(|(idx, spec)| {
                let backend = spec.backend();
                if spec.is_optional() && !self.policy.backend_enabled_public(backend) {
                    None
                } else {
                    Some(idx)
                }
            })
    }

    // ── Helper methods ──────────────────────────────────────────────────

    /// Return the backend family that runtime resolution would attempt for
    /// a given role (assuming cycle 1). This mirrors the resolution path in
    /// `BackendPolicyService::resolve_role_target()`:
    /// - Roles with explicit overrides → override family
    /// - Planner-family roles (Planner, Implementer, PromptReviewer, etc.) → base_backend.family
    /// - Opposite-family roles (Reviewer, Qa, AcceptanceQa, Completer) → opposite_family(base)
    fn family_for_role(&self, role: BackendPolicyRole) -> String {
        let bp = self.config.backend_policy();

        // Check explicit override first
        let explicit = match role {
            BackendPolicyRole::Planner => bp.planner_backend.as_ref(),
            BackendPolicyRole::Implementer => bp.implementer_backend.as_ref(),
            BackendPolicyRole::Reviewer => bp.reviewer_backend.as_ref(),
            BackendPolicyRole::Qa | BackendPolicyRole::AcceptanceQa => bp.qa_backend.as_ref(),
            BackendPolicyRole::PromptReviewer => bp.prompt_review_refiner_backend.as_ref(),
            BackendPolicyRole::Arbiter => bp.final_review_arbiter_backend.as_ref(),
            _ => None,
        };
        if let Some(sel) = explicit {
            return sel.family.as_str().to_owned();
        }

        // No explicit override — mirror runtime resolution path (cycle 1).
        // Opposite-family roles use opposite_family(planner_family).
        let planner_family = bp.base_backend.family;
        if Self::role_uses_opposite_family(role) {
            match self.policy.opposite_family(planner_family) {
                Ok(family) => family.as_str().to_owned(),
                // When no opposite family is enabled, report the attempted
                // resolution path so operators understand the failure.
                Err(_) => format!("opposite_of({})", planner_family.as_str()),
            }
        } else {
            planner_family.as_str().to_owned()
        }
    }

    /// Returns true if the given role resolves to the opposite family of
    /// the planner in the runtime resolution path.
    fn role_uses_opposite_family(role: BackendPolicyRole) -> bool {
        matches!(
            role,
            BackendPolicyRole::Reviewer
                | BackendPolicyRole::Qa
                | BackendPolicyRole::AcceptanceQa
                | BackendPolicyRole::Completer
        )
    }

    fn config_source_for_role(&self, role: BackendPolicyRole) -> String {
        // If the role has an explicit override set, return the role-specific
        // config key. Otherwise the role inherits from `default_backend`.
        if self.policy.has_explicit_override(role) {
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
        } else {
            "default_backend".to_owned()
        }
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
                if entry.source
                    == crate::contexts::workspace_governance::config::ConfigValueSource::Default
                {
                    if self.policy.has_explicit_override(role) {
                        // Some roles can resolve via a compiled per-role default
                        // even when the config key itself was never set.
                        format!("{key} (default)")
                    } else {
                        // No explicit override set — role inherits from default_backend
                        let base_source = self.source_for_base_backend();
                        format!("default_backend ({})", base_source)
                    }
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

    fn final_review_reviewers_override_source(&self) -> String {
        match self.config.get("final_review.backends") {
            Ok(entry) => format!("final_review.backends ({})", entry.source),
            Err(_) => "final_review.backends".to_owned(),
        }
    }

    fn role_model_source_for_role(
        &self,
        role: BackendPolicyRole,
        family: BackendFamily,
        include_defaults: bool,
    ) -> Option<String> {
        let key = format!("backends.{}.role_models.{}", family.as_str(), role.as_str());
        let entry = self.config.get(&key).ok()?;
        if matches!(
            entry.value,
            crate::contexts::workspace_governance::config::ConfigValue::String(None)
        ) {
            return None;
        }

        if !include_defaults
            && entry.source
                == crate::contexts::workspace_governance::config::ConfigValueSource::Default
        {
            return None;
        }

        Some(match entry.source {
            crate::contexts::workspace_governance::config::ConfigValueSource::Default => {
                format!("{key} (default)")
            }
            source => source.to_string(),
        })
    }

    /// Determine the source precedence for a role's resolved model_id.
    ///
    /// Model resolution order (matching `BackendPolicyService::target_for_family`):
    /// 1. Explicit model from the role's backend selection override
    /// 2. Explicit role-specific model from `backends.<family>.role_models.<role>`
    /// 3. Base backend embedded model from `default_backend = "family(model)"`
    ///    or `settings.default_model` when it applies to the resolved family
    /// 4. Compiled role-specific default model
    /// 5. Family default model
    fn model_source_for_role(&self, role: BackendPolicyRole, family: BackendFamily) -> String {
        let bp = self.config.backend_policy();

        // 1. If the role has an explicit selection with a model, model comes
        //    from the same config source as the role override.
        let selection = match role {
            BackendPolicyRole::Planner => bp.planner_backend.as_ref(),
            BackendPolicyRole::Implementer => bp.implementer_backend.as_ref(),
            BackendPolicyRole::Reviewer => bp.reviewer_backend.as_ref(),
            BackendPolicyRole::Qa | BackendPolicyRole::AcceptanceQa => bp.qa_backend.as_ref(),
            BackendPolicyRole::PromptReviewer => bp.prompt_review_refiner_backend.as_ref(),
            BackendPolicyRole::Arbiter => bp.final_review_arbiter_backend.as_ref(),
            _ => None,
        };
        if let Some(sel) = selection {
            if sel.model.is_some() {
                return self.override_source_for_role(role);
            }
        }

        // 2. Check explicit role-specific models from workspace/project config.
        if let Some(source) = self.role_model_source_for_role(role, family, false) {
            return source;
        }

        // 3. Check explicit base backend model overrides.
        if family == bp.base_backend.family && bp.base_backend.model.is_some() {
            if bp.default_model.as_ref() == bp.base_backend.model.as_ref() {
                return self.source_for_default_model();
            } else {
                return self.source_for_base_backend();
            }
        }

        // 4. Check compiled role-specific defaults.
        if let Some(source) = self.role_model_source_for_role(role, family, true) {
            return source;
        }

        // 5. Family default
        "default".to_owned()
    }

    fn role_effective_view(&self, role: BackendPolicyRole) -> RoleEffectiveView {
        let override_source = self.override_source_for_role(role);
        let session_policy = session_policy_for_role(role);

        match self.policy.resolve_role_target(role, 1) {
            Ok(target) => {
                let family = target.backend.family;
                let timeout = self.policy.timeout_for_role(family, role);
                let model_source = self.model_source_for_role(role, family);
                let timeout_source = self.timeout_source_for_role(role, family);
                RoleEffectiveView {
                    role: role.as_str().to_owned(),
                    backend_family: family.as_str().to_owned(),
                    model_id: target.model.model_id.clone(),
                    timeout_seconds: timeout.as_secs(),
                    session_policy,
                    override_source,
                    model_source,
                    timeout_source,
                    resolution_error: None,
                }
            }
            Err(err) => {
                let configured_family = self.family_for_role(role);
                RoleEffectiveView {
                    role: role.as_str().to_owned(),
                    backend_family: configured_family,
                    model_id: "unresolved".to_owned(),
                    timeout_seconds: 0,
                    session_policy,
                    override_source,
                    model_source: "unresolved".to_owned(),
                    timeout_source: "unresolved".to_owned(),
                    resolution_error: Some(err.to_string()),
                }
            }
        }
    }

    fn final_review_reviewer_effective_views(&self) -> Vec<RoleEffectiveView> {
        let override_source = self.final_review_reviewers_override_source();

        let member_views = self
            .config
            .final_review_policy()
            .backends
            .iter()
            .enumerate()
            .map(|(idx, spec)| {
                let selection = spec.selection();
                match self
                    .policy
                    .resolve_selection_target(BackendPolicyRole::FinalReviewer, selection)
                {
                    Ok(target) => {
                        let family = target.backend.family;
                        let timeout = self
                            .policy
                            .timeout_for_role(family, BackendPolicyRole::FinalReviewer);
                        let model_source = if selection.model.is_some() {
                            override_source.clone()
                        } else {
                            self.model_source_for_role(BackendPolicyRole::FinalReviewer, family)
                        };
                        RoleEffectiveView {
                            role: format!("final_review_panel.reviewer[{idx}]"),
                            backend_family: family.as_str().to_owned(),
                            model_id: target.model.model_id,
                            timeout_seconds: timeout.as_secs(),
                            session_policy: "new_session".to_owned(),
                            override_source: override_source.clone(),
                            model_source,
                            timeout_source: self
                                .timeout_source_for_role(BackendPolicyRole::FinalReviewer, family),
                            resolution_error: None,
                        }
                    }
                    Err(err) => RoleEffectiveView {
                        role: format!("final_review_panel.reviewer[{idx}]"),
                        backend_family: selection.family.as_str().to_owned(),
                        model_id: selection
                            .model
                            .clone()
                            .unwrap_or_else(|| "unresolved".to_owned()),
                        timeout_seconds: 0,
                        session_policy: "new_session".to_owned(),
                        override_source: override_source.clone(),
                        model_source: "unresolved".to_owned(),
                        timeout_source: "unresolved".to_owned(),
                        resolution_error: Some(err.to_string()),
                    },
                }
            })
            .collect::<Vec<_>>();

        let mut views =
            Vec::with_capacity(member_views.len() + usize::from(!member_views.is_empty()));
        if let Some(first_member) = self
            .primary_final_review_reviewer_configured_index()
            .and_then(|idx| member_views.get(idx).cloned())
            .or_else(|| member_views.first().cloned())
        {
            views.push(RoleEffectiveView {
                role: "final_reviewer".to_owned(),
                ..first_member
            });
        }
        views.extend(member_views);
        views
    }

    /// Determine the source precedence for a role's resolved timeout_seconds.
    ///
    /// Timeout resolution order (matching `BackendPolicyService::timeout_for_role`):
    /// 1. Role-specific timeout from `backends.<family>.role_timeouts.<role>`
    /// 2. Backend-level timeout from `backends.<family>.timeout_seconds`
    /// 3. Global default (`DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS`)
    fn timeout_source_for_role(&self, role: BackendPolicyRole, family: BackendFamily) -> String {
        // 1. Check role-specific timeout
        let role_key = format!(
            "backends.{}.role_timeouts.{}",
            family.as_str(),
            role.as_str()
        );
        if let Ok(entry) = self.config.get(&role_key) {
            let source = entry.source;
            if source != crate::contexts::workspace_governance::config::ConfigValueSource::Default {
                return source.to_string();
            }
        }

        // 2. Check backend-level timeout
        let backend_key = format!("backends.{}.timeout_seconds", family.as_str());
        if let Ok(entry) = self.config.get(&backend_key) {
            let source = entry.source;
            if source != crate::contexts::workspace_governance::config::ConfigValueSource::Default {
                return source.to_string();
            }
        }

        // 3. Global default
        "default".to_owned()
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

/// Return the stage-derived roles that are effectively required for the
/// `backend check` command, excluding panel-specific roles (Completer,
/// FinalReviewer) that have their own dedicated panel checks.
///
/// This prevents false failures when explicit panel config (e.g.,
/// `completion.backends`, `final_review.backends`) fully determines
/// runtime targets, making the generic stage-derived role resolution
/// irrelevant.
fn effectively_required_stage_roles(flow_def: &FlowDefinition) -> Vec<BackendPolicyRole> {
    roles_for_flow(flow_def)
        .into_iter()
        .filter(|role| {
            !matches!(
                role,
                BackendPolicyRole::Completer | BackendPolicyRole::FinalReviewer
            )
        })
        .collect()
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

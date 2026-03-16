use std::time::Duration;

use crate::contexts::workspace_governance::config::{
    EffectiveConfig, DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
};
use crate::shared::domain::{
    BackendFamily, BackendPolicyRole, BackendSelection, PanelBackendSpec, ResolvedBackendTarget,
    StageId,
};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionPanelResolution {
    pub planner: ResolvedBackendTarget,
    pub completers: Vec<ResolvedBackendTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptReviewPanelResolution {
    pub refiner: ResolvedBackendTarget,
    pub validators: Vec<ResolvedBackendTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalReviewPanelResolution {
    pub reviewers: Vec<ResolvedBackendTarget>,
    pub arbiter: ResolvedBackendTarget,
}

#[derive(Debug, Clone, Copy)]
pub struct BackendPolicyService<'a> {
    config: &'a EffectiveConfig,
}

impl<'a> BackendPolicyService<'a> {
    pub fn new(config: &'a EffectiveConfig) -> Self {
        Self { config }
    }

    pub fn resolve_stage_target(
        &self,
        stage_id: StageId,
        cycle: u32,
    ) -> AppResult<ResolvedBackendTarget> {
        self.resolve_role_target(self.policy_role_for_stage(stage_id), cycle)
    }

    pub fn resolve_role_target(
        &self,
        role: BackendPolicyRole,
        cycle: u32,
    ) -> AppResult<ResolvedBackendTarget> {
        if let Some(selection) = self.selection_for_role(role) {
            return self.selection_to_target(role, selection);
        }

        let planner_family = self.planner_family_for_cycle(cycle)?;
        let family = match role {
            BackendPolicyRole::Planner
            | BackendPolicyRole::PromptReviewer
            | BackendPolicyRole::PromptValidator
            | BackendPolicyRole::Reviewer
            | BackendPolicyRole::FinalReviewer
            | BackendPolicyRole::Arbiter => planner_family,
            BackendPolicyRole::Implementer
            | BackendPolicyRole::Qa
            | BackendPolicyRole::AcceptanceQa
            | BackendPolicyRole::Completer => self.opposite_family(planner_family)?,
        };

        self.target_for_family(role, family, None)
    }

    pub fn resolve_completion_panel(&self, cycle: u32) -> AppResult<CompletionPanelResolution> {
        let planner = self.resolve_role_target(BackendPolicyRole::Planner, cycle)?;
        let completers = self.resolve_panel_backends(
            &self.config.completion_policy().backends,
            self.config.completion_policy().min_completers,
            BackendPolicyRole::Completer,
        )?;

        Ok(CompletionPanelResolution {
            planner,
            completers,
        })
    }

    pub fn resolve_prompt_review_panel(
        &self,
        cycle: u32,
    ) -> AppResult<PromptReviewPanelResolution> {
        let refiner = self
            .config
            .prompt_review_policy()
            .refiner_backend
            .as_ref()
            .map(|selection| self.selection_to_target(BackendPolicyRole::PromptReviewer, selection))
            .transpose()?
            .unwrap_or(self.resolve_role_target(BackendPolicyRole::PromptReviewer, cycle)?);
        let validators = self.resolve_panel_backends(
            &self.config.prompt_review_policy().validator_backends,
            self.config.prompt_review_policy().min_reviewers,
            BackendPolicyRole::PromptValidator,
        )?;

        Ok(PromptReviewPanelResolution {
            refiner,
            validators,
        })
    }

    pub fn resolve_final_review_panel(
        &self,
        cycle: u32,
    ) -> AppResult<FinalReviewPanelResolution> {
        let reviewers = self.resolve_panel_backends(
            &self.config.final_review_policy().backends,
            self.config.final_review_policy().min_reviewers,
            BackendPolicyRole::FinalReviewer,
        )?;
        let arbiter = self
            .config
            .final_review_policy()
            .arbiter_backend
            .as_ref()
            .map(|selection| self.selection_to_target(BackendPolicyRole::Arbiter, selection))
            .transpose()?
            .unwrap_or(self.resolve_role_target(BackendPolicyRole::Arbiter, cycle)?);

        Ok(FinalReviewPanelResolution { reviewers, arbiter })
    }

    pub fn timeout_for_role(&self, backend: BackendFamily, role: BackendPolicyRole) -> Duration {
        let settings = self.runtime_settings(backend);
        let seconds = settings
            .and_then(|settings| settings.role_timeouts.timeout_for(role))
            .or_else(|| settings.and_then(|settings| settings.timeout_seconds))
            .unwrap_or(DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS);
        Duration::from_secs(seconds)
    }

    pub fn opposite_family(&self, family: BackendFamily) -> AppResult<BackendFamily> {
        let candidates = match family {
            BackendFamily::Claude => [BackendFamily::Codex, BackendFamily::OpenRouter],
            BackendFamily::Codex => [BackendFamily::Claude, BackendFamily::OpenRouter],
            BackendFamily::OpenRouter => [BackendFamily::Claude, BackendFamily::Codex],
            BackendFamily::Stub => [BackendFamily::Stub, BackendFamily::Stub],
        };

        for candidate in candidates {
            if candidate == BackendFamily::Stub {
                continue;
            }
            if self.backend_enabled(candidate) {
                return Ok(candidate);
            }
        }

        Err(AppError::BackendUnavailable {
            backend: family.to_string(),
            details: "no opposite backend family is enabled".to_owned(),
        })
    }

    pub fn planner_family_for_cycle(&self, cycle: u32) -> AppResult<BackendFamily> {
        if cycle == 0 {
            return Err(AppError::InvalidStageCursorField { field: "cycle" });
        }

        if cycle % 2 == 1 {
            Ok(self.config.backend_policy().base_backend.family)
        } else {
            self.opposite_family(self.config.backend_policy().base_backend.family)
        }
    }

    pub fn policy_role_for_stage(&self, stage_id: StageId) -> BackendPolicyRole {
        match stage_id {
            StageId::PromptReview | StageId::Planning | StageId::DocsPlan | StageId::CiPlan => {
                BackendPolicyRole::Planner
            }
            StageId::Implementation
            | StageId::PlanAndImplement
            | StageId::ApplyFixes
            | StageId::DocsUpdate
            | StageId::CiUpdate => BackendPolicyRole::Implementer,
            StageId::Qa | StageId::DocsValidation | StageId::CiValidation => BackendPolicyRole::Qa,
            StageId::AcceptanceQa => BackendPolicyRole::AcceptanceQa,
            StageId::Review => BackendPolicyRole::Reviewer,
            StageId::CompletionPanel => BackendPolicyRole::Completer,
            StageId::FinalReview => BackendPolicyRole::FinalReviewer,
        }
    }

    fn resolve_panel_backends(
        &self,
        specs: &[PanelBackendSpec],
        minimum: usize,
        role: BackendPolicyRole,
    ) -> AppResult<Vec<ResolvedBackendTarget>> {
        let mut resolved = Vec::new();

        for spec in specs {
            let backend = spec.backend();
            if !self.backend_enabled(backend) {
                if spec.is_optional() {
                    continue;
                }
                return Err(AppError::BackendUnavailable {
                    backend: backend.to_string(),
                    details: "required backend is disabled or unavailable".to_owned(),
                });
            }

            resolved.push(self.target_for_family(role, backend, None)?);
        }

        if resolved.len() < minimum {
            return Err(AppError::InvalidConfigValue {
                key: "panel_backends".to_owned(),
                value: resolved.len().to_string(),
                reason: format!("resolved backend count is below the required minimum of {minimum}"),
            });
        }

        Ok(resolved)
    }

    fn selection_to_target(
        &self,
        role: BackendPolicyRole,
        selection: &BackendSelection,
    ) -> AppResult<ResolvedBackendTarget> {
        if !self.backend_enabled(selection.family) {
            return Err(AppError::BackendUnavailable {
                backend: selection.family.to_string(),
                details: "configured backend is disabled or unavailable".to_owned(),
            });
        }

        self.target_for_family(role, selection.family, selection.model.clone())
    }

    fn target_for_family(
        &self,
        role: BackendPolicyRole,
        family: BackendFamily,
        explicit_model: Option<String>,
    ) -> AppResult<ResolvedBackendTarget> {
        let model_id = explicit_model
            .or_else(|| {
                self.runtime_settings(family)
                    .and_then(|settings| settings.role_models.model_for(role))
                    .map(str::to_owned)
            })
            .or_else(|| {
                if family == self.config.backend_policy().base_backend.family {
                    self.config.backend_policy().base_backend.model.clone()
                } else {
                    None
                }
            })
            .unwrap_or_else(|| family.default_model_id().to_owned());

        Ok(ResolvedBackendTarget::new(family, model_id))
    }

    fn selection_for_role(&self, role: BackendPolicyRole) -> Option<&BackendSelection> {
        match role {
            BackendPolicyRole::Planner => self.config.backend_policy().planner_backend.as_ref(),
            BackendPolicyRole::Implementer => {
                self.config.backend_policy().implementer_backend.as_ref()
            }
            BackendPolicyRole::Reviewer => self.config.backend_policy().reviewer_backend.as_ref(),
            BackendPolicyRole::Qa | BackendPolicyRole::AcceptanceQa => {
                self.config.backend_policy().qa_backend.as_ref()
            }
            BackendPolicyRole::PromptReviewer => self
                .config
                .backend_policy()
                .prompt_review_refiner_backend
                .as_ref(),
            BackendPolicyRole::Arbiter => self
                .config
                .backend_policy()
                .final_review_arbiter_backend
                .as_ref(),
            BackendPolicyRole::Completer
            | BackendPolicyRole::FinalReviewer
            | BackendPolicyRole::PromptValidator => None,
        }
    }

    fn runtime_settings(
        &self,
        family: BackendFamily,
    ) -> Option<&crate::shared::domain::BackendRuntimeSettings> {
        self.config
            .backend_policy()
            .backends
            .get(family.as_str())
    }

    fn backend_enabled(&self, family: BackendFamily) -> bool {
        self.runtime_settings(family)
            .and_then(|settings| settings.enabled)
            .unwrap_or(matches!(family, BackendFamily::Claude | BackendFamily::Codex))
    }
}

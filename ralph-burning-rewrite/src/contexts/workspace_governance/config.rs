use std::fmt;
use std::path::Path;

use toml_edit::{value, Array, DocumentMut, Item};

use crate::adapters::fs::FileSystem;
use crate::shared::domain::{
    BackendFamily, BackendPolicyRole, BackendRoleModels, BackendRoleTimeouts,
    BackendRuntimeSettings, BackendSelection, EffectiveBackendPolicy, EffectiveCompletionPolicy,
    EffectiveFinalReviewPolicy, EffectivePromptReviewPolicy, EffectiveRunPolicy,
    EffectiveValidationPolicy, FlowPreset, PanelBackendSpec, ProjectConfig, ProjectId,
    PromptChangeAction, WorkspaceConfig,
};
use crate::shared::error::{AppError, AppResult};

use super::{load_workspace_config, workspace_config_path};

/// Default: enabled.
pub const DEFAULT_PROMPT_REVIEW_ENABLED: bool = true;
/// Default: standard.
pub const DEFAULT_FLOW_PRESET: FlowPreset = FlowPreset::Standard;
pub const DEFAULT_MAX_QA_ITERATIONS: u32 = 3;
pub const DEFAULT_MAX_REVIEW_ITERATIONS: u32 = 3;
pub const DEFAULT_MIN_REVIEWERS: usize = 2;
pub const DEFAULT_MIN_COMPLETERS: usize = 2;
pub const DEFAULT_CONSENSUS_THRESHOLD: f64 = 0.66;
pub const DEFAULT_MAX_FINAL_RESTARTS: u32 = 2;
pub const DEFAULT_MAX_COMPLETION_ROUNDS: u32 = 10;
pub const DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS: u64 = 3600;

const DEFAULT_BASE_BACKEND: BackendFamily = BackendFamily::Claude;
const UNSET_LITERALS: &[&str] = &["unset", "none", "null"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigValueSource {
    Default,
    WorkspaceToml,
    ProjectToml,
    CliOverride,
}

impl fmt::Display for ConfigValueSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("default"),
            Self::WorkspaceToml => f.write_str("workspace.toml"),
            Self::ProjectToml => f.write_str("project config.toml"),
            Self::CliOverride => f.write_str("cli override"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    Bool(bool),
    FlowPreset(FlowPreset),
    Float(f64),
    Integer(u64),
    String(Option<String>),
    StringList(Vec<String>),
}

impl ConfigValue {
    pub fn display_value(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::FlowPreset(value) => value.as_str().to_owned(),
            Self::Float(value) => format_float(*value),
            Self::Integer(value) => value.to_string(),
            Self::String(Some(value)) => value.clone(),
            Self::String(None) => "<unset>".to_owned(),
            Self::StringList(values) => {
                let rendered = values
                    .iter()
                    .map(|value| format!("\"{value}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{rendered}]")
            }
        }
    }

    pub fn toml_like_value(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::FlowPreset(value) => format!("\"{}\"", value.as_str()),
            Self::Float(value) => format_float(*value),
            Self::Integer(value) => value.to_string(),
            Self::String(Some(value)) => format!("\"{value}\""),
            Self::String(None) => "\"<unset>\"".to_owned(),
            Self::StringList(values) => {
                let rendered = values
                    .iter()
                    .map(|value| format!("\"{value}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{rendered}]")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfigEntry {
    pub key: String,
    pub value: ConfigValue,
    pub source: ConfigValueSource,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CliBackendOverrides {
    pub backend: Option<BackendSelection>,
    pub planner_backend: Option<BackendSelection>,
    pub implementer_backend: Option<BackendSelection>,
    pub reviewer_backend: Option<BackendSelection>,
    pub qa_backend: Option<BackendSelection>,
}

impl CliBackendOverrides {
    pub fn is_empty(&self) -> bool {
        self.backend.is_none()
            && self.planner_backend.is_none()
            && self.implementer_backend.is_none()
            && self.reviewer_backend.is_none()
            && self.qa_backend.is_none()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveConfig {
    workspace_config: WorkspaceConfig,
    project_config: ProjectConfig,
    cli_overrides: CliBackendOverrides,
    run_policy: EffectiveRunPolicy,
    prompt_review_policy: EffectivePromptReviewPolicy,
    completion_policy: EffectiveCompletionPolicy,
    final_review_policy: EffectiveFinalReviewPolicy,
    validation_policy: EffectiveValidationPolicy,
    backend_policy: EffectiveBackendPolicy,
}

impl EffectiveConfig {
    pub fn load(base_dir: &Path) -> AppResult<Self> {
        Self::load_for_project(base_dir, None, CliBackendOverrides::default())
    }

    pub fn load_for_project(
        base_dir: &Path,
        project_id: Option<&ProjectId>,
        cli_overrides: CliBackendOverrides,
    ) -> AppResult<Self> {
        let workspace_config = load_workspace_config(base_dir)?;
        let project_config = match project_id {
            Some(project_id) => FileSystem::read_project_config(base_dir, project_id)?,
            None => ProjectConfig::default(),
        };

        let run_policy = EffectiveRunPolicy {
            default_flow: resolve_scalar(
                workspace_config.settings.default_flow,
                project_config.settings.default_flow,
                None,
                DEFAULT_FLOW_PRESET,
            ),
            max_qa_iterations: resolve_scalar(
                workspace_config.workflow.max_qa_iterations,
                project_config.workflow.max_qa_iterations,
                None,
                DEFAULT_MAX_QA_ITERATIONS,
            ),
            max_review_iterations: resolve_scalar(
                workspace_config.workflow.max_review_iterations,
                project_config.workflow.max_review_iterations,
                None,
                DEFAULT_MAX_REVIEW_ITERATIONS,
            ),
            prompt_change_action: resolve_scalar(
                workspace_config.workflow.prompt_change_action,
                project_config.workflow.prompt_change_action,
                None,
                PromptChangeAction::RestartCycle,
            ),
        };

        let prompt_review_policy = EffectivePromptReviewPolicy {
            enabled: resolve_scalar(
                workspace_config.prompt_review.enabled,
                project_config.prompt_review.enabled,
                None,
                DEFAULT_PROMPT_REVIEW_ENABLED,
            ),
            refiner_backend: resolve_backend_selection(
                workspace_config.prompt_review.refiner_backend.as_deref(),
                project_config.prompt_review.refiner_backend.as_deref(),
                None,
            )?,
            validator_backends: resolve_vec(
                workspace_config.prompt_review.validator_backends.as_deref(),
                project_config.prompt_review.validator_backends.as_deref(),
                None,
                default_prompt_review_backends(),
            ),
            min_reviewers: resolve_scalar(
                workspace_config.prompt_review.min_reviewers,
                project_config.prompt_review.min_reviewers,
                None,
                DEFAULT_MIN_REVIEWERS,
            ),
        };

        let completion_policy = EffectiveCompletionPolicy {
            backends: resolve_vec(
                workspace_config.completion.backends.as_deref(),
                project_config.completion.backends.as_deref(),
                None,
                default_completion_backends(),
            ),
            min_completers: resolve_scalar(
                workspace_config.completion.min_completers,
                project_config.completion.min_completers,
                None,
                DEFAULT_MIN_COMPLETERS,
            ),
            consensus_threshold: resolve_scalar(
                workspace_config.completion.consensus_threshold,
                project_config.completion.consensus_threshold,
                None,
                DEFAULT_CONSENSUS_THRESHOLD,
            ),
        };

        let final_review_policy = EffectiveFinalReviewPolicy {
            enabled: resolve_scalar(
                workspace_config.final_review.enabled,
                project_config.final_review.enabled,
                None,
                true,
            ),
            backends: resolve_vec(
                workspace_config.final_review.backends.as_deref(),
                project_config.final_review.backends.as_deref(),
                None,
                default_final_review_backends(),
            ),
            arbiter_backend: resolve_backend_selection(
                workspace_config.final_review.arbiter_backend.as_deref(),
                project_config.final_review.arbiter_backend.as_deref(),
                None,
            )?,
            min_reviewers: resolve_scalar(
                workspace_config.final_review.min_reviewers,
                project_config.final_review.min_reviewers,
                None,
                DEFAULT_MIN_REVIEWERS,
            ),
            consensus_threshold: resolve_scalar(
                workspace_config.final_review.consensus_threshold,
                project_config.final_review.consensus_threshold,
                None,
                DEFAULT_CONSENSUS_THRESHOLD,
            ),
            max_restarts: resolve_scalar(
                workspace_config.final_review.max_restarts,
                project_config.final_review.max_restarts,
                None,
                DEFAULT_MAX_FINAL_RESTARTS,
            ),
        };

        let validation_policy = EffectiveValidationPolicy {
            standard_commands: resolve_vec(
                workspace_config.validation.standard_commands.as_deref(),
                project_config.validation.standard_commands.as_deref(),
                None,
                Vec::<String>::new(),
            ),
            docs_commands: resolve_vec(
                workspace_config.validation.docs_commands.as_deref(),
                project_config.validation.docs_commands.as_deref(),
                None,
                Vec::<String>::new(),
            ),
            ci_commands: resolve_vec(
                workspace_config.validation.ci_commands.as_deref(),
                project_config.validation.ci_commands.as_deref(),
                None,
                Vec::<String>::new(),
            ),
            pre_commit_fmt: resolve_scalar(
                workspace_config.validation.pre_commit_fmt,
                project_config.validation.pre_commit_fmt,
                None,
                true,
            ),
            pre_commit_clippy: resolve_scalar(
                workspace_config.validation.pre_commit_clippy,
                project_config.validation.pre_commit_clippy,
                None,
                true,
            ),
            pre_commit_nix_build: resolve_scalar(
                workspace_config.validation.pre_commit_nix_build,
                project_config.validation.pre_commit_nix_build,
                None,
                false,
            ),
            pre_commit_fmt_auto_fix: resolve_scalar(
                workspace_config.validation.pre_commit_fmt_auto_fix,
                project_config.validation.pre_commit_fmt_auto_fix,
                None,
                false,
            ),
        };

        let base_backend_string = cli_overrides
            .backend
            .as_ref()
            .map(|selection| selection.display_string());
        let mut base_backend = resolve_backend_selection(
            workspace_config.settings.default_backend.as_deref(),
            project_config.settings.default_backend.as_deref(),
            cli_overrides
                .backend
                .as_ref()
                .map(|selection| selection.display_string()),
        )?
        .unwrap_or_else(|| BackendSelection::new(DEFAULT_BASE_BACKEND, None));

        let default_model = resolve_optional(
            workspace_config.settings.default_model.clone(),
            project_config.settings.default_model.clone(),
            cli_overrides
                .backend
                .as_ref()
                .and_then(|selection| selection.model.clone()),
        );
        if base_backend.model.is_none() {
            base_backend.model = default_model.clone();
        }

        let backend_policy = EffectiveBackendPolicy {
            base_backend,
            default_model,
            planner_backend: resolve_backend_selection(
                workspace_config.workflow.planner_backend.as_deref(),
                project_config.workflow.planner_backend.as_deref(),
                cli_overrides
                    .planner_backend
                    .as_ref()
                    .map(|selection| selection.display_string()),
            )?,
            implementer_backend: resolve_backend_selection(
                workspace_config.workflow.implementer_backend.as_deref(),
                project_config.workflow.implementer_backend.as_deref(),
                cli_overrides
                    .implementer_backend
                    .as_ref()
                    .map(|selection| selection.display_string()),
            )?,
            reviewer_backend: resolve_backend_selection(
                workspace_config.workflow.reviewer_backend.as_deref(),
                project_config.workflow.reviewer_backend.as_deref(),
                cli_overrides
                    .reviewer_backend
                    .as_ref()
                    .map(|selection| selection.display_string()),
            )?,
            qa_backend: resolve_backend_selection(
                workspace_config.workflow.qa_backend.as_deref(),
                project_config.workflow.qa_backend.as_deref(),
                cli_overrides
                    .qa_backend
                    .as_ref()
                    .map(|selection| selection.display_string()),
            )?,
            prompt_review_refiner_backend: prompt_review_policy.refiner_backend.clone(),
            final_review_arbiter_backend: final_review_policy.arbiter_backend.clone(),
            backends: merge_backend_runtime_map(
                workspace_config.backends.clone(),
                project_config.backends.clone(),
            ),
        };

        let _ = base_backend_string;

        Ok(Self {
            workspace_config,
            project_config,
            cli_overrides,
            run_policy,
            prompt_review_policy,
            completion_policy,
            final_review_policy,
            validation_policy,
            backend_policy,
        })
    }

    pub fn run_policy(&self) -> &EffectiveRunPolicy {
        &self.run_policy
    }

    pub fn prompt_review_policy(&self) -> &EffectivePromptReviewPolicy {
        &self.prompt_review_policy
    }

    pub fn completion_policy(&self) -> &EffectiveCompletionPolicy {
        &self.completion_policy
    }

    pub(crate) fn completion_backends_are_explicit(&self) -> bool {
        self.project_config.completion.backends.is_some()
            || self.workspace_config.completion.backends.is_some()
    }

    pub fn final_review_policy(&self) -> &EffectiveFinalReviewPolicy {
        &self.final_review_policy
    }

    pub fn validation_policy(&self) -> &EffectiveValidationPolicy {
        &self.validation_policy
    }

    pub fn backend_policy(&self) -> &EffectiveBackendPolicy {
        &self.backend_policy
    }

    pub fn get(&self, key: &str) -> AppResult<ConfigEntry> {
        if !known_config_keys().iter().any(|candidate| candidate == key) {
            return Err(AppError::UnknownConfigKey {
                key: key.to_owned(),
            });
        }

        self.entry_for(key)
    }

    pub fn set(base_dir: &Path, key: &str, value: &str) -> AppResult<ConfigEntry> {
        let _ = Self::load(base_dir)?;

        let config_path = workspace_config_path(base_dir);
        let raw = FileSystem::read_to_string(&config_path)?;
        let mut document = raw.parse::<DocumentMut>()?;
        apply_to_document(&mut document, key, value)?;
        FileSystem::write_atomic(&config_path, &document.to_string())?;

        Self::load(base_dir)?.get(key)
    }

    pub fn get_project(
        base_dir: &Path,
        project_id: &ProjectId,
        key: &str,
    ) -> AppResult<ConfigEntry> {
        Self::load_for_project(base_dir, Some(project_id), CliBackendOverrides::default())?.get(key)
    }

    pub fn set_project(
        base_dir: &Path,
        project_id: &ProjectId,
        key: &str,
        value: &str,
    ) -> AppResult<ConfigEntry> {
        let _ = Self::load_for_project(base_dir, Some(project_id), CliBackendOverrides::default())?;
        let mut project_config = FileSystem::read_project_config(base_dir, project_id)?;
        let raw = toml::to_string_pretty(&project_config)?;
        let mut document = raw.parse::<DocumentMut>()?;
        apply_to_document(&mut document, key, value)?;
        project_config = toml::from_str(&document.to_string())?;
        FileSystem::write_project_config(base_dir, project_id, &project_config)?;

        Self::load_for_project(base_dir, Some(project_id), CliBackendOverrides::default())?.get(key)
    }

    pub fn entries(&self) -> Vec<ConfigEntry> {
        known_config_keys()
            .into_iter()
            .filter_map(|key| self.entry_for(&key).ok())
            .collect()
    }

    /// Default: `true`.
    pub fn prompt_review_enabled(&self) -> bool {
        self.prompt_review_policy.enabled
    }

    /// Default: `standard`.
    pub fn default_flow(&self) -> FlowPreset {
        self.run_policy.default_flow
    }

    /// Explicit default backend override from CLI, project config, or workspace config.
    pub fn default_backend(&self) -> Option<&str> {
        self.cli_overrides
            .backend
            .as_ref()
            .map(|selection| selection.family.as_str())
            .or(self.project_config.settings.default_backend.as_deref())
            .or(self.workspace_config.settings.default_backend.as_deref())
    }

    /// Explicit default model override from CLI, project config, or workspace config.
    pub fn default_model(&self) -> Option<&str> {
        self.cli_overrides
            .backend
            .as_ref()
            .and_then(|selection| selection.model.as_deref())
            .or(self.project_config.settings.default_model.as_deref())
            .or(self.workspace_config.settings.default_model.as_deref())
    }

    fn entry_for(&self, key: &str) -> AppResult<ConfigEntry> {
        let segments = split_key(key)?;

        let (value, source) = match segments.as_slice() {
            ["default_flow"] => (
                ConfigValue::FlowPreset(self.run_policy.default_flow),
                source_for_option(
                    self.workspace_config.settings.default_flow,
                    self.project_config.settings.default_flow,
                    None::<FlowPreset>,
                ),
            ),
            ["default_backend"] => (
                ConfigValue::String(Some(
                    self.backend_policy.base_backend.family.as_str().to_owned(),
                )),
                source_for_option(
                    self.workspace_config
                        .settings
                        .default_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .settings
                        .default_backend
                        .clone()
                        .map(|_| ()),
                    self.cli_overrides.backend.clone().map(|_| ()),
                ),
            ),
            ["default_model"] => (
                ConfigValue::String(self.default_model().map(str::to_owned)),
                source_for_option(
                    self.workspace_config
                        .settings
                        .default_model
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .settings
                        .default_model
                        .clone()
                        .map(|_| ()),
                    self.cli_overrides
                        .backend
                        .as_ref()
                        .and_then(|selection| selection.model.clone())
                        .map(|_| ()),
                ),
            ),
            ["prompt_review", "enabled"] => (
                ConfigValue::Bool(self.prompt_review_policy.enabled),
                source_for_option(
                    self.workspace_config.prompt_review.enabled,
                    self.project_config.prompt_review.enabled,
                    None::<bool>,
                ),
            ),
            ["prompt_review", "refiner_backend"] => (
                ConfigValue::String(
                    self.prompt_review_policy
                        .refiner_backend
                        .as_ref()
                        .map(ToString::to_string),
                ),
                source_for_option(
                    self.workspace_config
                        .prompt_review
                        .refiner_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .prompt_review
                        .refiner_backend
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["prompt_review", "validator_backends"] => (
                ConfigValue::StringList(
                    self.prompt_review_policy
                        .validator_backends
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
                source_for_option(
                    self.workspace_config
                        .prompt_review
                        .validator_backends
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .prompt_review
                        .validator_backends
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["prompt_review", "min_reviewers"] => (
                ConfigValue::Integer(self.prompt_review_policy.min_reviewers as u64),
                source_for_option(
                    self.workspace_config.prompt_review.min_reviewers,
                    self.project_config.prompt_review.min_reviewers,
                    None::<usize>,
                ),
            ),
            ["workflow", "planner_backend"] => (
                ConfigValue::String(
                    self.backend_policy
                        .planner_backend
                        .as_ref()
                        .map(ToString::to_string),
                ),
                source_for_option(
                    self.workspace_config
                        .workflow
                        .planner_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .workflow
                        .planner_backend
                        .clone()
                        .map(|_| ()),
                    self.cli_overrides.planner_backend.clone().map(|_| ()),
                ),
            ),
            ["workflow", "implementer_backend"] => (
                ConfigValue::String(
                    self.backend_policy
                        .implementer_backend
                        .as_ref()
                        .map(ToString::to_string),
                ),
                source_for_option(
                    self.workspace_config
                        .workflow
                        .implementer_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .workflow
                        .implementer_backend
                        .clone()
                        .map(|_| ()),
                    self.cli_overrides.implementer_backend.clone().map(|_| ()),
                ),
            ),
            ["workflow", "reviewer_backend"] => (
                ConfigValue::String(
                    self.backend_policy
                        .reviewer_backend
                        .as_ref()
                        .map(ToString::to_string),
                ),
                source_for_option(
                    self.workspace_config
                        .workflow
                        .reviewer_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .workflow
                        .reviewer_backend
                        .clone()
                        .map(|_| ()),
                    self.cli_overrides.reviewer_backend.clone().map(|_| ()),
                ),
            ),
            ["workflow", "qa_backend"] => (
                ConfigValue::String(
                    self.backend_policy
                        .qa_backend
                        .as_ref()
                        .map(ToString::to_string),
                ),
                source_for_option(
                    self.workspace_config
                        .workflow
                        .qa_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config.workflow.qa_backend.clone().map(|_| ()),
                    self.cli_overrides.qa_backend.clone().map(|_| ()),
                ),
            ),
            ["workflow", "max_qa_iterations"] => (
                ConfigValue::Integer(self.run_policy.max_qa_iterations as u64),
                source_for_option(
                    self.workspace_config.workflow.max_qa_iterations,
                    self.project_config.workflow.max_qa_iterations,
                    None::<u32>,
                ),
            ),
            ["workflow", "max_review_iterations"] => (
                ConfigValue::Integer(self.run_policy.max_review_iterations as u64),
                source_for_option(
                    self.workspace_config.workflow.max_review_iterations,
                    self.project_config.workflow.max_review_iterations,
                    None::<u32>,
                ),
            ),
            ["workflow", "prompt_change_action"] => (
                ConfigValue::String(Some(
                    self.run_policy.prompt_change_action.as_str().to_owned(),
                )),
                source_for_option(
                    self.workspace_config
                        .workflow
                        .prompt_change_action
                        .map(|_| ()),
                    self.project_config
                        .workflow
                        .prompt_change_action
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["completion", "backends"] => (
                ConfigValue::StringList(
                    self.completion_policy
                        .backends
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
                source_for_option(
                    self.workspace_config
                        .completion
                        .backends
                        .clone()
                        .map(|_| ()),
                    self.project_config.completion.backends.clone().map(|_| ()),
                    None::<()>,
                ),
            ),
            ["completion", "min_completers"] => (
                ConfigValue::Integer(self.completion_policy.min_completers as u64),
                source_for_option(
                    self.workspace_config.completion.min_completers,
                    self.project_config.completion.min_completers,
                    None::<usize>,
                ),
            ),
            ["completion", "consensus_threshold"] => (
                ConfigValue::Float(self.completion_policy.consensus_threshold),
                source_for_option(
                    self.workspace_config
                        .completion
                        .consensus_threshold
                        .map(|_| ()),
                    self.project_config
                        .completion
                        .consensus_threshold
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["final_review", "enabled"] => (
                ConfigValue::Bool(self.final_review_policy.enabled),
                source_for_option(
                    self.workspace_config.final_review.enabled,
                    self.project_config.final_review.enabled,
                    None::<bool>,
                ),
            ),
            ["final_review", "backends"] => (
                ConfigValue::StringList(
                    self.final_review_policy
                        .backends
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                ),
                source_for_option(
                    self.workspace_config
                        .final_review
                        .backends
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .final_review
                        .backends
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["final_review", "arbiter_backend"] => (
                ConfigValue::String(
                    self.final_review_policy
                        .arbiter_backend
                        .as_ref()
                        .map(ToString::to_string),
                ),
                source_for_option(
                    self.workspace_config
                        .final_review
                        .arbiter_backend
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .final_review
                        .arbiter_backend
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["final_review", "min_reviewers"] => (
                ConfigValue::Integer(self.final_review_policy.min_reviewers as u64),
                source_for_option(
                    self.workspace_config.final_review.min_reviewers,
                    self.project_config.final_review.min_reviewers,
                    None::<usize>,
                ),
            ),
            ["final_review", "consensus_threshold"] => (
                ConfigValue::Float(self.final_review_policy.consensus_threshold),
                source_for_option(
                    self.workspace_config
                        .final_review
                        .consensus_threshold
                        .map(|_| ()),
                    self.project_config
                        .final_review
                        .consensus_threshold
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["final_review", "max_restarts"] => (
                ConfigValue::Integer(self.final_review_policy.max_restarts as u64),
                source_for_option(
                    self.workspace_config.final_review.max_restarts,
                    self.project_config.final_review.max_restarts,
                    None::<u32>,
                ),
            ),
            ["validation", "standard_commands"] => (
                ConfigValue::StringList(self.validation_policy.standard_commands.clone()),
                source_for_option(
                    self.workspace_config
                        .validation
                        .standard_commands
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .validation
                        .standard_commands
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["validation", "docs_commands"] => (
                ConfigValue::StringList(self.validation_policy.docs_commands.clone()),
                source_for_option(
                    self.workspace_config
                        .validation
                        .docs_commands
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .validation
                        .docs_commands
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["validation", "ci_commands"] => (
                ConfigValue::StringList(self.validation_policy.ci_commands.clone()),
                source_for_option(
                    self.workspace_config
                        .validation
                        .ci_commands
                        .clone()
                        .map(|_| ()),
                    self.project_config
                        .validation
                        .ci_commands
                        .clone()
                        .map(|_| ()),
                    None::<()>,
                ),
            ),
            ["validation", "pre_commit_fmt"] => (
                ConfigValue::Bool(self.validation_policy.pre_commit_fmt),
                source_for_option(
                    self.workspace_config.validation.pre_commit_fmt,
                    self.project_config.validation.pre_commit_fmt,
                    None::<bool>,
                ),
            ),
            ["validation", "pre_commit_clippy"] => (
                ConfigValue::Bool(self.validation_policy.pre_commit_clippy),
                source_for_option(
                    self.workspace_config.validation.pre_commit_clippy,
                    self.project_config.validation.pre_commit_clippy,
                    None::<bool>,
                ),
            ),
            ["validation", "pre_commit_nix_build"] => (
                ConfigValue::Bool(self.validation_policy.pre_commit_nix_build),
                source_for_option(
                    self.workspace_config.validation.pre_commit_nix_build,
                    self.project_config.validation.pre_commit_nix_build,
                    None::<bool>,
                ),
            ),
            ["validation", "pre_commit_fmt_auto_fix"] => (
                ConfigValue::Bool(self.validation_policy.pre_commit_fmt_auto_fix),
                source_for_option(
                    self.workspace_config.validation.pre_commit_fmt_auto_fix,
                    self.project_config.validation.pre_commit_fmt_auto_fix,
                    None::<bool>,
                ),
            ),
            ["backends", backend_name, field] => {
                let runtime = self.backend_runtime_settings(backend_name)?;
                match *field {
                    "enabled" => (
                        ConfigValue::Bool(
                            runtime
                                .enabled
                                .unwrap_or(default_backend_enabled(backend_name)?),
                        ),
                        source_for_option(
                            self.workspace_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.enabled)
                                .map(|_| ()),
                            self.project_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.enabled)
                                .map(|_| ()),
                            None::<()>,
                        ),
                    ),
                    "command" => (
                        ConfigValue::String(runtime.command.clone()),
                        source_for_option(
                            self.workspace_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.command.clone())
                                .map(|_| ()),
                            self.project_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.command.clone())
                                .map(|_| ()),
                            None::<()>,
                        ),
                    ),
                    "args" => (
                        ConfigValue::StringList(runtime.args.clone().unwrap_or_default()),
                        source_for_option(
                            self.workspace_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.args.clone())
                                .map(|_| ()),
                            self.project_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.args.clone())
                                .map(|_| ()),
                            None::<()>,
                        ),
                    ),
                    "timeout_seconds" => (
                        ConfigValue::Integer(
                            runtime
                                .timeout_seconds
                                .unwrap_or(DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS),
                        ),
                        source_for_option(
                            self.workspace_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.timeout_seconds)
                                .map(|_| ()),
                            self.project_config
                                .backends
                                .get(*backend_name)
                                .and_then(|settings| settings.timeout_seconds)
                                .map(|_| ()),
                            None::<()>,
                        ),
                    ),
                    _ => {
                        return Err(AppError::UnknownConfigKey {
                            key: key.to_owned(),
                        })
                    }
                }
            }
            ["backends", backend_name, "role_models", role_name] => {
                let runtime = self.backend_runtime_settings(backend_name)?;
                let role = role_name.parse::<BackendPolicyRole>()?;
                (
                    ConfigValue::String(runtime.role_models.model_for(role).map(str::to_owned)),
                    source_for_option(
                        workspace_role_model(&self.workspace_config, backend_name, role),
                        project_role_model(&self.project_config, backend_name, role),
                        None::<String>,
                    ),
                )
            }
            ["backends", backend_name, "role_timeouts", role_name] => {
                let runtime = self.backend_runtime_settings(backend_name)?;
                let role = role_name.parse::<BackendPolicyRole>()?;
                (
                    ConfigValue::Integer(
                        runtime.role_timeouts.timeout_for(role).unwrap_or(
                            runtime
                                .timeout_seconds
                                .unwrap_or(DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS),
                        ),
                    ),
                    source_for_option(
                        workspace_role_timeout(&self.workspace_config, backend_name, role),
                        project_role_timeout(&self.project_config, backend_name, role),
                        None::<u64>,
                    ),
                )
            }
            _ => {
                return Err(AppError::UnknownConfigKey {
                    key: key.to_owned(),
                })
            }
        };

        Ok(ConfigEntry {
            key: key.to_owned(),
            value,
            source,
        })
    }

    fn backend_runtime_settings(&self, backend_name: &str) -> AppResult<&BackendRuntimeSettings> {
        self.backend_policy
            .backends
            .get(backend_name)
            .ok_or_else(|| AppError::UnknownConfigKey {
                key: format!("backends.{backend_name}"),
            })
    }
}

fn resolve_scalar<T: Copy>(
    workspace: Option<T>,
    project: Option<T>,
    cli: Option<T>,
    default: T,
) -> T {
    cli.or(project).or(workspace).unwrap_or(default)
}

fn resolve_optional<T>(workspace: Option<T>, project: Option<T>, cli: Option<T>) -> Option<T> {
    cli.or(project).or(workspace)
}

fn resolve_vec<T: Clone>(
    workspace: Option<&[T]>,
    project: Option<&[T]>,
    cli: Option<&[T]>,
    default: Vec<T>,
) -> Vec<T> {
    cli.or(project)
        .or(workspace)
        .map(|values| values.to_vec())
        .unwrap_or(default)
}

fn resolve_backend_selection(
    workspace: Option<&str>,
    project: Option<&str>,
    cli: Option<String>,
) -> AppResult<Option<BackendSelection>> {
    cli.as_deref()
        .or(project)
        .or(workspace)
        .map(BackendSelection::from_backend_name)
        .transpose()
}

fn merge_backend_runtime_map(
    workspace: std::collections::BTreeMap<String, BackendRuntimeSettings>,
    project: std::collections::BTreeMap<String, BackendRuntimeSettings>,
) -> std::collections::BTreeMap<String, BackendRuntimeSettings> {
    let mut merged = std::collections::BTreeMap::new();
    for backend_name in ["claude", "codex", "openrouter"] {
        let defaults = default_backend_runtime_settings(backend_name)
            .expect("default backend runtime settings should be valid");
        let workspace_settings = workspace.get(backend_name);
        let project_settings = project.get(backend_name);
        merged.insert(
            backend_name.to_owned(),
            merge_backend_runtime_settings(defaults, workspace_settings, project_settings),
        );
    }
    for (name, settings) in workspace {
        merged.entry(name).or_insert(settings);
    }
    for (name, settings) in project {
        merged
            .entry(name.clone())
            .and_modify(|existing| {
                *existing = merge_backend_runtime_settings(existing.clone(), None, Some(&settings))
            })
            .or_insert(settings);
    }
    merged
}

fn merge_backend_runtime_settings(
    defaults: BackendRuntimeSettings,
    workspace: Option<&BackendRuntimeSettings>,
    project: Option<&BackendRuntimeSettings>,
) -> BackendRuntimeSettings {
    BackendRuntimeSettings {
        enabled: project
            .and_then(|settings| settings.enabled)
            .or_else(|| workspace.and_then(|settings| settings.enabled))
            .or(defaults.enabled),
        command: project
            .and_then(|settings| settings.command.clone())
            .or_else(|| workspace.and_then(|settings| settings.command.clone()))
            .or(defaults.command),
        args: project
            .and_then(|settings| settings.args.clone())
            .or_else(|| workspace.and_then(|settings| settings.args.clone()))
            .or(defaults.args),
        timeout_seconds: project
            .and_then(|settings| settings.timeout_seconds)
            .or_else(|| workspace.and_then(|settings| settings.timeout_seconds))
            .or(defaults.timeout_seconds),
        role_models: merge_role_models(
            defaults.role_models,
            workspace.map(|settings| &settings.role_models),
            project.map(|settings| &settings.role_models),
        ),
        role_timeouts: merge_role_timeouts(
            defaults.role_timeouts,
            workspace.map(|settings| &settings.role_timeouts),
            project.map(|settings| &settings.role_timeouts),
        ),
        extra: project
            .map(|settings| settings.extra.clone())
            .or_else(|| workspace.map(|settings| settings.extra.clone()))
            .unwrap_or(defaults.extra),
    }
}

fn merge_role_models(
    defaults: BackendRoleModels,
    workspace: Option<&BackendRoleModels>,
    project: Option<&BackendRoleModels>,
) -> BackendRoleModels {
    BackendRoleModels {
        planner: project
            .and_then(|models| models.planner.clone())
            .or_else(|| workspace.and_then(|models| models.planner.clone()))
            .or(defaults.planner),
        implementer: project
            .and_then(|models| models.implementer.clone())
            .or_else(|| workspace.and_then(|models| models.implementer.clone()))
            .or(defaults.implementer),
        reviewer: project
            .and_then(|models| models.reviewer.clone())
            .or_else(|| workspace.and_then(|models| models.reviewer.clone()))
            .or(defaults.reviewer),
        qa: project
            .and_then(|models| models.qa.clone())
            .or_else(|| workspace.and_then(|models| models.qa.clone()))
            .or(defaults.qa),
        completer: project
            .and_then(|models| models.completer.clone())
            .or_else(|| workspace.and_then(|models| models.completer.clone()))
            .or(defaults.completer),
        final_reviewer: project
            .and_then(|models| models.final_reviewer.clone())
            .or_else(|| workspace.and_then(|models| models.final_reviewer.clone()))
            .or(defaults.final_reviewer),
        prompt_reviewer: project
            .and_then(|models| models.prompt_reviewer.clone())
            .or_else(|| workspace.and_then(|models| models.prompt_reviewer.clone()))
            .or(defaults.prompt_reviewer),
        prompt_validator: project
            .and_then(|models| models.prompt_validator.clone())
            .or_else(|| workspace.and_then(|models| models.prompt_validator.clone()))
            .or(defaults.prompt_validator),
        arbiter: project
            .and_then(|models| models.arbiter.clone())
            .or_else(|| workspace.and_then(|models| models.arbiter.clone()))
            .or(defaults.arbiter),
        acceptance_qa: project
            .and_then(|models| models.acceptance_qa.clone())
            .or_else(|| workspace.and_then(|models| models.acceptance_qa.clone()))
            .or(defaults.acceptance_qa),
        extra: project
            .map(|models| models.extra.clone())
            .or_else(|| workspace.map(|models| models.extra.clone()))
            .unwrap_or(defaults.extra),
    }
}

fn merge_role_timeouts(
    defaults: BackendRoleTimeouts,
    workspace: Option<&BackendRoleTimeouts>,
    project: Option<&BackendRoleTimeouts>,
) -> BackendRoleTimeouts {
    BackendRoleTimeouts {
        planner: project
            .and_then(|timeouts| timeouts.planner)
            .or_else(|| workspace.and_then(|timeouts| timeouts.planner))
            .or(defaults.planner),
        implementer: project
            .and_then(|timeouts| timeouts.implementer)
            .or_else(|| workspace.and_then(|timeouts| timeouts.implementer))
            .or(defaults.implementer),
        reviewer: project
            .and_then(|timeouts| timeouts.reviewer)
            .or_else(|| workspace.and_then(|timeouts| timeouts.reviewer))
            .or(defaults.reviewer),
        qa: project
            .and_then(|timeouts| timeouts.qa)
            .or_else(|| workspace.and_then(|timeouts| timeouts.qa))
            .or(defaults.qa),
        completer: project
            .and_then(|timeouts| timeouts.completer)
            .or_else(|| workspace.and_then(|timeouts| timeouts.completer))
            .or(defaults.completer),
        final_reviewer: project
            .and_then(|timeouts| timeouts.final_reviewer)
            .or_else(|| workspace.and_then(|timeouts| timeouts.final_reviewer))
            .or(defaults.final_reviewer),
        prompt_reviewer: project
            .and_then(|timeouts| timeouts.prompt_reviewer)
            .or_else(|| workspace.and_then(|timeouts| timeouts.prompt_reviewer))
            .or(defaults.prompt_reviewer),
        prompt_validator: project
            .and_then(|timeouts| timeouts.prompt_validator)
            .or_else(|| workspace.and_then(|timeouts| timeouts.prompt_validator))
            .or(defaults.prompt_validator),
        arbiter: project
            .and_then(|timeouts| timeouts.arbiter)
            .or_else(|| workspace.and_then(|timeouts| timeouts.arbiter))
            .or(defaults.arbiter),
        acceptance_qa: project
            .and_then(|timeouts| timeouts.acceptance_qa)
            .or_else(|| workspace.and_then(|timeouts| timeouts.acceptance_qa))
            .or(defaults.acceptance_qa),
        extra: project
            .map(|timeouts| timeouts.extra.clone())
            .or_else(|| workspace.map(|timeouts| timeouts.extra.clone()))
            .unwrap_or(defaults.extra),
    }
}

fn default_prompt_review_backends() -> Vec<PanelBackendSpec> {
    vec![
        PanelBackendSpec::required(BackendFamily::Claude),
        PanelBackendSpec::required(BackendFamily::Codex),
    ]
}

fn default_completion_backends() -> Vec<PanelBackendSpec> {
    vec![
        PanelBackendSpec::required(BackendFamily::Claude),
        PanelBackendSpec::required(BackendFamily::Codex),
    ]
}

fn default_final_review_backends() -> Vec<PanelBackendSpec> {
    vec![
        PanelBackendSpec::required(BackendFamily::Claude),
        PanelBackendSpec::required(BackendFamily::Codex),
    ]
}

fn default_backend_enabled(backend_name: &str) -> AppResult<bool> {
    match backend_name {
        "claude" | "codex" => Ok(true),
        "openrouter" => Ok(false),
        _ => Err(AppError::InvalidConfigValue {
            key: "backends".to_owned(),
            value: backend_name.to_owned(),
            reason: "unknown backend name".to_owned(),
        }),
    }
}

fn default_backend_runtime_settings(backend_name: &str) -> AppResult<BackendRuntimeSettings> {
    let command = match backend_name {
        "claude" => "claude",
        "codex" => "codex",
        "openrouter" => "goose",
        _ => {
            return Err(AppError::InvalidConfigValue {
                key: "backends".to_owned(),
                value: backend_name.to_owned(),
                reason: "unknown backend name".to_owned(),
            })
        }
    };

    Ok(BackendRuntimeSettings {
        enabled: Some(default_backend_enabled(backend_name)?),
        command: Some(command.to_owned()),
        args: Some(Vec::new()),
        timeout_seconds: None,
        role_models: BackendRoleModels::default(),
        role_timeouts: BackendRoleTimeouts::default(),
        extra: toml::Table::new(),
    })
}

fn workspace_role_model(
    config: &WorkspaceConfig,
    backend_name: &str,
    role: BackendPolicyRole,
) -> Option<String> {
    config
        .backends
        .get(backend_name)
        .and_then(|settings| settings.role_models.model_for(role))
        .map(str::to_owned)
}

fn project_role_model(
    config: &ProjectConfig,
    backend_name: &str,
    role: BackendPolicyRole,
) -> Option<String> {
    config
        .backends
        .get(backend_name)
        .and_then(|settings| settings.role_models.model_for(role))
        .map(str::to_owned)
}

fn workspace_role_timeout(
    config: &WorkspaceConfig,
    backend_name: &str,
    role: BackendPolicyRole,
) -> Option<u64> {
    config
        .backends
        .get(backend_name)
        .and_then(|settings| settings.role_timeouts.timeout_for(role))
}

fn project_role_timeout(
    config: &ProjectConfig,
    backend_name: &str,
    role: BackendPolicyRole,
) -> Option<u64> {
    config
        .backends
        .get(backend_name)
        .and_then(|settings| settings.role_timeouts.timeout_for(role))
}

fn source_for_option<T>(
    workspace: Option<T>,
    project: Option<T>,
    cli: Option<T>,
) -> ConfigValueSource {
    if cli.is_some() {
        ConfigValueSource::CliOverride
    } else if project.is_some() {
        ConfigValueSource::ProjectToml
    } else if workspace.is_some() {
        ConfigValueSource::WorkspaceToml
    } else {
        ConfigValueSource::Default
    }
}

fn split_key(key: &str) -> AppResult<Vec<&str>> {
    let segments = key.split('.').collect::<Vec<_>>();
    if segments.is_empty() || segments.iter().any(|segment| segment.trim().is_empty()) {
        return Err(AppError::UnknownConfigKey {
            key: key.to_owned(),
        });
    }
    Ok(segments)
}

fn format_float(value: f64) -> String {
    let mut rendered = format!("{value:.4}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.push('0');
    }
    rendered
}

fn is_unset(raw_value: &str) -> bool {
    UNSET_LITERALS
        .iter()
        .any(|candidate| raw_value.trim().eq_ignore_ascii_case(candidate))
}

fn known_config_keys() -> Vec<String> {
    let mut keys = vec![
        "default_flow".to_owned(),
        "default_backend".to_owned(),
        "default_model".to_owned(),
        "prompt_review.enabled".to_owned(),
        "prompt_review.refiner_backend".to_owned(),
        "prompt_review.validator_backends".to_owned(),
        "prompt_review.min_reviewers".to_owned(),
        "workflow.planner_backend".to_owned(),
        "workflow.implementer_backend".to_owned(),
        "workflow.reviewer_backend".to_owned(),
        "workflow.qa_backend".to_owned(),
        "workflow.max_qa_iterations".to_owned(),
        "workflow.max_review_iterations".to_owned(),
        "workflow.prompt_change_action".to_owned(),
        "completion.backends".to_owned(),
        "completion.min_completers".to_owned(),
        "completion.consensus_threshold".to_owned(),
        "final_review.enabled".to_owned(),
        "final_review.backends".to_owned(),
        "final_review.arbiter_backend".to_owned(),
        "final_review.min_reviewers".to_owned(),
        "final_review.consensus_threshold".to_owned(),
        "final_review.max_restarts".to_owned(),
        "validation.standard_commands".to_owned(),
        "validation.docs_commands".to_owned(),
        "validation.ci_commands".to_owned(),
        "validation.pre_commit_fmt".to_owned(),
        "validation.pre_commit_clippy".to_owned(),
        "validation.pre_commit_nix_build".to_owned(),
        "validation.pre_commit_fmt_auto_fix".to_owned(),
    ];

    for backend_name in ["claude", "codex", "openrouter"] {
        keys.push(format!("backends.{backend_name}.enabled"));
        keys.push(format!("backends.{backend_name}.command"));
        keys.push(format!("backends.{backend_name}.args"));
        keys.push(format!("backends.{backend_name}.timeout_seconds"));
        for role in BackendPolicyRole::ALL {
            keys.push(format!(
                "backends.{backend_name}.role_models.{}",
                role.as_str()
            ));
            keys.push(format!(
                "backends.{backend_name}.role_timeouts.{}",
                role.as_str()
            ));
        }
    }

    keys
}

fn apply_to_document(document: &mut DocumentMut, key: &str, raw_value: &str) -> AppResult<()> {
    let segments = split_key(key)?;
    match segments.as_slice() {
        ["default_flow"] => {
            if is_unset(raw_value) {
                document["settings"]["default_flow"] = Item::None;
            } else {
                let parsed = parse_flow_preset(key, raw_value)?;
                document["settings"]["default_flow"] = value(parsed.as_str());
            }
        }
        ["default_backend"] => {
            apply_optional_string(document, &["settings", "default_backend"], raw_value)?
        }
        ["default_model"] => {
            apply_optional_string(document, &["settings", "default_model"], raw_value)?
        }
        ["prompt_review", "enabled"] => {
            apply_optional_bool(document, &["prompt_review", "enabled"], key, raw_value)?
        }
        ["prompt_review", "refiner_backend"] => {
            apply_optional_string(document, &["prompt_review", "refiner_backend"], raw_value)?
        }
        ["prompt_review", "validator_backends"] => apply_string_list(
            document,
            &["prompt_review", "validator_backends"],
            raw_value,
        )?,
        ["prompt_review", "min_reviewers"] => apply_optional_u64(
            document,
            &["prompt_review", "min_reviewers"],
            key,
            raw_value,
        )?,
        ["workflow", "planner_backend"] => {
            apply_optional_string(document, &["workflow", "planner_backend"], raw_value)?
        }
        ["workflow", "implementer_backend"] => {
            apply_optional_string(document, &["workflow", "implementer_backend"], raw_value)?
        }
        ["workflow", "reviewer_backend"] => {
            apply_optional_string(document, &["workflow", "reviewer_backend"], raw_value)?
        }
        ["workflow", "qa_backend"] => {
            apply_optional_string(document, &["workflow", "qa_backend"], raw_value)?
        }
        ["workflow", "max_qa_iterations"] => {
            apply_optional_u64(document, &["workflow", "max_qa_iterations"], key, raw_value)?
        }
        ["workflow", "max_review_iterations"] => apply_optional_u64(
            document,
            &["workflow", "max_review_iterations"],
            key,
            raw_value,
        )?,
        ["workflow", "prompt_change_action"] => {
            if is_unset(raw_value) {
                document["workflow"]["prompt_change_action"] = Item::None;
            } else {
                let parsed = parse_prompt_change_action(key, raw_value)?;
                document["workflow"]["prompt_change_action"] = value(parsed.as_str());
            }
        }
        ["completion", "backends"] => {
            apply_string_list(document, &["completion", "backends"], raw_value)?
        }
        ["completion", "min_completers"] => {
            apply_optional_u64(document, &["completion", "min_completers"], key, raw_value)?
        }
        ["completion", "consensus_threshold"] => apply_optional_float(
            document,
            &["completion", "consensus_threshold"],
            key,
            raw_value,
        )?,
        ["final_review", "enabled"] => {
            apply_optional_bool(document, &["final_review", "enabled"], key, raw_value)?
        }
        ["final_review", "backends"] => {
            apply_string_list(document, &["final_review", "backends"], raw_value)?
        }
        ["final_review", "arbiter_backend"] => {
            apply_optional_string(document, &["final_review", "arbiter_backend"], raw_value)?
        }
        ["final_review", "min_reviewers"] => {
            apply_optional_u64(document, &["final_review", "min_reviewers"], key, raw_value)?
        }
        ["final_review", "consensus_threshold"] => apply_optional_float(
            document,
            &["final_review", "consensus_threshold"],
            key,
            raw_value,
        )?,
        ["final_review", "max_restarts"] => {
            apply_optional_u64(document, &["final_review", "max_restarts"], key, raw_value)?
        }
        ["validation", "standard_commands"] => {
            apply_string_list(document, &["validation", "standard_commands"], raw_value)?
        }
        ["validation", "docs_commands"] => {
            apply_string_list(document, &["validation", "docs_commands"], raw_value)?
        }
        ["validation", "ci_commands"] => {
            apply_string_list(document, &["validation", "ci_commands"], raw_value)?
        }
        ["validation", "pre_commit_fmt"] => {
            apply_optional_bool(document, &["validation", "pre_commit_fmt"], key, raw_value)?
        }
        ["validation", "pre_commit_clippy"] => apply_optional_bool(
            document,
            &["validation", "pre_commit_clippy"],
            key,
            raw_value,
        )?,
        ["validation", "pre_commit_nix_build"] => apply_optional_bool(
            document,
            &["validation", "pre_commit_nix_build"],
            key,
            raw_value,
        )?,
        ["validation", "pre_commit_fmt_auto_fix"] => apply_optional_bool(
            document,
            &["validation", "pre_commit_fmt_auto_fix"],
            key,
            raw_value,
        )?,
        ["backends", backend_name, field] => match *field {
            "enabled" => apply_optional_bool(
                document,
                &["backends", backend_name, "enabled"],
                key,
                raw_value,
            )?,
            "command" => {
                apply_optional_string(document, &["backends", backend_name, "command"], raw_value)?
            }
            "args" => apply_string_list(document, &["backends", backend_name, "args"], raw_value)?,
            "timeout_seconds" => apply_optional_u64(
                document,
                &["backends", backend_name, "timeout_seconds"],
                key,
                raw_value,
            )?,
            _ => {
                return Err(AppError::UnknownConfigKey {
                    key: key.to_owned(),
                })
            }
        },
        ["backends", backend_name, "role_models", role_name] => {
            let role = role_name.parse::<BackendPolicyRole>()?;
            let _ = role;
            apply_optional_string(
                document,
                &["backends", backend_name, "role_models", role_name],
                raw_value,
            )?;
        }
        ["backends", backend_name, "role_timeouts", role_name] => {
            let role = role_name.parse::<BackendPolicyRole>()?;
            let _ = role;
            apply_optional_u64(
                document,
                &["backends", backend_name, "role_timeouts", role_name],
                key,
                raw_value,
            )?;
        }
        _ => {
            return Err(AppError::UnknownConfigKey {
                key: key.to_owned(),
            })
        }
    }

    Ok(())
}

fn apply_optional_string(
    document: &mut DocumentMut,
    path: &[&str],
    raw_value: &str,
) -> AppResult<()> {
    if is_unset(raw_value) {
        set_item(document, path, Item::None);
        return Ok(());
    }

    set_item(document, path, value(raw_value.trim()));
    Ok(())
}

fn apply_optional_bool(
    document: &mut DocumentMut,
    path: &[&str],
    key: &str,
    raw_value: &str,
) -> AppResult<()> {
    if is_unset(raw_value) {
        set_item(document, path, Item::None);
        return Ok(());
    }

    let parsed = parse_bool(key, raw_value)?;
    set_item(document, path, value(parsed));
    Ok(())
}

fn apply_optional_u64(
    document: &mut DocumentMut,
    path: &[&str],
    key: &str,
    raw_value: &str,
) -> AppResult<()> {
    if is_unset(raw_value) {
        set_item(document, path, Item::None);
        return Ok(());
    }

    let parsed = parse_u64(key, raw_value)?;
    set_item(document, path, value(parsed as i64));
    Ok(())
}

fn apply_optional_float(
    document: &mut DocumentMut,
    path: &[&str],
    key: &str,
    raw_value: &str,
) -> AppResult<()> {
    if is_unset(raw_value) {
        set_item(document, path, Item::None);
        return Ok(());
    }

    let parsed = parse_f64(key, raw_value)?;
    set_item(document, path, value(parsed));
    Ok(())
}

fn apply_string_list(document: &mut DocumentMut, path: &[&str], raw_value: &str) -> AppResult<()> {
    if is_unset(raw_value) {
        set_item(document, path, Item::None);
        return Ok(());
    }

    let values = parse_string_list(path.join(".").as_str(), raw_value)?;
    let mut array = Array::default();
    for entry in values {
        array.push(entry.as_str());
    }
    set_item(document, path, value(array));
    Ok(())
}

fn set_item(document: &mut DocumentMut, path: &[&str], item: Item) {
    match path {
        [a] => document[*a] = item,
        [a, b] => document[*a][*b] = item,
        [a, b, c] => document[*a][*b][*c] = item,
        [a, b, c, d] => document[*a][*b][*c][*d] = item,
        _ => unreachable!("unsupported config path depth"),
    }
}

fn parse_bool(key: &str, raw_value: &str) -> AppResult<bool> {
    match raw_value.trim() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected `true` or `false`".to_owned(),
        }),
    }
}

fn parse_u64(key: &str, raw_value: &str) -> AppResult<u64> {
    raw_value
        .trim()
        .parse::<u64>()
        .map_err(|_| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected a non-negative integer".to_owned(),
        })
}

fn parse_f64(key: &str, raw_value: &str) -> AppResult<f64> {
    raw_value
        .trim()
        .parse::<f64>()
        .map_err(|_| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected a floating-point number".to_owned(),
        })
}

fn parse_flow_preset(key: &str, raw_value: &str) -> AppResult<FlowPreset> {
    raw_value
        .trim()
        .parse::<FlowPreset>()
        .map_err(|_| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected a built-in flow preset".to_owned(),
        })
}

fn parse_prompt_change_action(key: &str, raw_value: &str) -> AppResult<PromptChangeAction> {
    raw_value
        .trim()
        .parse::<PromptChangeAction>()
        .map_err(|_| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected continue, abort, or restart_cycle".to_owned(),
        })
}

fn parse_string_list(key: &str, raw_value: &str) -> AppResult<Vec<String>> {
    let parsed =
        raw_value
            .trim()
            .parse::<toml::Value>()
            .map_err(|_| AppError::InvalidConfigValue {
                key: key.to_owned(),
                value: raw_value.to_owned(),
                reason: "expected a TOML array of strings".to_owned(),
            })?;
    let array = parsed
        .as_array()
        .ok_or_else(|| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected a TOML array".to_owned(),
        })?;
    let mut values = Vec::with_capacity(array.len());
    for value in array {
        let string = value.as_str().ok_or_else(|| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "array entries must be strings".to_owned(),
        })?;
        values.push(string.to_owned());
    }
    Ok(values)
}

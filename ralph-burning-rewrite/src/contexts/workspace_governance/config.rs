use std::fmt;
use std::path::Path;

use toml_edit::{value, DocumentMut};

use crate::adapters::fs::FileSystem;
use crate::shared::domain::{FlowPreset, WorkspaceConfig};
use crate::shared::error::{AppError, AppResult};

use super::{load_workspace_config, workspace_config_path};

/// Default: enabled.
pub const DEFAULT_PROMPT_REVIEW_ENABLED: bool = true;
/// Default: standard.
pub const DEFAULT_FLOW_PRESET: FlowPreset = FlowPreset::Standard;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigValueSource {
    Default,
    WorkspaceToml,
}

impl fmt::Display for ConfigValueSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("default"),
            Self::WorkspaceToml => f.write_str("workspace.toml"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValue {
    Bool(bool),
    FlowPreset(FlowPreset),
    String(Option<String>),
}

impl ConfigValue {
    pub fn display_value(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::FlowPreset(value) => value.as_str().to_owned(),
            Self::String(Some(value)) => value.clone(),
            Self::String(None) => "<unset>".to_owned(),
        }
    }

    pub fn toml_like_value(&self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::FlowPreset(value) => format!("\"{}\"", value.as_str()),
            Self::String(Some(value)) => format!("\"{value}\""),
            Self::String(None) => "\"<unset>\"".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub key: &'static str,
    pub value: ConfigValue,
    pub source: ConfigValueSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedValue<T> {
    value: T,
    source: ConfigValueSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveConfig {
    prompt_review_enabled: ResolvedValue<bool>,
    default_flow: ResolvedValue<FlowPreset>,
    default_backend: ResolvedValue<Option<String>>,
    default_model: ResolvedValue<Option<String>>,
}

impl EffectiveConfig {
    pub fn load(base_dir: &Path) -> AppResult<Self> {
        let config = load_workspace_config(base_dir)?;
        Ok(Self::from_workspace_config(&config))
    }

    pub fn get(&self, key: &str) -> AppResult<ConfigEntry> {
        let key = ConfigKey::parse(key)?;
        Ok(self.entry_for(key))
    }

    pub fn set(base_dir: &Path, key: &str, value: &str) -> AppResult<ConfigEntry> {
        let key = ConfigKey::parse(key)?;
        let _ = Self::load(base_dir)?;

        let config_path = workspace_config_path(base_dir);
        let raw = FileSystem::read_to_string(&config_path)?;
        let mut document = raw.parse::<DocumentMut>()?;
        key.apply_to_document(&mut document, value)?;
        FileSystem::write_atomic(&config_path, &document.to_string())?;

        Self::load(base_dir)?.get(key.as_str())
    }

    pub fn entries(&self) -> Vec<ConfigEntry> {
        ConfigKey::ALL
            .iter()
            .copied()
            .map(|key| self.entry_for(key))
            .collect()
    }

    /// Default: `true`.
    pub fn prompt_review_enabled(&self) -> bool {
        self.prompt_review_enabled.value
    }

    /// Default: `standard`.
    pub fn default_flow(&self) -> FlowPreset {
        self.default_flow.value
    }

    /// Default: unset.
    pub fn default_backend(&self) -> Option<&str> {
        self.default_backend.value.as_deref()
    }

    /// Default: unset.
    pub fn default_model(&self) -> Option<&str> {
        self.default_model.value.as_deref()
    }

    fn from_workspace_config(config: &WorkspaceConfig) -> Self {
        let settings = &config.settings;
        Self {
            prompt_review_enabled: resolve_value(
                settings.prompt_review.enabled,
                DEFAULT_PROMPT_REVIEW_ENABLED,
            ),
            default_flow: resolve_value(settings.default_flow, DEFAULT_FLOW_PRESET),
            default_backend: resolve_optional_value(settings.default_backend.as_ref()),
            default_model: resolve_optional_value(settings.default_model.as_ref()),
        }
    }

    fn entry_for(&self, key: ConfigKey) -> ConfigEntry {
        match key {
            ConfigKey::PromptReviewEnabled => ConfigEntry {
                key: key.as_str(),
                value: ConfigValue::Bool(self.prompt_review_enabled()),
                source: self.prompt_review_enabled.source,
            },
            ConfigKey::DefaultFlow => ConfigEntry {
                key: key.as_str(),
                value: ConfigValue::FlowPreset(self.default_flow()),
                source: self.default_flow.source,
            },
            ConfigKey::DefaultBackend => ConfigEntry {
                key: key.as_str(),
                value: ConfigValue::String(self.default_backend.value.clone()),
                source: self.default_backend.source,
            },
            ConfigKey::DefaultModel => ConfigEntry {
                key: key.as_str(),
                value: ConfigValue::String(self.default_model.value.clone()),
                source: self.default_model.source,
            },
        }
    }
}

fn resolve_value<T: Copy>(value: Option<T>, default: T) -> ResolvedValue<T> {
    match value {
        Some(value) => ResolvedValue {
            value,
            source: ConfigValueSource::WorkspaceToml,
        },
        None => ResolvedValue {
            value: default,
            source: ConfigValueSource::Default,
        },
    }
}

fn resolve_optional_value(value: Option<&String>) -> ResolvedValue<Option<String>> {
    match value {
        Some(value) => ResolvedValue {
            value: Some(value.clone()),
            source: ConfigValueSource::WorkspaceToml,
        },
        None => ResolvedValue {
            value: None,
            source: ConfigValueSource::Default,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigKey {
    PromptReviewEnabled,
    DefaultFlow,
    DefaultBackend,
    DefaultModel,
}

impl ConfigKey {
    const ALL: [Self; 4] = [
        Self::PromptReviewEnabled,
        Self::DefaultFlow,
        Self::DefaultBackend,
        Self::DefaultModel,
    ];

    fn parse(value: &str) -> AppResult<Self> {
        match value {
            "prompt_review.enabled" => Ok(Self::PromptReviewEnabled),
            "default_flow" => Ok(Self::DefaultFlow),
            "default_backend" => Ok(Self::DefaultBackend),
            "default_model" => Ok(Self::DefaultModel),
            _ => Err(AppError::UnknownConfigKey {
                key: value.to_owned(),
            }),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::PromptReviewEnabled => "prompt_review.enabled",
            Self::DefaultFlow => "default_flow",
            Self::DefaultBackend => "default_backend",
            Self::DefaultModel => "default_model",
        }
    }

    fn apply_to_document(self, document: &mut DocumentMut, raw_value: &str) -> AppResult<()> {
        match self {
            Self::PromptReviewEnabled => {
                let parsed = parse_bool(self.as_str(), raw_value)?;
                document["settings"]["prompt_review"]["enabled"] = value(parsed);
            }
            Self::DefaultFlow => {
                let parsed = parse_flow_preset(self.as_str(), raw_value)?;
                document["settings"]["default_flow"] = value(parsed.as_str());
            }
            Self::DefaultBackend => apply_optional_string(document, "default_backend", raw_value)?,
            Self::DefaultModel => apply_optional_string(document, "default_model", raw_value)?,
        }

        Ok(())
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

fn parse_flow_preset(key: &str, raw_value: &str) -> AppResult<FlowPreset> {
    raw_value
        .trim()
        .parse::<FlowPreset>()
        .map_err(|_| AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected one of: standard, quick_dev, docs_change, ci_improvement".to_owned(),
        })
}

fn parse_optional_string(key: &str, raw_value: &str) -> AppResult<Option<String>> {
    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: raw_value.to_owned(),
            reason: "expected a non-empty string or one of: unset, none, null".to_owned(),
        });
    }

    if matches!(trimmed, "unset" | "none" | "null") {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_owned()))
    }
}

fn apply_optional_string(
    document: &mut DocumentMut,
    setting_name: &str,
    raw_value: &str,
) -> AppResult<()> {
    match parse_optional_string(setting_name, raw_value)? {
        Some(parsed) => {
            document["settings"][setting_name] = value(parsed);
        }
        None => {
            if let Some(settings) = document
                .as_table_mut()
                .get_mut("settings")
                .and_then(|item| item.as_table_mut())
            {
                settings.remove(setting_name);
            }
        }
    }

    Ok(())
}

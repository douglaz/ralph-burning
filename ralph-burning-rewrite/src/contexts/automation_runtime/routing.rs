use crate::contexts::automation_runtime::model::{RoutingResolution, RoutingSource};
use crate::shared::domain::FlowPreset;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, Default)]
pub struct RoutingEngine;

impl RoutingEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn resolve_flow(
        &self,
        routing_command: Option<&str>,
        routing_labels: &[String],
        default_flow: FlowPreset,
    ) -> AppResult<RoutingResolution> {
        if let Some(command) = routing_command.filter(|value| !value.trim().is_empty()) {
            let flow = self.parse_command(command)?;
            return Ok(RoutingResolution {
                flow,
                source: RoutingSource::Command,
                warnings: Vec::new(),
            });
        }

        let (labels, warnings) = self.parse_labels(routing_labels)?;
        if let Some(flow) = labels.first().copied() {
            return Ok(RoutingResolution {
                flow,
                source: RoutingSource::Label,
                warnings,
            });
        }

        Ok(RoutingResolution {
            flow: default_flow,
            source: RoutingSource::DefaultFlow,
            warnings,
        })
    }

    pub fn parse_command(&self, command: &str) -> AppResult<FlowPreset> {
        let trimmed = command.trim();
        if trimmed.is_empty() {
            return Err(AppError::RoutingResolutionFailed {
                input: command.to_owned(),
                details: "routing command cannot be empty".to_owned(),
            });
        }

        if let Ok(flow) = trimmed.parse::<FlowPreset>() {
            return Ok(flow);
        }

        let tokens: Vec<_> = trimmed.split_whitespace().collect();
        if tokens.len() == 3 && matches!(tokens[0], "/rb" | "rb") && tokens[1] == "flow" {
            return tokens[2].parse::<FlowPreset>().map_err(|_| {
                AppError::RoutingResolutionFailed {
                    input: trimmed.to_owned(),
                    details: format!("unknown flow preset '{}'", tokens[2]),
                }
            });
        }

        Err(AppError::RoutingResolutionFailed {
            input: trimmed.to_owned(),
            details: "expected `<preset>` or `/rb flow <preset>`".to_owned(),
        })
    }

    pub fn parse_labels(&self, labels: &[String]) -> AppResult<(Vec<FlowPreset>, Vec<String>)> {
        let mut flows = Vec::new();
        let mut warnings = Vec::new();

        for label in labels {
            let trimmed = label.trim();
            if trimmed.is_empty() {
                continue;
            }

            if trimmed.starts_with("rb:flow:") {
                let preset = trimmed.trim_start_matches("rb:flow:");
                if preset.is_empty() {
                    warnings.push(format!("ignored malformed routing label '{trimmed}'"));
                    continue;
                }

                let flow = preset.parse::<FlowPreset>().map_err(|_| {
                    AppError::RoutingResolutionFailed {
                        input: trimmed.to_owned(),
                        details: format!("unknown flow preset '{preset}'"),
                    }
                })?;
                flows.push(flow);
                continue;
            }

            if trimmed.starts_with("rb:flow") {
                warnings.push(format!("ignored malformed routing label '{trimmed}'"));
            }
        }

        flows.sort_by_key(|flow| flow.as_str());
        flows.dedup();

        if flows.len() > 1 {
            return Err(AppError::AmbiguousRouting {
                labels: flows
                    .into_iter()
                    .map(|flow| flow.as_str().to_owned())
                    .collect(),
            });
        }

        Ok((flows, warnings))
    }
}

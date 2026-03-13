use std::collections::HashSet;
use std::path::Path;

use crate::shared::error::{AppError, AppResult};

use super::model::{IdSource, ScenarioKind, ScenarioMeta};

/// Compile-time path to the conformance feature directory.
const FEATURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/conformance/features");

/// Returns the path to the conformance features directory.
pub fn features_dir() -> &'static Path {
    Path::new(FEATURES_DIR)
}

/// Discover all conformance scenarios from `.feature` files.
///
/// Returns scenarios in deterministic order: feature files sorted by path,
/// then scenarios in file order.
pub fn discover_scenarios() -> AppResult<Vec<ScenarioMeta>> {
    discover_scenarios_from(features_dir())
}

/// Discover scenarios from a specific features directory (testable).
pub fn discover_scenarios_from(dir: &Path) -> AppResult<Vec<ScenarioMeta>> {
    if !dir.is_dir() {
        return Err(AppError::ConformanceDiscoveryFailed {
            details: format!("features directory not found: {}", dir.display()),
        });
    }

    let mut feature_files: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| AppError::ConformanceDiscoveryFailed {
            details: format!("cannot read features directory: {e}"),
        })?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "feature") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    feature_files.sort();

    let mut all_scenarios = Vec::new();

    for file_path in &feature_files {
        let content = std::fs::read_to_string(file_path).map_err(|e| {
            AppError::ConformanceDiscoveryFailed {
                details: format!("cannot read {}: {e}", file_path.display()),
            }
        })?;
        let file_name = file_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let scenarios = parse_feature_file(&content, &file_name)?;
        all_scenarios.extend(scenarios);
    }

    if all_scenarios.is_empty() {
        return Err(AppError::ConformanceDiscoveryFailed {
            details: "no scenarios discovered from feature files".to_owned(),
        });
    }

    Ok(all_scenarios)
}

/// Validate that all discovered scenario IDs are unique and well-formed.
pub fn validate_ids(scenarios: &[ScenarioMeta]) -> AppResult<()> {
    let mut seen = HashSet::new();

    for scenario in scenarios {
        // Validate ID format: non-empty, no whitespace
        if scenario.id.is_empty() {
            return Err(AppError::ConformanceParseFailed {
                file: scenario.source_file.clone(),
                line: scenario.source_line,
                details: "scenario ID is empty".to_owned(),
            });
        }

        if scenario.id.chars().any(|c| c.is_whitespace()) {
            return Err(AppError::ConformanceParseFailed {
                file: scenario.source_file.clone(),
                line: scenario.source_line,
                details: format!("scenario ID '{}' contains whitespace", scenario.id),
            });
        }

        // Check for duplicates
        if !seen.insert(&scenario.id) {
            return Err(AppError::ConformanceParseFailed {
                file: scenario.source_file.clone(),
                line: scenario.source_line,
                details: format!("duplicate scenario ID '{}'", scenario.id),
            });
        }
    }

    Ok(())
}

/// Parse a single `.feature` file and extract scenario metadata.
fn parse_feature_file(content: &str, file_name: &str) -> AppResult<Vec<ScenarioMeta>> {
    let lines: Vec<&str> = content.lines().collect();
    let mut scenarios = Vec::new();
    let mut feature_title = String::new();

    // Find the Feature: line
    for line in &lines {
        let trimmed = line.trim();
        if let Some(title) = trimmed.strip_prefix("Feature:") {
            feature_title = title.trim().to_owned();
            break;
        }
    }

    if feature_title.is_empty() {
        return Err(AppError::ConformanceParseFailed {
            file: file_name.to_owned(),
            line: 1,
            details: "no Feature: line found".to_owned(),
        });
    }

    // Scan for Scenario: and Scenario Outline: lines
    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        let line_number = idx + 1; // 1-indexed

        let (kind, title) = if let Some(rest) = trimmed.strip_prefix("Scenario Outline:") {
            (ScenarioKind::ScenarioOutline, rest.trim().to_owned())
        } else if let Some(rest) = trimmed.strip_prefix("Scenario:") {
            (ScenarioKind::Scenario, rest.trim().to_owned())
        } else {
            continue;
        };

        // Extract ID from preceding line(s)
        let (id, id_source) = extract_scenario_id(&lines, idx).ok_or_else(|| {
            AppError::ConformanceParseFailed {
                file: file_name.to_owned(),
                line: line_number,
                details: format!(
                    "scenario '{}' has no ID (expected a # <id> comment or @<tag> on preceding line)",
                    title
                ),
            }
        })?;

        scenarios.push(ScenarioMeta {
            id,
            feature_title: feature_title.clone(),
            scenario_title: title,
            source_file: file_name.to_owned(),
            source_line: line_number,
            kind,
            id_source,
        });
    }

    Ok(scenarios)
}

/// Extract a scenario ID from the line(s) preceding a Scenario/Scenario Outline.
///
/// Preference order:
/// 1. A `# <scenario-id>` comment on the immediately preceding line
/// 2. A scenario-level `@<tag>` on the immediately preceding line
///
/// Feature-level tags (before the Feature: line) are NOT used as scenario IDs.
fn extract_scenario_id(lines: &[&str], scenario_line_idx: usize) -> Option<(String, IdSource)> {
    if scenario_line_idx == 0 {
        return None;
    }

    let prev_line = lines[scenario_line_idx - 1].trim();

    // Check for `# <scenario-id>` comment
    if let Some(rest) = prev_line.strip_prefix('#') {
        let id = rest.trim();
        if !id.is_empty() && !id.contains(' ') {
            return Some((id.to_owned(), IdSource::Comment));
        }
    }

    // Check for `@<tag>` (scenario-level tag)
    if let Some(rest) = prev_line.strip_prefix('@') {
        let tag = rest.trim();
        if !tag.is_empty() && !tag.contains(' ') {
            return Some((tag.to_owned(), IdSource::Tag));
        }
    }

    None
}

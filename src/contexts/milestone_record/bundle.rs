#![forbid(unsafe_code)]

//! MilestoneBundle: the versioned contract for milestone planning output.
//!
//! A MilestoneBundle is the machine-readable and human-readable output of the
//! milestone planner. Downstream consumers use this bundle to:
//!
//! 1. Persist `plan.json` and `plan.md` for the milestone.
//! 2. Export bead proposals into the `.beads/` graph.
//! 3. Determine the default execution flow for each bead.
//! 4. Generate AGENTS.md seed material for bead execution prompts.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::model::MilestoneId;
use crate::shared::domain::FlowPreset;

/// Current MilestoneBundle schema version.
pub const MILESTONE_BUNDLE_VERSION: u32 = 1;

// ── Top-level bundle ────────────────────────────────────────────────────────

/// The complete output of milestone planning. Versioned for forward
/// compatibility; consumers must check `schema_version` before processing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MilestoneBundle {
    /// Schema version for forward compatibility.
    pub schema_version: u32,

    /// Milestone identity.
    pub identity: MilestoneIdentity,

    /// Executive summary of what this milestone delivers.
    pub executive_summary: String,

    /// Concrete goals that define success.
    pub goals: Vec<String>,

    /// Explicit non-goals to prevent scope creep.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_goals: Vec<String>,

    /// Constraints and assumptions that shape the plan.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub constraints: Vec<String>,

    /// Map from acceptance criterion ID to its description.
    pub acceptance_map: Vec<AcceptanceCriterion>,

    /// Workstreams or epics that organize the beads.
    pub workstreams: Vec<Workstream>,

    /// Recommended default flow preset for bead execution.
    pub default_flow: FlowPreset,

    /// Optional AGENTS.md seed material for bead execution context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents_guidance: Option<String>,
}

impl MilestoneBundle {
    pub fn new(identity: MilestoneIdentity, executive_summary: String) -> Self {
        Self {
            schema_version: MILESTONE_BUNDLE_VERSION,
            identity,
            executive_summary,
            goals: Vec::new(),
            non_goals: Vec::new(),
            constraints: Vec::new(),
            acceptance_map: Vec::new(),
            workstreams: Vec::new(),
            default_flow: FlowPreset::QuickDev,
            agents_guidance: None,
        }
    }

    /// Validate the bundle's internal consistency.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        let milestone_id = self.identity.id.trim();

        if self.schema_version != MILESTONE_BUNDLE_VERSION {
            errors.push(format!(
                "unsupported schema_version {} (expected {})",
                self.schema_version, MILESTONE_BUNDLE_VERSION
            ));
        }
        if self.identity.id != milestone_id {
            errors.push("identity.id must not contain leading or trailing whitespace".to_owned());
        } else if let Err(error) = MilestoneId::new(self.identity.id.clone()) {
            errors.push(format!("identity.id is invalid: {error}"));
        }
        if self.identity.name.trim().is_empty() {
            errors.push("identity.name must not be empty".to_owned());
        }
        if self.executive_summary.trim().is_empty() {
            errors.push("executive_summary must not be empty".to_owned());
        }
        if self.goals.is_empty() {
            errors.push("goals must contain at least one item".to_owned());
        }
        if self.acceptance_map.is_empty() {
            errors.push("acceptance_map must contain at least one criterion".to_owned());
        }
        if self.workstreams.is_empty() {
            errors.push("workstreams must contain at least one workstream".to_owned());
        }

        let mut acceptance_ids = BTreeSet::new();
        for (i, ac) in self.acceptance_map.iter().enumerate() {
            if ac.id.trim().is_empty() {
                errors.push(format!("acceptance_map[{i}].id must not be empty"));
            } else if !acceptance_ids.insert(ac.id.trim().to_owned()) {
                errors.push(format!(
                    "acceptance_map[{i}].id '{}' is duplicate",
                    ac.id.trim()
                ));
            }
            if ac.description.trim().is_empty() {
                errors.push(format!("acceptance_map[{i}].description must not be empty"));
            }
        }

        let mut bead_ids = BTreeMap::new();
        let mut bead_locations = BTreeMap::new();
        let mut title_only_titles = BTreeMap::new();
        let mut next_implicit_bead = 1usize;
        for (i, ws) in self.workstreams.iter().enumerate() {
            if ws.name.trim().is_empty() {
                errors.push(format!("workstreams[{i}].name must not be empty"));
            }
            if ws.beads.is_empty() {
                errors.push(format!(
                    "workstreams[{i}].beads must contain at least one bead"
                ));
            }
            for (j, bead) in ws.beads.iter().enumerate() {
                let location = format!("workstreams[{i}].beads[{j}]");
                if bead.title.trim().is_empty() {
                    errors.push(format!("{location}.title must not be empty"));
                }

                let implicit_bead_id = format!("bead-{next_implicit_bead}");
                next_implicit_bead += 1;

                let canonical_bead_id = if let Some(bead_id) = bead.bead_id.as_deref() {
                    match normalize_bead_reference(milestone_id, bead_id) {
                        Ok(canonical) => canonical,
                        Err(reason) => {
                            errors.push(format!("{location}.bead_id {reason}"));
                            implicit_bead_id
                        }
                    }
                } else {
                    if !bead.title.trim().is_empty() {
                        if let Some(previous) =
                            title_only_titles.insert(bead.title.trim().to_owned(), location.clone())
                        {
                            errors.push(format!(
                                "{location}.title '{}' duplicates title-only proposal from {}",
                                bead.title.trim(),
                                previous
                            ));
                        }
                    }
                    implicit_bead_id
                };

                if let Some(previous) = bead_ids.insert(canonical_bead_id.clone(), location.clone())
                {
                    errors.push(format!(
                        "{location} resolves to duplicate bead identifier '{}' already used by {}",
                        canonical_bead_id, previous
                    ));
                }
                bead_locations.insert((i, j), canonical_bead_id);
            }
        }

        for (i, ws) in self.workstreams.iter().enumerate() {
            for (j, bead) in ws.beads.iter().enumerate() {
                let location = format!("workstreams[{i}].beads[{j}]");
                let canonical_bead_id = bead_locations
                    .get(&(i, j))
                    .cloned()
                    .expect("canonical bead id collected during first validation pass");

                for (k, depends_on) in bead.depends_on.iter().enumerate() {
                    match normalize_bead_reference(milestone_id, depends_on) {
                        Ok(reference) => {
                            if reference == canonical_bead_id {
                                errors.push(format!(
                                    "{location}.depends_on[{k}] must not reference the bead itself"
                                ));
                            } else if !bead_ids.contains_key(&reference) {
                                errors.push(format!(
                                    "{location}.depends_on[{k}] references unknown bead '{}'",
                                    depends_on.trim()
                                ));
                            }
                        }
                        Err(reason) => {
                            errors.push(format!("{location}.depends_on[{k}] {reason}"));
                        }
                    }
                }

                for (k, acceptance_id) in bead.acceptance_criteria.iter().enumerate() {
                    if acceptance_id.trim().is_empty() {
                        errors.push(format!(
                            "{location}.acceptance_criteria[{k}] must not be empty"
                        ));
                    } else if !acceptance_ids.contains(acceptance_id.trim()) {
                        errors.push(format!(
                            "{location}.acceptance_criteria[{k}] references unknown acceptance criterion '{}'",
                            acceptance_id.trim()
                        ));
                    }
                }
            }
        }

        for (i, ac) in self.acceptance_map.iter().enumerate() {
            for (j, covered_by) in ac.covered_by.iter().enumerate() {
                match normalize_bead_reference(milestone_id, covered_by) {
                    Ok(reference) => {
                        if !bead_ids.contains_key(&reference) {
                            errors.push(format!(
                                "acceptance_map[{i}].covered_by[{j}] references unknown bead '{}'",
                                covered_by.trim()
                            ));
                        }
                    }
                    Err(reason) => {
                        errors.push(format!("acceptance_map[{i}].covered_by[{j}] {reason}"));
                    }
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Collect all bead proposals across all workstreams.
    pub fn all_beads(&self) -> Vec<&BeadProposal> {
        self.workstreams.iter().flat_map(|ws| &ws.beads).collect()
    }

    /// Total count of proposed beads.
    pub fn bead_count(&self) -> usize {
        self.workstreams.iter().map(|ws| ws.beads.len()).sum()
    }
}

pub(crate) fn progress_shape_signature(bundle: &MilestoneBundle) -> Result<String, Vec<String>> {
    #[derive(Serialize)]
    struct ProgressShapeSignature {
        explicit_bead_ids: BTreeSet<String>,
        implicit_beads: Vec<ImplicitBeadSignature>,
    }

    #[derive(Serialize)]
    struct ImplicitBeadSignature {
        workstream_name: String,
        workstream_description: Option<String>,
        title: String,
        description: Option<String>,
        bead_type: Option<String>,
        priority: Option<u32>,
        labels: Vec<String>,
        depends_on: Vec<String>,
        acceptance_criteria: Vec<String>,
        flow_override: Option<FlowPreset>,
    }

    let mut errors = Vec::new();
    let milestone_id = bundle.identity.id.trim();
    let mut explicit_bead_ids = BTreeSet::new();
    let mut implicit_beads = Vec::new();

    for (workstream_index, workstream) in bundle.workstreams.iter().enumerate() {
        for (bead_index, bead) in workstream.beads.iter().enumerate() {
            let location = format!("workstreams[{workstream_index}].beads[{bead_index}]");
            if let Some(bead_id) = bead.bead_id.as_deref() {
                match normalize_bead_reference(milestone_id, bead_id) {
                    Ok(canonical) => {
                        if !explicit_bead_ids.insert(canonical.clone()) {
                            errors.push(format!(
                                "{location} resolves to duplicate bead identifier '{}'",
                                canonical
                            ));
                        }
                    }
                    Err(reason) => errors.push(format!("{location}.bead_id {reason}")),
                }
                continue;
            }

            let mut depends_on = Vec::with_capacity(bead.depends_on.len());
            for (dependency_index, depends_on_ref) in bead.depends_on.iter().enumerate() {
                match normalize_bead_reference(milestone_id, depends_on_ref) {
                    Ok(reference) => depends_on.push(reference),
                    Err(reason) => errors.push(format!(
                        "{location}.depends_on[{dependency_index}] {reason}"
                    )),
                }
            }

            implicit_beads.push(ImplicitBeadSignature {
                workstream_name: workstream.name.clone(),
                workstream_description: workstream.description.clone(),
                title: bead.title.clone(),
                description: bead.description.clone(),
                bead_type: bead.bead_type.clone(),
                priority: bead.priority,
                labels: bead.labels.clone(),
                depends_on,
                acceptance_criteria: bead.acceptance_criteria.clone(),
                flow_override: bead.flow_override,
            });
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    serde_json::to_string(&ProgressShapeSignature {
        explicit_bead_ids,
        implicit_beads,
    })
    .map_err(|error| {
        vec![format!(
            "failed to serialize progress shape signature: {error}"
        )]
    })
}

fn normalize_bead_reference(milestone_id: &str, raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if raw != trimmed {
        return Err("must not contain leading or trailing whitespace".to_owned());
    }
    if trimmed.is_empty() {
        return Err("must not be empty".to_owned());
    }
    if trimmed.starts_with('.') || trimmed.contains('/') || trimmed.contains('\\') {
        return Err(format!("'{}' is not a valid bead identifier", trimmed));
    }

    let qualified_prefix = format!("{milestone_id}.");
    if let Some(suffix) = trimmed.strip_prefix(&qualified_prefix) {
        if suffix.is_empty() {
            return Err(format!("'{}' is not a valid bead identifier", trimmed));
        }
        return Ok(suffix.to_owned());
    }

    if trimmed.contains('.') {
        return Err(format!(
            "'{}' does not belong to milestone '{}'",
            trimmed, milestone_id
        ));
    }

    Ok(trimmed.to_owned())
}

// ── Sub-types ───────────────────────────────────────────────────────────────

/// Milestone identity within the bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MilestoneIdentity {
    /// Machine-readable milestone ID (matches MilestoneId).
    pub id: String,
    /// Human-readable milestone name.
    pub name: String,
}

/// A single acceptance criterion for the milestone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AcceptanceCriterion {
    /// Short identifier (e.g., "AC-1").
    pub id: String,
    /// What must be true for this criterion to be met.
    pub description: String,
    /// Bead IDs that contribute to meeting this criterion.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub covered_by: Vec<String>,
}

/// A logical grouping of related beads (epic/workstream).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Workstream {
    /// Human-readable workstream name.
    pub name: String,
    /// Optional description of the workstream's purpose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Ordered list of bead proposals in this workstream.
    pub beads: Vec<BeadProposal>,
}

/// A proposed bead for execution, with dependency hints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct BeadProposal {
    /// Optional stable bead ID or suffix (for example `bead-2`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_id: Option<String>,
    /// Proposed bead title (used as br title).
    pub title: String,
    /// Detailed description of what the bead should accomplish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Suggested bead type (task, feature, bug, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_type: Option<String>,
    /// Suggested priority (1=P1, 2=P2, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    /// Labels to apply to the bead.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    /// IDs of beads this one depends on (must complete first).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Acceptance criterion IDs this bead contributes to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance_criteria: Vec<String>,
    /// Suggested flow preset override for this bead (defaults to milestone's default_flow).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_override: Option<FlowPreset>,
}

// ── Renderers ───────────────────────────────────────────────────────────────

/// Render a MilestoneBundle as deterministic `plan.md` content.
pub fn render_plan_md(bundle: &MilestoneBundle) -> String {
    let mut out = String::new();

    writeln!(out, "# {}", bundle.identity.name).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Executive Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", bundle.executive_summary).unwrap();
    writeln!(out).unwrap();

    // Goals
    writeln!(out, "## Goals").unwrap();
    writeln!(out).unwrap();
    for goal in &bundle.goals {
        writeln!(out, "- {goal}").unwrap();
    }
    writeln!(out).unwrap();

    // Non-goals
    if !bundle.non_goals.is_empty() {
        writeln!(out, "## Non-Goals").unwrap();
        writeln!(out).unwrap();
        for ng in &bundle.non_goals {
            writeln!(out, "- {ng}").unwrap();
        }
        writeln!(out).unwrap();
    }

    // Constraints
    if !bundle.constraints.is_empty() {
        writeln!(out, "## Constraints & Assumptions").unwrap();
        writeln!(out).unwrap();
        for constraint in &bundle.constraints {
            writeln!(out, "- {constraint}").unwrap();
        }
        writeln!(out).unwrap();
    }

    // Acceptance map
    writeln!(out, "## Acceptance Criteria").unwrap();
    writeln!(out).unwrap();
    for ac in &bundle.acceptance_map {
        writeln!(out, "- **{}**: {}", ac.id, ac.description).unwrap();
        if !ac.covered_by.is_empty() {
            writeln!(out, "  - Covered by: {}", ac.covered_by.join(", ")).unwrap();
        }
    }
    writeln!(out).unwrap();

    // Workstreams
    writeln!(out, "## Workstreams").unwrap();
    writeln!(out).unwrap();
    for ws in &bundle.workstreams {
        writeln!(out, "### {}", ws.name).unwrap();
        writeln!(out).unwrap();
        if let Some(desc) = &ws.description {
            writeln!(out, "{desc}").unwrap();
            writeln!(out).unwrap();
        }
        writeln!(out, "| # | Title | Type | Priority | Dependencies |").unwrap();
        writeln!(out, "|---|-------|------|----------|--------------|").unwrap();
        for (i, bead) in ws.beads.iter().enumerate() {
            let bead_type = bead.bead_type.as_deref().unwrap_or("task");
            let priority = bead
                .priority
                .map(|p| format!("P{p}"))
                .unwrap_or_else(|| "-".to_owned());
            let deps = if bead.depends_on.is_empty() {
                "-".to_owned()
            } else {
                bead.depends_on.join(", ")
            };
            writeln!(
                out,
                "| {} | {} | {} | {} | {} |",
                i + 1,
                bead.title,
                bead_type,
                priority,
                deps
            )
            .unwrap();
        }
        writeln!(out).unwrap();
    }

    // Execution defaults
    writeln!(out, "## Execution Defaults").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- **Default flow:** {}", bundle.default_flow).unwrap();
    writeln!(out, "- **Total beads:** {}", bundle.bead_count()).unwrap();
    writeln!(out).unwrap();

    // AGENTS guidance
    if let Some(guidance) = &bundle.agents_guidance {
        writeln!(out, "## AGENTS Guidance").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "{guidance}").unwrap();
    }

    out
}

/// Render a MilestoneBundle as deterministic `plan.json` content.
pub fn render_plan_json(bundle: &MilestoneBundle) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(bundle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_bundle() -> MilestoneBundle {
        MilestoneBundle {
            schema_version: MILESTONE_BUNDLE_VERSION,
            identity: MilestoneIdentity {
                id: "ms-alpha".to_owned(),
                name: "Alpha Milestone".to_owned(),
            },
            executive_summary: "Deliver the alpha release with core features.".to_owned(),
            goals: vec![
                "Ship core feature A".to_owned(),
                "Pass acceptance tests".to_owned(),
            ],
            non_goals: vec!["Performance optimization".to_owned()],
            constraints: vec!["Must use existing database schema".to_owned()],
            acceptance_map: vec![
                AcceptanceCriterion {
                    id: "AC-1".to_owned(),
                    description: "Feature A works end-to-end".to_owned(),
                    covered_by: vec!["bead-1".to_owned(), "bead-2".to_owned()],
                },
                AcceptanceCriterion {
                    id: "AC-2".to_owned(),
                    description: "All tests pass".to_owned(),
                    covered_by: vec!["bead-3".to_owned()],
                },
            ],
            workstreams: vec![Workstream {
                name: "Core Feature".to_owned(),
                description: Some("Implement the core feature set.".to_owned()),
                beads: vec![
                    BeadProposal {
                        bead_id: None,
                        title: "Implement data model".to_owned(),
                        description: Some("Define schema and types".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["backend".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: None,
                        title: "Build API endpoints".to_owned(),
                        description: Some("REST API for feature A".to_owned()),
                        bead_type: Some("feature".to_owned()),
                        priority: Some(1),
                        labels: vec!["backend".to_owned(), "api".to_owned()],
                        depends_on: vec!["bead-1".to_owned()],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: None,
                        title: "Write integration tests".to_owned(),
                        description: None,
                        bead_type: Some("task".to_owned()),
                        priority: Some(2),
                        labels: vec!["testing".to_owned()],
                        depends_on: vec!["bead-2".to_owned()],
                        acceptance_criteria: vec!["AC-2".to_owned()],
                        flow_override: None,
                    },
                ],
            }],
            default_flow: FlowPreset::QuickDev,
            agents_guidance: Some("Follow existing patterns in src/contexts/.".to_owned()),
        }
    }

    #[test]
    fn bundle_validates_successfully() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        bundle.validate().map_err(|e| e.join("; "))?;
        Ok(())
    }

    #[test]
    fn bundle_validation_catches_empty_goals() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.goals.clear();
        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("goals")));
        Ok(())
    }

    #[test]
    fn bundle_validation_catches_empty_summary() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.executive_summary = "  ".to_owned();
        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("executive_summary")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_unsupported_schema_version(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.schema_version = MILESTONE_BUNDLE_VERSION + 1;
        let errors = bundle.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("unsupported schema_version")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_invalid_identity_id() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.identity.id = "../invalid".to_owned();
        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("identity.id")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_noncanonical_identity_id() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut bundle = sample_bundle();
        bundle.identity.id = " ms-alpha ".to_owned();
        let errors = bundle.validate().unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.contains("must not contain leading or trailing whitespace")));
        Ok(())
    }

    #[test]
    fn bundle_validation_catches_missing_workstreams() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams.clear();
        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("workstreams")));
        Ok(())
    }

    #[test]
    fn bundle_validation_catches_empty_bead_title() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].title = "".to_owned();
        let errors = bundle.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("title")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_duplicate_bead_identifiers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("ms-alpha.bead-1".to_owned());
        bundle.workstreams[0].beads[1].bead_id = Some("bead-1".to_owned());

        let errors = bundle.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("duplicate bead identifier")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_duplicate_title_only_proposals(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[1].title = bundle.workstreams[0].beads[0].title.clone();

        let errors = bundle.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("duplicates title-only proposal")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_invalid_cross_references() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut bundle = sample_bundle();
        bundle.acceptance_map[0].covered_by = vec!["other-ms.bead-1".to_owned()];
        bundle.workstreams[0].beads[1].depends_on = vec!["bead-999".to_owned()];
        bundle.workstreams[0].beads[2].acceptance_criteria = vec!["AC-404".to_owned()];

        let errors = bundle.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("does not belong to milestone")));
        assert!(errors.iter().any(|e| e.contains("references unknown bead")));
        assert!(errors
            .iter()
            .any(|e| e.contains("references unknown acceptance criterion")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_noncanonical_bead_references(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some(" bead-1 ".to_owned());
        bundle.acceptance_map[0].covered_by = vec![" bead-1 ".to_owned()];

        let errors = bundle.validate().unwrap_err();

        assert!(errors.iter().any(|e| e.contains(".bead_id")));
        assert!(errors.iter().any(|e| e.contains("covered_by[0]")));
        assert!(errors
            .iter()
            .any(|e| e.contains("must not contain leading or trailing whitespace")));
        Ok(())
    }

    #[test]
    fn bead_count_is_correct() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        assert_eq!(bundle.bead_count(), 3);
        Ok(())
    }

    #[test]
    fn all_beads_collects_across_workstreams() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let beads = bundle.all_beads();
        assert_eq!(beads.len(), 3);
        assert_eq!(beads[0].title, "Implement data model");
        Ok(())
    }

    #[test]
    fn json_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let json = render_plan_json(&bundle)?;
        let parsed: MilestoneBundle = serde_json::from_str(&json)?;
        assert_eq!(parsed.schema_version, MILESTONE_BUNDLE_VERSION);
        assert_eq!(parsed.identity.id, "ms-alpha");
        assert_eq!(parsed.bead_count(), 3);
        Ok(())
    }

    #[test]
    fn plan_md_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let md1 = render_plan_md(&bundle);
        let md2 = render_plan_md(&bundle);
        assert_eq!(md1, md2, "plan.md must be deterministic");
        Ok(())
    }

    #[test]
    fn plan_md_contains_key_sections() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let md = render_plan_md(&bundle);
        assert!(md.contains("# Alpha Milestone"));
        assert!(md.contains("## Executive Summary"));
        assert!(md.contains("## Goals"));
        assert!(md.contains("## Non-Goals"));
        assert!(md.contains("## Constraints & Assumptions"));
        assert!(md.contains("## Acceptance Criteria"));
        assert!(md.contains("## Workstreams"));
        assert!(md.contains("### Core Feature"));
        assert!(md.contains("## Execution Defaults"));
        assert!(md.contains("## AGENTS Guidance"));
        assert!(md.contains("quick_dev"));
        Ok(())
    }

    #[test]
    fn plan_md_contains_bead_table() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let md = render_plan_md(&bundle);
        assert!(md.contains("| # | Title | Type | Priority | Dependencies |"));
        assert!(md.contains("Implement data model"));
        assert!(md.contains("Build API endpoints"));
        Ok(())
    }

    #[test]
    fn json_schema_is_valid() -> Result<(), Box<dyn std::error::Error>> {
        let schema = schemars::schema_for!(MilestoneBundle);
        let schema_json = serde_json::to_string_pretty(&schema)?;
        // Verify schema is non-empty valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&schema_json)?;
        assert!(parsed.is_object());
        Ok(())
    }
}

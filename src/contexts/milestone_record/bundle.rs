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
        self.validate_with_mode(BundleValidationMode::Compatibility)
    }

    /// Validate the stricter planner-generation contract used for fresh LLM output.
    pub fn validate_generated(&self) -> Result<(), Vec<String>> {
        self.validate_with_mode(BundleValidationMode::Generated)
    }

    fn validate_with_mode(&self, mode: BundleValidationMode) -> Result<(), Vec<String>> {
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
        let mut non_explicit_titles = BTreeMap::new();
        let mut next_implicit_bead = 1usize;
        for (i, ws) in self.workstreams.iter().enumerate() {
            if ws.name.trim().is_empty() {
                errors.push(format!("workstreams[{i}].name must not be empty"));
            }
            let has_description = ws
                .description
                .as_deref()
                .map(str::trim)
                .filter(|description| !description.is_empty())
                .is_some();
            if mode.requires_generated_metadata() && !has_description {
                errors.push(format!("workstreams[{i}].description must not be empty"));
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
                let has_description = bead
                    .description
                    .as_deref()
                    .map(str::trim)
                    .filter(|description| !description.is_empty())
                    .is_some();
                if mode.requires_generated_metadata() && !has_description {
                    errors.push(format!("{location}.description must not be empty"));
                }
                let has_bead_type = bead
                    .bead_type
                    .as_deref()
                    .map(str::trim)
                    .filter(|bead_type| !bead_type.is_empty())
                    .is_some();
                if mode.requires_generated_metadata() && !has_bead_type {
                    errors.push(format!("{location}.bead_type must not be empty"));
                }
                let has_bead_id = bead
                    .bead_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|bead_id| !bead_id.is_empty())
                    .is_some();
                if mode.requires_generated_metadata() && !has_bead_id {
                    errors.push(format!("{location}.bead_id must not be empty"));
                }
                match bead.priority {
                    Some(priority) if mode.allows_priority(priority) => {}
                    Some(priority) => errors.push(format!(
                        "{location}.priority must be {}, got {priority}",
                        mode.priority_expectation()
                    )),
                    None if mode.requires_generated_metadata() => {
                        errors.push(format!("{location}.priority must not be empty"))
                    }
                    None => {}
                }
                if mode.requires_generated_metadata() && bead.labels.is_empty() {
                    errors.push(format!("{location}.labels must contain at least one label"));
                }
                for (k, label) in bead.labels.iter().enumerate() {
                    if label.trim().is_empty() {
                        errors.push(format!("{location}.labels[{k}] must not be empty"));
                    }
                }

                let implicit_bead_id = format!("bead-{next_implicit_bead}");
                next_implicit_bead += 1;
                let canonical_implicit_bead_id = format!("{milestone_id}.{implicit_bead_id}");
                validate_bead_identity_metadata(
                    &location,
                    milestone_id,
                    bead,
                    &canonical_implicit_bead_id,
                    &mut errors,
                );
                let has_explicit_id =
                    infer_bead_explicit_id(milestone_id, bead, &canonical_implicit_bead_id);

                if !has_explicit_id && !bead.title.trim().is_empty() {
                    if let Some(previous) =
                        non_explicit_titles.insert(bead.title.trim().to_owned(), location.clone())
                    {
                        errors.push(format!(
                            "{location}.title '{}' duplicates non-explicit proposal from {}",
                            bead.title.trim(),
                            previous
                        ));
                    }
                }

                let canonical_bead_id = if let Some(bead_id) = bead.bead_id.as_deref() {
                    match normalize_bead_reference(milestone_id, bead_id) {
                        Ok(canonical) => canonical,
                        Err(reason) => {
                            errors.push(format!("{location}.bead_id {reason}"));
                            implicit_bead_id
                        }
                    }
                } else {
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

                if mode.requires_generated_metadata() && bead.acceptance_criteria.is_empty() {
                    errors.push(format!(
                        "{location}.acceptance_criteria must contain at least one item"
                    ));
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
                    } else if bead.acceptance_criteria[..k]
                        .iter()
                        .any(|previous| previous.trim() == acceptance_id.trim())
                    {
                        errors.push(format!(
                            "{location}.acceptance_criteria[{k}] duplicates acceptance criterion '{}'",
                            acceptance_id.trim()
                        ));
                    }
                }
            }
        }

        for (i, ac) in self.acceptance_map.iter().enumerate() {
            if mode.requires_generated_metadata() && ac.covered_by.is_empty() {
                errors.push(format!(
                    "acceptance_map[{i}].covered_by must contain at least one bead"
                ));
            }
            for (j, covered_by) in ac.covered_by.iter().enumerate() {
                match normalize_bead_reference(milestone_id, covered_by) {
                    Ok(reference) => {
                        if !bead_ids.contains_key(&reference) {
                            errors.push(format!(
                                "acceptance_map[{i}].covered_by[{j}] references unknown bead '{}'",
                                covered_by.trim()
                            ));
                        } else if ac.covered_by[..j].iter().any(|previous| {
                            normalize_bead_reference(milestone_id, previous)
                                .map(|normalized| normalized == reference)
                                .unwrap_or(false)
                        }) {
                            errors.push(format!(
                                "acceptance_map[{i}].covered_by[{j}] duplicates bead '{}'",
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
            match canonicalize_bundle(self) {
                Ok(canonical) => {
                    let covered_by_from_beads = derived_acceptance_coverage(&canonical);

                    for (i, criterion) in canonical.acceptance_map.iter().enumerate() {
                        let declared = criterion
                            .covered_by
                            .iter()
                            .cloned()
                            .collect::<BTreeSet<_>>();
                        let derived = covered_by_from_beads
                            .get(&criterion.id)
                            .cloned()
                            .unwrap_or_default();

                        if mode.requires_generated_metadata() && declared.is_empty() {
                            errors.push(format!(
                                "acceptance_map[{i}].covered_by must contain at least one bead"
                            ));
                        }

                        if declared != derived {
                            let missing_from_map =
                                derived.difference(&declared).cloned().collect::<Vec<_>>();
                            let missing_from_beads =
                                declared.difference(&derived).cloned().collect::<Vec<_>>();
                            let mut mismatch_details = Vec::new();
                            if !missing_from_map.is_empty() {
                                mismatch_details.push(format!(
                                    "missing from acceptance_map: {}",
                                    missing_from_map.join(", ")
                                ));
                            }
                            if !missing_from_beads.is_empty() {
                                mismatch_details.push(format!(
                                    "missing from bead.acceptance_criteria: {}",
                                    missing_from_beads.join(", ")
                                ));
                            }
                            errors.push(format!(
                                "acceptance_map[{i}] coverage for '{}' must match bead.acceptance_criteria ({})",
                                criterion.id,
                                mismatch_details.join("; ")
                            ));
                        }
                    }

                    if let Err(mut graph_errors) =
                        dependency_graph_analysis_from_canonical(&canonical)
                    {
                        errors.append(&mut graph_errors);
                    }
                }
                Err(mut canonicalization_errors) => errors.append(&mut canonicalization_errors),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BundleValidationMode {
    Compatibility,
    Generated,
}

impl BundleValidationMode {
    fn requires_generated_metadata(self) -> bool {
        matches!(self, Self::Generated)
    }

    fn allows_priority(self, priority: u32) -> bool {
        match self {
            Self::Compatibility => matches!(priority, 0..=4),
            Self::Generated => matches!(priority, 1..=3),
        }
    }

    fn priority_expectation(self) -> &'static str {
        match self {
            Self::Compatibility => "between 0 and 4",
            Self::Generated => "between 1 and 3",
        }
    }
}

pub(crate) fn progress_shape_signature(bundle: &MilestoneBundle) -> Result<String, Vec<String>> {
    progress_shape_signature_with_explicit_id_hints(bundle, None)
}

pub(crate) fn progress_shape_signature_with_explicit_id_hints(
    bundle: &MilestoneBundle,
    explicit_id_hints: Option<&BTreeMap<String, bool>>,
) -> Result<String, Vec<String>> {
    #[derive(Serialize)]
    struct ProgressShapeSignature {
        beads: Vec<CanonicalBeadSignature>,
    }

    #[derive(Serialize)]
    struct CanonicalBeadSignature {
        bead_id: String,
        has_explicit_id: bool,
        workstream_name: Option<String>,
        workstream_description: Option<String>,
        title: Option<String>,
        description: Option<String>,
        bead_type: Option<String>,
        priority: Option<u32>,
        labels: Vec<String>,
        depends_on: Vec<String>,
        acceptance_criteria: Vec<String>,
        flow_override: Option<FlowPreset>,
    }

    let canonical_input = if let Some(hints) = explicit_id_hints {
        apply_explicit_id_hints(bundle, hints)?
    } else {
        bundle.clone()
    };
    let canonical = validated_canonical_bundle(&canonical_input)?;
    let mut beads = Vec::new();

    for workstream in &canonical.workstreams {
        for bead in &workstream.beads {
            let has_explicit_id = bead.explicit_id.unwrap_or(false);
            let mut labels = bead.labels.clone();
            labels.sort();

            let mut depends_on = bead.depends_on.clone();
            depends_on.sort();

            let mut acceptance_criteria = bead.acceptance_criteria.clone();
            acceptance_criteria.sort();

            beads.push(CanonicalBeadSignature {
                bead_id: bead
                    .bead_id
                    .as_deref()
                    .expect("canonicalized bundles assign every bead an id")
                    .to_owned(),
                has_explicit_id,
                workstream_name: (!has_explicit_id).then(|| workstream.name.clone()),
                workstream_description: (!has_explicit_id)
                    .then(|| workstream.description.clone())
                    .flatten(),
                title: (!has_explicit_id).then(|| bead.title.clone()),
                description: (!has_explicit_id)
                    .then(|| bead.description.clone())
                    .flatten(),
                bead_type: (!has_explicit_id).then(|| bead.bead_type.clone()).flatten(),
                priority: (!has_explicit_id).then_some(bead.priority).flatten(),
                labels: if has_explicit_id { Vec::new() } else { labels },
                depends_on,
                acceptance_criteria,
                flow_override: bead.flow_override,
            });
        }
    }

    beads.sort_by(|left, right| left.bead_id.cmp(&right.bead_id));

    serde_json::to_string(&ProgressShapeSignature { beads }).map_err(|error| {
        vec![format!(
            "failed to serialize progress shape signature: {error}"
        )]
    })
}

fn infer_bead_explicit_id(milestone_id: &str, bead: &BeadProposal, implicit_bead_id: &str) -> bool {
    bead.explicit_id.unwrap_or_else(|| {
        bead.bead_id.as_deref().is_some_and(|candidate| {
            !bead_matches_implicit_slot(candidate, milestone_id, implicit_bead_id)
        })
    })
}

fn validate_bead_identity_metadata(
    location: &str,
    milestone_id: &str,
    bead: &BeadProposal,
    implicit_bead_id: &str,
    errors: &mut Vec<String>,
) {
    match bead.explicit_id {
        Some(true) if bead.bead_id.is_none() => errors.push(format!(
            "{location}.explicit_id cannot be true without bead_id"
        )),
        Some(false) => {
            if let Some(candidate) = bead.bead_id.as_deref() {
                if let Ok(canonical_candidate) = normalize_bead_reference(milestone_id, candidate) {
                    if !bead_matches_implicit_slot(
                        &canonical_candidate,
                        milestone_id,
                        implicit_bead_id,
                    ) {
                        errors.push(format!(
                            "{location}.explicit_id cannot be false when bead_id '{}' does not match implicit slot '{}'",
                            candidate.trim(),
                            implicit_bead_id
                        ));
                    }
                }
            }
        }
        _ => {}
    }
}

pub(crate) fn bead_matches_implicit_slot(
    candidate: &str,
    milestone_id: &str,
    implicit_bead_id: &str,
) -> bool {
    candidate == implicit_bead_id
        || candidate
            == implicit_bead_id
                .strip_prefix(&format!("{milestone_id}."))
                .unwrap_or(implicit_bead_id)
}

pub(crate) fn explicit_id_hints(
    bundle: &MilestoneBundle,
) -> Result<BTreeMap<String, bool>, Vec<String>> {
    let canonical = validated_canonical_bundle(bundle)?;
    let mut hints = BTreeMap::new();
    for workstream in canonical.workstreams {
        for bead in workstream.beads {
            let bead_id = bead
                .bead_id
                .expect("canonicalized bundles assign every bead an id");
            hints.insert(bead_id, bead.explicit_id.unwrap_or(false));
        }
    }
    Ok(hints)
}

pub(crate) fn planned_bead_membership_refs(
    bundle: &MilestoneBundle,
) -> Result<BTreeSet<String>, Vec<String>> {
    let canonical = validated_canonical_bundle(bundle)?;
    let milestone_id = canonical.identity.id.clone();
    let qualified_prefix = format!("{milestone_id}.");
    let mut refs = BTreeSet::new();

    for workstream in canonical.workstreams {
        for bead in workstream.beads {
            let bead_id = bead
                .bead_id
                .expect("canonicalized bundles assign every bead an id");
            refs.insert(bead_id.clone());

            // Explicit beads may already exist in `.beads/` under their authored
            // short ID, so keep both forms for runtime membership checks.
            if bead.explicit_id.unwrap_or(false) {
                if let Some(short_ref) = bead_id.strip_prefix(&qualified_prefix) {
                    refs.insert(short_ref.to_owned());
                }
            }
        }
    }

    Ok(refs)
}

fn apply_explicit_id_hints(
    bundle: &MilestoneBundle,
    hints: &BTreeMap<String, bool>,
) -> Result<MilestoneBundle, Vec<String>> {
    let milestone_id = bundle.identity.id.trim();
    let mut with_hints = bundle.clone();
    let mut next_implicit_bead = 1usize;

    for workstream in &mut with_hints.workstreams {
        for bead in &mut workstream.beads {
            let implicit_bead_id = format!("{milestone_id}.bead-{next_implicit_bead}");
            next_implicit_bead += 1;

            if bead.explicit_id.is_some() {
                continue;
            }

            let canonical_id = bead
                .bead_id
                .as_deref()
                .map(|raw| canonicalize_bead_reference(milestone_id, raw))
                .transpose()
                .map_err(|error| vec![error])?
                .unwrap_or(implicit_bead_id);
            if let Some(explicit_id) = hints.get(&canonical_id) {
                bead.explicit_id = Some(*explicit_id);
            }
        }
    }

    Ok(with_hints)
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

fn canonicalize_bead_reference(milestone_id: &str, raw: &str) -> Result<String, String> {
    normalize_bead_reference(milestone_id, raw)
        .map(|normalized| format!("{milestone_id}.{normalized}"))
}

fn derived_acceptance_coverage(bundle: &MilestoneBundle) -> BTreeMap<String, BTreeSet<String>> {
    let mut covered_by_from_beads = bundle
        .acceptance_map
        .iter()
        .map(|criterion| (criterion.id.clone(), BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();

    for workstream in &bundle.workstreams {
        for bead in &workstream.beads {
            let bead_id = bead
                .bead_id
                .as_deref()
                .expect("canonicalized bundles assign every bead an id")
                .to_owned();
            for acceptance_id in &bead.acceptance_criteria {
                covered_by_from_beads
                    .entry(acceptance_id.clone())
                    .or_default()
                    .insert(bead_id.clone());
            }
        }
    }

    covered_by_from_beads
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkstreamSummary {
    pub name: String,
    pub bead_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleSummary {
    pub total_workstreams: usize,
    pub total_beads: usize,
    pub beads_per_workstream: Vec<WorkstreamSummary>,
    pub dependency_depth: usize,
    pub covered_acceptance_criteria: usize,
    pub total_acceptance_criteria: usize,
    pub acceptance_coverage_percentage: usize,
}

#[derive(Debug, Clone)]
struct DependencyNode {
    bead_id: String,
    workstream_index: usize,
    depends_on: Vec<String>,
    location: String,
}

#[derive(Debug, Clone)]
struct DependencyGraphAnalysis {
    bead_order: Vec<String>,
    dependency_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkstreamOrderAnalysis {
    ordered: Vec<usize>,
    blocked: Vec<usize>,
}

fn inferred_bead_identifier(raw_bead_id: Option<&str>, implicit_index: usize) -> String {
    raw_bead_id
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("bead-{implicit_index}"))
}

fn bead_reference_aliases(reference: &str) -> Vec<String> {
    let trimmed = reference.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    let mut aliases = vec![trimmed.to_owned()];
    if let Some((_, suffix)) = trimmed.rsplit_once('.') {
        if !suffix.is_empty() && suffix != trimmed {
            aliases.push(suffix.to_owned());
        }
    }
    aliases.sort();
    aliases.dedup();
    aliases
}

fn inferred_dependency_nodes(workstreams: &[Workstream]) -> Vec<DependencyNode> {
    let mut next_implicit_bead = 1usize;
    let mut nodes = Vec::new();

    for (workstream_index, workstream) in workstreams.iter().enumerate() {
        for (bead_index, bead) in workstream.beads.iter().enumerate() {
            let bead_id = inferred_bead_identifier(bead.bead_id.as_deref(), next_implicit_bead);
            next_implicit_bead += 1;
            nodes.push(DependencyNode {
                bead_id,
                workstream_index,
                depends_on: bead.depends_on.clone(),
                location: format!("workstreams[{workstream_index}].beads[{bead_index}]"),
            });
        }
    }

    nodes
}

fn analyze_workstream_order(workstreams: &[Workstream]) -> WorkstreamOrderAnalysis {
    let nodes = inferred_dependency_nodes(workstreams);
    let mut bead_index = BTreeMap::new();
    let mut ambiguous_aliases = BTreeSet::new();

    for (index, node) in nodes.iter().enumerate() {
        for alias in bead_reference_aliases(&node.bead_id) {
            if bead_index.insert(alias.clone(), index).is_some() {
                ambiguous_aliases.insert(alias);
            }
        }
    }
    for alias in ambiguous_aliases {
        bead_index.remove(&alias);
    }

    let mut workstream_edges = vec![BTreeSet::new(); workstreams.len()];
    let mut indegree = vec![0usize; workstreams.len()];
    for node in &nodes {
        for depends_on in &node.depends_on {
            for alias in bead_reference_aliases(depends_on) {
                if let Some(dependency_index) = bead_index.get(&alias) {
                    let upstream = nodes[*dependency_index].workstream_index;
                    let downstream = node.workstream_index;
                    if upstream != downstream && workstream_edges[upstream].insert(downstream) {
                        indegree[downstream] += 1;
                    }
                    break;
                }
            }
        }
    }

    let mut ready = BTreeSet::new();
    for (index, count) in indegree.iter().enumerate() {
        if *count == 0 {
            ready.insert(index);
        }
    }

    let mut ordered = Vec::new();
    while let Some(index) = ready.pop_first() {
        ordered.push(index);
        for dependent in &workstream_edges[index] {
            indegree[*dependent] -= 1;
            if indegree[*dependent] == 0 {
                ready.insert(*dependent);
            }
        }
    }

    let blocked = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, count)| (*count > 0).then_some(index))
        .collect();

    WorkstreamOrderAnalysis { ordered, blocked }
}

fn dependency_graph_analysis_from_canonical(
    canonical: &MilestoneBundle,
) -> Result<DependencyGraphAnalysis, Vec<String>> {
    let mut nodes = Vec::new();
    for (workstream_index, workstream) in canonical.workstreams.iter().enumerate() {
        for (bead_index, bead) in workstream.beads.iter().enumerate() {
            nodes.push(DependencyNode {
                bead_id: bead
                    .bead_id
                    .as_deref()
                    .expect("canonicalized bundles assign every bead an id")
                    .to_owned(),
                workstream_index,
                depends_on: bead.depends_on.clone(),
                location: format!("workstreams[{workstream_index}].beads[{bead_index}]"),
            });
        }
    }

    dependency_graph_analysis_from_nodes(&nodes)
}

fn dependency_graph_analysis_from_nodes(
    nodes: &[DependencyNode],
) -> Result<DependencyGraphAnalysis, Vec<String>> {
    let mut bead_index = BTreeMap::new();
    let mut errors = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        if let Some(previous_index) = bead_index.insert(node.bead_id.clone(), index) {
            errors.push(format!(
                "{} resolves to duplicate bead identifier '{}' already used by {}",
                node.location, node.bead_id, nodes[previous_index].location
            ));
        }
    }

    for node in nodes {
        for depends_on in &node.depends_on {
            if !bead_index.contains_key(depends_on) {
                errors.push(format!(
                    "dependency '{}' referenced by '{}' does not exist",
                    depends_on, node.bead_id
                ));
            }
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    fn detect_cycle(
        node_index: usize,
        nodes: &[DependencyNode],
        bead_index: &BTreeMap<String, usize>,
        states: &mut [u8],
        stack: &mut Vec<usize>,
        cycle_reports: &mut BTreeSet<String>,
    ) {
        states[node_index] = 1;
        stack.push(node_index);

        for depends_on in &nodes[node_index].depends_on {
            let dependency_index = *bead_index
                .get(depends_on)
                .expect("missing dependencies handled before cycle detection");
            match states[dependency_index] {
                0 => detect_cycle(
                    dependency_index,
                    nodes,
                    bead_index,
                    states,
                    stack,
                    cycle_reports,
                ),
                1 => {
                    if let Some(cycle_start) =
                        stack.iter().position(|index| *index == dependency_index)
                    {
                        let mut cycle = stack[cycle_start..]
                            .iter()
                            .map(|index| nodes[*index].bead_id.clone())
                            .collect::<Vec<_>>();
                        cycle.push(nodes[dependency_index].bead_id.clone());
                        cycle_reports
                            .insert(format!("dependency cycle detected: {}", cycle.join(" -> ")));
                    }
                }
                _ => {}
            }
        }

        stack.pop();
        states[node_index] = 2;
    }

    let mut states = vec![0u8; nodes.len()];
    let mut stack = Vec::new();
    let mut cycle_reports = BTreeSet::new();
    for node_index in 0..nodes.len() {
        if states[node_index] == 0 {
            detect_cycle(
                node_index,
                nodes,
                &bead_index,
                &mut states,
                &mut stack,
                &mut cycle_reports,
            );
        }
    }

    if !cycle_reports.is_empty() {
        return Err(cycle_reports.into_iter().collect());
    }

    let mut dependents = vec![Vec::new(); nodes.len()];
    let mut indegree = vec![0usize; nodes.len()];
    for (node_index, node) in nodes.iter().enumerate() {
        indegree[node_index] = node.depends_on.len();
        for depends_on in &node.depends_on {
            let dependency_index = *bead_index
                .get(depends_on)
                .expect("missing dependencies handled before ordering");
            dependents[dependency_index].push(node_index);
        }
    }

    let mut ready = BTreeSet::new();
    for (node_index, count) in indegree.iter().enumerate() {
        if *count == 0 {
            ready.insert(node_index);
        }
    }

    let mut bead_order = Vec::new();
    let mut depth = vec![1usize; nodes.len()];
    while let Some(node_index) = ready.pop_first() {
        bead_order.push(nodes[node_index].bead_id.clone());

        for dependent_index in &dependents[node_index] {
            depth[*dependent_index] = depth[*dependent_index].max(depth[node_index] + 1);
            indegree[*dependent_index] -= 1;
            if indegree[*dependent_index] == 0 {
                ready.insert(*dependent_index);
            }
        }
    }

    Ok(DependencyGraphAnalysis {
        bead_order,
        dependency_depth: depth.into_iter().max().unwrap_or(0),
    })
}

fn dependency_graph_analysis(
    bundle: &MilestoneBundle,
) -> Result<DependencyGraphAnalysis, Vec<String>> {
    let canonical = canonicalize_bundle(bundle)?;
    dependency_graph_analysis_from_canonical(&canonical)
}

fn contains_deferred_note(text: &str) -> bool {
    let lowered = text.to_ascii_lowercase();
    [
        "defer",
        "deferred",
        "not included",
        "not part of this milestone",
        "out of scope",
        "future work",
        "later phase",
        "follow-up work",
        "follow-up milestone",
        "intentionally excluded",
    ]
    .iter()
    .any(|needle| lowered.contains(needle))
}

fn collect_deferred_items(bundle: &MilestoneBundle, workstream_labels: &[String]) -> Vec<String> {
    let mut items = Vec::new();

    for (workstream, workstream_label) in bundle.workstreams.iter().zip(workstream_labels.iter()) {
        if let Some(description) = workstream
            .description
            .as_deref()
            .filter(|description| contains_deferred_note(description))
        {
            items.push(format!("Workstream {workstream_label}: {description}"));
        }

        for bead in &workstream.beads {
            if let Some(description) = bead
                .description
                .as_deref()
                .filter(|description| contains_deferred_note(description))
            {
                let bead_id = bead.bead_id.as_deref().unwrap_or("-");
                items.push(format!("Bead {bead_id} ({}): {description}", bead.title));
            }
        }
    }

    items
}

pub fn infer_workstream_order(workstreams: &[Workstream]) -> Vec<usize> {
    let analysis = analyze_workstream_order(workstreams);
    if analysis.blocked.is_empty() {
        analysis.ordered
    } else {
        Vec::new()
    }
}

pub fn validate_dependency_graph(bundle: &MilestoneBundle) -> Result<(), Vec<String>> {
    let (nodes, mut errors) = canonicalize_dependency_nodes(bundle);
    if let Err(mut graph_errors) = dependency_graph_analysis_from_nodes(&nodes) {
        errors.append(&mut graph_errors);
    }
    errors.sort();
    errors.dedup();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn summarize_bundle(bundle: &MilestoneBundle) -> BundleSummary {
    let canonical = canonicalize_bundle(bundle).unwrap_or_else(|_| bundle.clone());
    let dependency_depth = dependency_graph_analysis(&canonical)
        .map(|analysis| analysis.dependency_depth)
        .unwrap_or(0);
    let covered_acceptance_ids = canonical
        .workstreams
        .iter()
        .flat_map(|workstream| workstream.beads.iter())
        .flat_map(|bead| bead.acceptance_criteria.iter())
        .map(|acceptance_id| acceptance_id.trim().to_owned())
        .filter(|acceptance_id| !acceptance_id.is_empty())
        .collect::<BTreeSet<_>>();
    let covered_acceptance_criteria = canonical
        .acceptance_map
        .iter()
        .filter(|criterion| covered_acceptance_ids.contains(criterion.id.trim()))
        .count();
    let total_acceptance_criteria = canonical.acceptance_map.len();
    let acceptance_coverage_percentage = covered_acceptance_criteria
        .checked_mul(100)
        .and_then(|scaled| scaled.checked_div(total_acceptance_criteria))
        .unwrap_or(0);

    BundleSummary {
        total_workstreams: canonical.workstreams.len(),
        total_beads: canonical.bead_count(),
        beads_per_workstream: canonical
            .workstreams
            .iter()
            .zip(render_workstream_labels(&canonical.workstreams))
            .map(|(workstream, name)| WorkstreamSummary {
                name,
                bead_count: workstream.beads.len(),
            })
            .collect(),
        dependency_depth,
        covered_acceptance_criteria,
        total_acceptance_criteria,
        acceptance_coverage_percentage,
    }
}

fn render_workstream_labels(workstreams: &[Workstream]) -> Vec<String> {
    let mut name_counts = BTreeMap::new();
    for workstream in workstreams {
        *name_counts
            .entry(workstream.name.as_str())
            .or_insert(0usize) += 1;
    }

    let mut name_ordinals = BTreeMap::new();
    workstreams
        .iter()
        .map(|workstream| {
            if name_counts
                .get(workstream.name.as_str())
                .copied()
                .unwrap_or(0)
                <= 1
            {
                return workstream.name.clone();
            }

            let ordinal = name_ordinals
                .entry(workstream.name.as_str())
                .and_modify(|count| *count += 1)
                .or_insert(1usize);
            format!("{} [{}]", workstream.name, ordinal)
        })
        .collect()
}

fn validated_canonical_bundle(bundle: &MilestoneBundle) -> Result<MilestoneBundle, Vec<String>> {
    bundle.validate()?;
    canonicalize_bundle(bundle)
}

fn canonicalize_dependency_nodes(bundle: &MilestoneBundle) -> (Vec<DependencyNode>, Vec<String>) {
    let milestone_id = bundle.identity.id.trim();
    let mut nodes = Vec::new();
    let mut errors = Vec::new();
    let mut next_implicit_bead = 1usize;

    for (workstream_index, workstream) in bundle.workstreams.iter().enumerate() {
        for (bead_index, bead) in workstream.beads.iter().enumerate() {
            let location = format!("workstreams[{workstream_index}].beads[{bead_index}]");
            let implicit_bead_id = format!("{milestone_id}.bead-{next_implicit_bead}");
            next_implicit_bead += 1;

            let bead_id = match bead
                .bead_id
                .as_deref()
                .map(|raw| canonicalize_bead_reference(milestone_id, raw))
                .transpose()
            {
                Ok(Some(canonical_id)) => canonical_id,
                Ok(None) => implicit_bead_id,
                Err(reason) => {
                    errors.push(format!("{location}.bead_id {reason}"));
                    implicit_bead_id
                }
            };

            let mut depends_on = Vec::new();
            for (dependency_index, dependency) in bead.depends_on.iter().enumerate() {
                match canonicalize_bead_reference(milestone_id, dependency) {
                    Ok(canonical_ref) => depends_on.push(canonical_ref),
                    Err(reason) => errors.push(format!(
                        "{location}.depends_on[{dependency_index}] {reason}"
                    )),
                }
            }

            nodes.push(DependencyNode {
                bead_id,
                workstream_index,
                depends_on,
                location,
            });
        }
    }

    (nodes, errors)
}

fn canonicalize_bundle(bundle: &MilestoneBundle) -> Result<MilestoneBundle, Vec<String>> {
    let milestone_id = bundle.identity.id.trim();
    let mut canonical = bundle.clone();
    let mut errors = Vec::new();
    let mut next_implicit_bead = 1usize;

    for (workstream_index, workstream) in canonical.workstreams.iter_mut().enumerate() {
        for (bead_index, bead) in workstream.beads.iter_mut().enumerate() {
            let implicit_bead_id = format!("{milestone_id}.bead-{next_implicit_bead}");
            next_implicit_bead += 1;
            let has_explicit_id = infer_bead_explicit_id(milestone_id, bead, &implicit_bead_id);

            match bead
                .bead_id
                .as_deref()
                .map(|raw| canonicalize_bead_reference(milestone_id, raw))
                .transpose()
            {
                Ok(Some(canonical_id)) => bead.bead_id = Some(canonical_id),
                Ok(None) => bead.bead_id = Some(implicit_bead_id),
                Err(reason) => errors.push(format!(
                    "workstreams[{workstream_index}].beads[{bead_index}].bead_id {reason}"
                )),
            }
            bead.explicit_id = Some(has_explicit_id);

            for (dependency_index, depends_on) in bead.depends_on.iter_mut().enumerate() {
                match canonicalize_bead_reference(milestone_id, depends_on) {
                    Ok(canonical_ref) => *depends_on = canonical_ref,
                    Err(reason) => errors.push(format!(
                        "workstreams[{workstream_index}].beads[{bead_index}].depends_on[{dependency_index}] {reason}"
                    )),
                }
            }

            for acceptance_id in &mut bead.acceptance_criteria {
                *acceptance_id = acceptance_id.trim().to_owned();
            }
        }
    }

    for (criterion_index, criterion) in canonical.acceptance_map.iter_mut().enumerate() {
        criterion.id = criterion.id.trim().to_owned();
        for (covered_index, covered_by) in criterion.covered_by.iter_mut().enumerate() {
            match canonicalize_bead_reference(milestone_id, covered_by) {
                Ok(canonical_ref) => *covered_by = canonical_ref,
                Err(reason) => errors.push(format!(
                    "acceptance_map[{criterion_index}].covered_by[{covered_index}] {reason}"
                )),
            }
        }
    }

    let covered_by_from_beads = derived_acceptance_coverage(&canonical);
    for criterion in &mut canonical.acceptance_map {
        if criterion.covered_by.is_empty() {
            criterion.covered_by = covered_by_from_beads
                .get(&criterion.id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
        }
    }

    if errors.is_empty() {
        Ok(canonical)
    } else {
        Err(errors)
    }
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
    /// Whether the planner explicitly supplied `bead_id` before canonicalization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_id: Option<bool>,
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

fn invalid_plan_render_error(errors: Vec<String>) -> serde_json::Error {
    serde_json::Error::io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        errors.join("; "),
    ))
}

fn escape_markdown_table_cell(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace("\r\n", "<br>")
        .replace(['\n', '\r'], "<br>")
}

pub(crate) fn render_plan_md_checked(bundle: &MilestoneBundle) -> Result<String, Vec<String>> {
    let canonical = validated_canonical_bundle(bundle)?;
    let workstream_labels = render_workstream_labels(&canonical.workstreams);
    let summary = summarize_bundle(&canonical);
    let dependency_graph = dependency_graph_analysis_from_canonical(&canonical)?;
    let workstream_order = analyze_workstream_order(&canonical.workstreams);
    let deferred_items = collect_deferred_items(&canonical, &workstream_labels);
    let mut out = String::new();

    writeln!(out, "# {}", canonical.identity.name).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- **Milestone ID:** {}", canonical.identity.id).unwrap();
    writeln!(out, "- **Schema Version:** {}", canonical.schema_version).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- **Workstreams:** {}", summary.total_workstreams).unwrap();
    writeln!(out, "- **Total beads:** {}", summary.total_beads).unwrap();
    writeln!(
        out,
        "- **Acceptance coverage:** {}/{} ({}%)",
        summary.covered_acceptance_criteria,
        summary.total_acceptance_criteria,
        summary.acceptance_coverage_percentage
    )
    .unwrap();
    writeln!(out, "- **Dependency depth:** {}", summary.dependency_depth).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Executive Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", canonical.executive_summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Goals").unwrap();
    writeln!(out).unwrap();
    for goal in &canonical.goals {
        writeln!(out, "- {goal}").unwrap();
    }
    writeln!(out).unwrap();

    if !canonical.non_goals.is_empty() {
        writeln!(out, "## Non-Goals").unwrap();
        writeln!(out).unwrap();
        for ng in &canonical.non_goals {
            writeln!(out, "- {ng}").unwrap();
        }
        writeln!(out).unwrap();
    }

    if !canonical.constraints.is_empty() {
        writeln!(out, "## Constraints & Assumptions").unwrap();
        writeln!(out).unwrap();
        for constraint in &canonical.constraints {
            writeln!(out, "- {constraint}").unwrap();
        }
        writeln!(out).unwrap();
    }

    let bead_lookup = canonical
        .workstreams
        .iter()
        .zip(workstream_labels.iter())
        .flat_map(|(workstream, workstream_label)| {
            workstream.beads.iter().filter_map(move |bead| {
                bead.bead_id.as_ref().map(|bead_id| {
                    (
                        bead_id.clone(),
                        (workstream_label.as_str(), bead.title.as_str()),
                    )
                })
            })
        })
        .collect::<BTreeMap<_, _>>();

    writeln!(out, "## Acceptance Criteria").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "| ID | Criterion | Workstreams | Covered By Beads |").unwrap();
    writeln!(out, "|----|-----------|-------------|------------------|").unwrap();
    for ac in &canonical.acceptance_map {
        let linked_beads = ac
            .covered_by
            .iter()
            .filter_map(|bead_id| {
                bead_lookup
                    .get(bead_id)
                    .map(|(workstream_name, bead_title)| {
                        (
                            (*workstream_name).to_owned(),
                            format!("{bead_id} ({bead_title})"),
                        )
                    })
            })
            .collect::<Vec<_>>();
        let workstreams = linked_beads
            .iter()
            .map(|(workstream_name, _)| workstream_name.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let covered_by = linked_beads
            .into_iter()
            .map(|(_, bead_summary)| bead_summary)
            .collect::<Vec<_>>();
        let workstream_cell = if workstreams.is_empty() {
            "-".to_owned()
        } else {
            workstreams.join(", ")
        };
        let covered_by_cell = if covered_by.is_empty() {
            "-".to_owned()
        } else {
            covered_by.join("; ")
        };
        writeln!(
            out,
            "| {} | {} | {} | {} |",
            escape_markdown_table_cell(&ac.id),
            escape_markdown_table_cell(&ac.description),
            escape_markdown_table_cell(&workstream_cell),
            escape_markdown_table_cell(&covered_by_cell)
        )
        .unwrap();
    }
    writeln!(out).unwrap();

    writeln!(out, "## Dependency Graph").unwrap();
    writeln!(out).unwrap();
    let ordered_workstreams = workstream_order
        .ordered
        .into_iter()
        .filter_map(|index| workstream_labels.get(index).cloned())
        .collect::<Vec<_>>();
    if workstream_order.blocked.is_empty() {
        writeln!(
            out,
            "- **Workstream order:** {}",
            ordered_workstreams.join(" -> ")
        )
        .unwrap();
    } else {
        let blocked_workstreams = workstream_order
            .blocked
            .into_iter()
            .filter_map(|index| workstream_labels.get(index).cloned())
            .collect::<Vec<_>>();
        writeln!(
            out,
            "- **Workstream order:** No single topological workstream order; cross-workstream dependencies interleave across {}",
            blocked_workstreams.join(", ")
        )
        .unwrap();
        if !ordered_workstreams.is_empty() {
            writeln!(
                out,
                "- **Workstream prefixes before interleaving:** {}",
                ordered_workstreams.join(" -> ")
            )
            .unwrap();
        }
    }
    writeln!(
        out,
        "- **Bead order:** {}",
        dependency_graph.bead_order.join(" -> ")
    )
    .unwrap();
    writeln!(out).unwrap();

    if !deferred_items.is_empty() {
        writeln!(out, "## Deferred Items").unwrap();
        writeln!(out).unwrap();
        for item in deferred_items {
            writeln!(out, "- {item}").unwrap();
        }
        writeln!(out).unwrap();
    }

    writeln!(out, "## Workstreams").unwrap();
    writeln!(out).unwrap();
    for (ws, workstream_label) in canonical.workstreams.iter().zip(workstream_labels.iter()) {
        writeln!(out, "### {}", workstream_label).unwrap();
        writeln!(out).unwrap();
        if let Some(desc) = &ws.description {
            writeln!(out, "{desc}").unwrap();
            writeln!(out).unwrap();
        }
        writeln!(
            out,
            "| # | Bead ID | Title | Type | Priority | Acceptance | Dependencies | Flow |"
        )
        .unwrap();
        writeln!(
            out,
            "|---|---------|-------|------|----------|------------|--------------|------|"
        )
        .unwrap();
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
            let acceptance = if bead.acceptance_criteria.is_empty() {
                "-".to_owned()
            } else {
                bead.acceptance_criteria.join(", ")
            };
            let flow = bead
                .flow_override
                .unwrap_or(canonical.default_flow)
                .to_string();
            writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} | {} | {} |",
                i + 1,
                escape_markdown_table_cell(bead.bead_id.as_deref().unwrap_or("-")),
                escape_markdown_table_cell(&bead.title),
                escape_markdown_table_cell(bead_type),
                escape_markdown_table_cell(&priority),
                escape_markdown_table_cell(&acceptance),
                escape_markdown_table_cell(&deps),
                escape_markdown_table_cell(&flow)
            )
            .unwrap();
        }
        writeln!(out).unwrap();
    }

    writeln!(out, "## Execution Defaults").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- **Default flow:** {}", canonical.default_flow).unwrap();
    writeln!(out, "- **Total beads:** {}", canonical.bead_count()).unwrap();
    writeln!(out).unwrap();

    if let Some(guidance) = &canonical.agents_guidance {
        writeln!(out, "## AGENTS Guidance").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "{guidance}").unwrap();
    }

    Ok(out)
}

/// Render a MilestoneBundle as deterministic `plan.md` content.
pub fn render_plan_md(bundle: &MilestoneBundle) -> String {
    render_plan_md_checked(bundle).unwrap_or_else(|errors| {
        let mut out = String::new();
        writeln!(out, "# {}", bundle.identity.name).unwrap();
        writeln!(out).unwrap();
        writeln!(out, "## Invalid Plan").unwrap();
        writeln!(out).unwrap();
        writeln!(
            out,
            "The milestone bundle is invalid and cannot be rendered authoritatively."
        )
        .unwrap();
        writeln!(out).unwrap();
        for error in errors {
            writeln!(out, "- {error}").unwrap();
        }
        out
    })
}

/// Render a MilestoneBundle as deterministic `plan.json` content.
pub fn render_plan_json(bundle: &MilestoneBundle) -> Result<String, serde_json::Error> {
    let canonical = validated_canonical_bundle(bundle).map_err(invalid_plan_render_error)?;
    serde_json::to_string_pretty(&canonical)
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
                        explicit_id: None,
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
                        explicit_id: None,
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
                        explicit_id: None,
                        title: "Write integration tests".to_owned(),
                        description: Some(
                            "Verify the end-to-end flow with integration coverage".to_owned(),
                        ),
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
    fn bundle_validation_accepts_legacy_priority_range() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].priority = Some(0);
        bundle.workstreams[0].beads[1].priority = Some(4);

        bundle.validate().map_err(|errors| errors.join("; "))?;
        Ok(())
    }

    #[test]
    fn generated_bundle_validation_rejects_legacy_priority_values(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].priority = Some(0);

        let errors = bundle.validate_generated().unwrap_err();

        assert!(errors.iter().any(|error| {
            error.contains("workstreams[0].beads[0].priority must be between 1 and 3")
        }));
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
    fn generated_bundle_validation_rejects_missing_required_bead_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].description = None;
        bundle.workstreams[0].beads[0].bead_type = None;
        bundle.workstreams[0].beads[0].priority = Some(4);
        bundle.workstreams[0].beads[0].labels.clear();

        let errors = bundle.validate_generated().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains(".description must not be empty")));
        assert!(errors
            .iter()
            .any(|e| e.contains(".bead_type must not be empty")));
        assert!(errors
            .iter()
            .any(|e| e.contains(".priority must be between 1 and 3")));
        assert!(errors
            .iter()
            .any(|e| e.contains(".labels must contain at least one label")));
        Ok(())
    }

    #[test]
    fn generated_bundle_validation_rejects_missing_workstream_descriptions(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].description = None;

        let errors = bundle.validate_generated().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("workstreams[0].description must not be empty")));
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
            .any(|e| e.contains("duplicates non-explicit proposal")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_duplicate_titles_for_canonicalized_implicit_beads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let rendered = render_plan_json(&bundle)?;
        let mut canonical: MilestoneBundle = serde_json::from_str(&rendered)?;
        canonical.workstreams[0].beads[1].title = canonical.workstreams[0].beads[0].title.clone();

        let errors = canonical.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("duplicates non-explicit proposal")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_duplicate_titles_for_slot_matching_bead_ids(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("bead-1".to_owned());
        bundle.workstreams[0].beads[1].bead_id = Some("ms-alpha.bead-2".to_owned());
        bundle.workstreams[0].beads[1].title = bundle.workstreams[0].beads[0].title.clone();

        let errors = bundle.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("duplicates non-explicit proposal")));
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
    fn bundle_validation_rejects_acceptance_map_mismatches(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned()];

        let errors = bundle.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("must match bead.acceptance_criteria")));
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
    fn bundle_validation_rejects_duplicate_acceptance_coverage_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].acceptance_criteria =
            vec!["AC-1".to_owned(), "AC-1".to_owned()];
        bundle.acceptance_map[0].covered_by =
            vec!["bead-1".to_owned(), "ms-alpha.bead-1".to_owned()];

        let errors = bundle.validate().unwrap_err();

        assert!(errors
            .iter()
            .any(|e| e.contains("duplicates acceptance criterion")));
        assert!(errors.iter().any(|e| e.contains("duplicates bead")));
        Ok(())
    }

    #[test]
    fn infer_workstream_order_respects_cross_workstream_dependencies() {
        let workstreams = vec![
            Workstream {
                name: "Testing".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("testing".to_owned()),
                    explicit_id: None,
                    title: "Verify release".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(2),
                    labels: vec!["testing".to_owned()],
                    depends_on: vec!["api".to_owned()],
                    acceptance_criteria: vec![],
                    flow_override: None,
                }],
            },
            Workstream {
                name: "Data Model".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("data".to_owned()),
                    explicit_id: None,
                    title: "Define schema".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["backend".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec![],
                    flow_override: None,
                }],
            },
            Workstream {
                name: "API".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("api".to_owned()),
                    explicit_id: None,
                    title: "Expose endpoints".to_owned(),
                    description: None,
                    bead_type: Some("feature".to_owned()),
                    priority: Some(1),
                    labels: vec!["api".to_owned()],
                    depends_on: vec!["data".to_owned()],
                    acceptance_criteria: vec![],
                    flow_override: None,
                }],
            },
        ];

        assert_eq!(infer_workstream_order(&workstreams), vec![1, 2, 0]);
    }

    #[test]
    fn infer_workstream_order_returns_empty_when_workstream_dependencies_interleave() {
        let workstreams = vec![
            Workstream {
                name: "API".to_owned(),
                description: None,
                beads: vec![
                    BeadProposal {
                        bead_id: Some("api-1".to_owned()),
                        explicit_id: None,
                        title: "Add endpoints".to_owned(),
                        description: None,
                        bead_type: Some("feature".to_owned()),
                        priority: Some(1),
                        labels: vec!["api".to_owned()],
                        depends_on: vec!["data-1".to_owned()],
                        acceptance_criteria: vec![],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("api-2".to_owned()),
                        explicit_id: None,
                        title: "Refine responses".to_owned(),
                        description: None,
                        bead_type: Some("task".to_owned()),
                        priority: Some(2),
                        labels: vec!["api".to_owned()],
                        depends_on: vec!["api-1".to_owned()],
                        acceptance_criteria: vec![],
                        flow_override: None,
                    },
                ],
            },
            Workstream {
                name: "Data".to_owned(),
                description: None,
                beads: vec![
                    BeadProposal {
                        bead_id: Some("data-1".to_owned()),
                        explicit_id: None,
                        title: "Create schema".to_owned(),
                        description: None,
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["backend".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec![],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("data-2".to_owned()),
                        explicit_id: None,
                        title: "Persist api fields".to_owned(),
                        description: None,
                        bead_type: Some("task".to_owned()),
                        priority: Some(2),
                        labels: vec!["backend".to_owned()],
                        depends_on: vec!["api-2".to_owned()],
                        acceptance_criteria: vec![],
                        flow_override: None,
                    },
                ],
            },
        ];

        assert!(infer_workstream_order(&workstreams).is_empty());
    }

    #[test]
    fn validate_dependency_graph_rejects_cycles() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].depends_on = vec!["bead-3".to_owned()];

        let errors = validate_dependency_graph(&bundle).expect_err("cycle should be rejected");
        assert!(errors
            .iter()
            .any(|error| error.contains("dependency cycle detected")));

        let validation_errors = bundle
            .validate()
            .expect_err("cycle should fail bundle validation");
        assert!(validation_errors
            .iter()
            .any(|error| error.contains("dependency cycle detected")));
        Ok(())
    }

    #[test]
    fn validate_dependency_graph_rejects_duplicate_normalized_bead_ids(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("ms-alpha.bead-1".to_owned());
        bundle.workstreams[0].beads[1].bead_id = Some("bead-1".to_owned());

        let errors = validate_dependency_graph(&bundle)
            .expect_err("duplicate canonical bead ids should be rejected");

        assert!(errors
            .iter()
            .any(|error| error.contains("duplicate bead identifier")));
        Ok(())
    }

    #[test]
    fn validate_dependency_graph_reports_cycles_even_with_non_graph_validation_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].depends_on = vec!["bead-3".to_owned()];
        bundle.workstreams[0].beads[1].description = None;

        let errors = validate_dependency_graph(&bundle).expect_err(
            "graph validation should still surface cycles when metadata validation fails",
        );

        assert!(errors
            .iter()
            .any(|error| error.contains("dependency cycle detected")));
        Ok(())
    }

    #[test]
    fn summarize_bundle_reports_counts_and_dependency_depth(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();

        let summary = summarize_bundle(&bundle);

        assert_eq!(summary.total_workstreams, 1);
        assert_eq!(summary.total_beads, 3);
        assert_eq!(summary.dependency_depth, 3);
        assert_eq!(summary.covered_acceptance_criteria, 2);
        assert_eq!(summary.total_acceptance_criteria, 2);
        assert_eq!(summary.acceptance_coverage_percentage, 100);
        assert_eq!(
            summary.beads_per_workstream,
            vec![WorkstreamSummary {
                name: "Core Feature".to_owned(),
                bead_count: 3,
            }]
        );
        Ok(())
    }

    #[test]
    fn summarize_bundle_derives_acceptance_coverage_from_beads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[2].acceptance_criteria.clear();

        let summary = summarize_bundle(&bundle);

        assert_eq!(summary.covered_acceptance_criteria, 1);
        assert_eq!(summary.total_acceptance_criteria, 2);
        assert_eq!(summary.acceptance_coverage_percentage, 50);
        Ok(())
    }

    #[test]
    fn generated_bundle_validation_rejects_missing_covered_by_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.acceptance_map[0].covered_by.clear();
        bundle.acceptance_map[1].covered_by.clear();

        let errors = bundle.validate_generated().unwrap_err();

        assert!(errors
            .iter()
            .any(|error| error
                .contains("acceptance_map[0].covered_by must contain at least one bead")));
        assert!(errors
            .iter()
            .any(|error| error
                .contains("acceptance_map[1].covered_by must contain at least one bead")));
        Ok(())
    }

    #[test]
    fn generated_bundle_validation_rejects_missing_bead_ids_and_acceptance_mappings(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = None;
        bundle.workstreams[0].beads[0].acceptance_criteria.clear();

        let errors = bundle.validate_generated().unwrap_err();

        assert!(errors
            .iter()
            .any(|error| error.contains("workstreams[0].beads[0].bead_id must not be empty")));
        assert!(errors.iter().any(|error| {
            error.contains(
                "workstreams[0].beads[0].acceptance_criteria must contain at least one item",
            )
        }));
        Ok(())
    }

    #[test]
    fn bundle_validation_accepts_legacy_missing_metadata_and_backfills_covered_by(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].description = None;
        bundle.workstreams[0].beads[0].bead_type = None;
        bundle.workstreams[0].beads[0].priority = Some(0);
        bundle.workstreams[0].beads[0].labels.clear();
        bundle.acceptance_map[0].covered_by.clear();
        bundle.acceptance_map[1].covered_by.clear();

        bundle.validate().map_err(|errors| errors.join("; "))?;

        let rendered = render_plan_json(&bundle)?;
        let parsed: MilestoneBundle = serde_json::from_str(&rendered)?;

        assert_eq!(
            parsed.acceptance_map[0].covered_by,
            vec!["ms-alpha.bead-1".to_owned(), "ms-alpha.bead-2".to_owned()]
        );
        assert_eq!(
            parsed.acceptance_map[1].covered_by,
            vec!["ms-alpha.bead-3".to_owned()]
        );
        assert_eq!(parsed.workstreams[0].beads[0].description, None);
        assert_eq!(parsed.workstreams[0].beads[0].bead_type, None);
        assert_eq!(parsed.workstreams[0].beads[0].priority, Some(0));
        assert!(parsed.workstreams[0].beads[0].labels.is_empty());
        Ok(())
    }

    #[test]
    fn render_plan_json_trims_acceptance_identifiers_during_canonicalization(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.acceptance_map[0].id = " AC-1 ".to_owned();
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];
        bundle.workstreams[0].beads[0].acceptance_criteria = vec![" AC-1 ".to_owned()];
        bundle.workstreams[0].beads[1].acceptance_criteria = vec!["AC-1".to_owned()];

        let rendered = render_plan_json(&bundle)?;
        let parsed: MilestoneBundle = serde_json::from_str(&rendered)?;

        assert_eq!(parsed.acceptance_map[0].id, "AC-1");
        assert_eq!(
            parsed.workstreams[0].beads[0].acceptance_criteria,
            vec!["AC-1".to_owned()]
        );
        assert_eq!(
            parsed.acceptance_map[0].covered_by,
            vec!["ms-alpha.bead-1".to_owned(), "ms-alpha.bead-2".to_owned()]
        );
        Ok(())
    }

    #[test]
    fn render_plan_json_rejects_invalid_bundle_before_canonicalizing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("bead-2".to_owned());

        let error = render_plan_json(&bundle).unwrap_err();

        assert!(error.to_string().contains("duplicate bead identifier"));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_explicit_id_true_without_bead_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].explicit_id = Some(true);

        let errors = bundle
            .validate()
            .expect_err("invalid explicit id should fail");

        assert!(errors
            .iter()
            .any(|error| error.contains("explicit_id cannot be true without bead_id")));
        Ok(())
    }

    #[test]
    fn bundle_validation_rejects_explicit_id_false_with_custom_bead_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("custom-bead".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(false);
        bundle.workstreams[0].beads[1].depends_on = vec!["custom-bead".to_owned()];
        bundle.acceptance_map[0].covered_by = vec!["custom-bead".to_owned(), "bead-2".to_owned()];

        let errors = bundle
            .validate()
            .expect_err("contradictory explicit id should fail");

        assert!(errors.iter().any(|error| {
            error.contains("explicit_id cannot be false")
                && error.contains("custom-bead")
                && error.contains("ms-alpha.bead-1")
        }));
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
        assert_eq!(
            parsed.workstreams[0].beads[0].bead_id.as_deref(),
            Some("ms-alpha.bead-1")
        );
        assert_eq!(parsed.acceptance_map[0].covered_by[0], "ms-alpha.bead-1");
        Ok(())
    }

    #[test]
    fn progress_shape_signature_accepts_legacy_plan_json_without_explicit_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let expected_shape =
            progress_shape_signature(&bundle).map_err(|errors| errors.join("; "))?;
        let hints = explicit_id_hints(&bundle).map_err(|errors| errors.join("; "))?;

        let mut legacy_json: serde_json::Value = serde_json::from_str(&render_plan_json(&bundle)?)?;
        let legacy_beads = legacy_json["workstreams"][0]["beads"]
            .as_array_mut()
            .expect("canonical plan has bead array");
        for bead in legacy_beads {
            bead.as_object_mut()
                .expect("canonical bead is an object")
                .remove("explicit_id");
        }

        let legacy_bundle: MilestoneBundle = serde_json::from_value(legacy_json)?;
        let legacy_shape =
            progress_shape_signature_with_explicit_id_hints(&legacy_bundle, Some(&hints))
                .map_err(|errors| errors.join("; "))?;

        assert_eq!(legacy_shape, expected_shape);
        Ok(())
    }

    #[test]
    fn render_plan_json_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let json1 = render_plan_json(&bundle)?;
        let json2 = render_plan_json(&bundle)?;
        assert_eq!(json1, json2, "plan.json must be deterministic");
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
    fn render_plan_md_includes_summary_dependency_graph_and_deferred_items(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].description = Some(
            "Implement the core feature set now; deferred CLI polish to a later phase so the alpha stays backend-first."
                .to_owned(),
        );

        let rendered = render_plan_md(&bundle);

        assert!(rendered.contains("## Summary"));
        assert!(rendered.contains("## Dependency Graph"));
        assert!(rendered.contains("## Deferred Items"));
        assert!(rendered.contains("**Acceptance coverage:** 2/2 (100%)"));
        assert!(rendered
            .contains("**Bead order:** ms-alpha.bead-1 -> ms-alpha.bead-2 -> ms-alpha.bead-3"));
        assert!(rendered.contains("deferred CLI polish"));
        Ok(())
    }

    #[test]
    fn render_plan_md_detects_prompt_style_deferred_phrases(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].description = Some(
            "Deliver the core backend now; follow-up work on CLI polish is not part of this milestone because the interface is still changing."
                .to_owned(),
        );

        let rendered = render_plan_md(&bundle);

        assert!(rendered.contains("## Deferred Items"));
        assert!(rendered.contains("follow-up work on CLI polish"));
        assert!(rendered.contains("not part of this milestone"));
        Ok(())
    }

    #[test]
    fn render_plan_md_surfaces_interleaved_workstream_dependencies(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bundle = MilestoneBundle {
            schema_version: MILESTONE_BUNDLE_VERSION,
            identity: MilestoneIdentity {
                id: "ms-interleaved".to_owned(),
                name: "Interleaved Milestone".to_owned(),
            },
            executive_summary: "Allow valid bead ordering even when workstreams interleave."
                .to_owned(),
            goals: vec!["Respect bead sequencing".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Interleaved sequencing is preserved.".to_owned(),
                covered_by: vec![
                    "bead-b1".to_owned(),
                    "bead-a1".to_owned(),
                    "bead-a2".to_owned(),
                    "bead-b2".to_owned(),
                ],
            }],
            workstreams: vec![
                Workstream {
                    name: "API".to_owned(),
                    description: Some(
                        "Deliver API changes without forcing a fake workstream order.".to_owned(),
                    ),
                    beads: vec![
                        BeadProposal {
                            bead_id: Some("bead-a1".to_owned()),
                            explicit_id: None,
                            title: "Implement API step one".to_owned(),
                            description: Some("Depends on the initial data model work.".to_owned()),
                            bead_type: Some("feature".to_owned()),
                            priority: Some(1),
                            labels: vec!["api".to_owned()],
                            depends_on: vec!["bead-b1".to_owned()],
                            acceptance_criteria: vec!["AC-1".to_owned()],
                            flow_override: None,
                        },
                        BeadProposal {
                            bead_id: Some("bead-a2".to_owned()),
                            explicit_id: None,
                            title: "Implement API step two".to_owned(),
                            description: Some(
                                "Completes the API slice before tests resume.".to_owned(),
                            ),
                            bead_type: Some("task".to_owned()),
                            priority: Some(1),
                            labels: vec!["api".to_owned()],
                            depends_on: vec!["bead-a1".to_owned()],
                            acceptance_criteria: vec!["AC-1".to_owned()],
                            flow_override: None,
                        },
                    ],
                },
                Workstream {
                    name: "Data".to_owned(),
                    description: Some(
                        "Own the storage prerequisites and the final follow-up.".to_owned(),
                    ),
                    beads: vec![
                        BeadProposal {
                            bead_id: Some("bead-b1".to_owned()),
                            explicit_id: None,
                            title: "Prepare storage".to_owned(),
                            description: Some("Unblocks the API slice.".to_owned()),
                            bead_type: Some("task".to_owned()),
                            priority: Some(1),
                            labels: vec!["backend".to_owned()],
                            depends_on: vec![],
                            acceptance_criteria: vec!["AC-1".to_owned()],
                            flow_override: None,
                        },
                        BeadProposal {
                            bead_id: Some("bead-b2".to_owned()),
                            explicit_id: None,
                            title: "Finalize storage".to_owned(),
                            description: Some("Resumes after the API work lands.".to_owned()),
                            bead_type: Some("task".to_owned()),
                            priority: Some(2),
                            labels: vec!["backend".to_owned()],
                            depends_on: vec!["bead-a2".to_owned()],
                            acceptance_criteria: vec!["AC-1".to_owned()],
                            flow_override: None,
                        },
                    ],
                },
            ],
            default_flow: FlowPreset::QuickDev,
            agents_guidance: None,
        };

        let rendered = render_plan_md(&bundle);

        assert!(rendered.contains("No single topological workstream order"));
        assert!(rendered.contains("API, Data"));
        assert!(rendered.contains(
            "**Bead order:** ms-interleaved.bead-b1 -> ms-interleaved.bead-a1 -> ms-interleaved.bead-a2 -> ms-interleaved.bead-b2"
        ));
        Ok(())
    }

    #[test]
    fn slot_matching_bead_ids_without_explicit_flag_stay_implicit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bundle = MilestoneBundle {
            schema_version: MILESTONE_BUNDLE_VERSION,
            identity: MilestoneIdentity {
                id: "ms-qualified".to_owned(),
                name: "Qualified".to_owned(),
            },
            executive_summary: "Check qualified ids.".to_owned(),
            goals: vec!["Preserve explicit bead identity".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Qualified explicit ids remain explicit.".to_owned(),
                covered_by: vec!["ms-qualified.bead-1".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Planning".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("ms-qualified.bead-1".to_owned()),
                    explicit_id: None,
                    title: "Preserve explicit id".to_owned(),
                    description: Some("Planner supplied a fully-qualified bead id.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["planning".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: FlowPreset::Minimal,
            agents_guidance: None,
        };

        let rendered = render_plan_json(&bundle)?;
        let parsed: MilestoneBundle = serde_json::from_str(&rendered)?;
        assert_eq!(parsed.workstreams[0].beads[0].explicit_id, Some(false));
        Ok(())
    }

    #[test]
    fn non_slot_matching_bead_ids_without_explicit_flag_stay_explicit(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("custom-bead".to_owned());
        bundle.workstreams[0].beads[1].depends_on = vec!["custom-bead".to_owned()];
        bundle.acceptance_map[0].covered_by = vec!["custom-bead".to_owned(), "bead-2".to_owned()];

        let rendered = render_plan_json(&bundle)?;
        let parsed: MilestoneBundle = serde_json::from_str(&rendered)?;

        assert_eq!(parsed.workstreams[0].beads[0].explicit_id, Some(true));
        Ok(())
    }

    #[test]
    fn planned_bead_membership_refs_include_explicit_short_ids(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.workstreams[0].beads[0].bead_id = Some("bead-e2e".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        bundle.workstreams[0].beads[1].depends_on = vec!["bead-e2e".to_owned()];
        bundle.acceptance_map[0].covered_by = vec!["bead-e2e".to_owned(), "bead-2".to_owned()];

        let refs = planned_bead_membership_refs(&bundle).map_err(|errors| errors.join("; "))?;

        assert!(refs.contains("ms-alpha.bead-e2e"));
        assert!(refs.contains("bead-e2e"));
        assert!(refs.contains("ms-alpha.bead-2"));
        assert!(!refs.contains("bead-2"));
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
        assert!(md.contains("- **Milestone ID:** ms-alpha"));
        assert!(md.contains("quick_dev"));
        Ok(())
    }

    #[test]
    fn plan_md_contains_bead_table() -> Result<(), Box<dyn std::error::Error>> {
        let bundle = sample_bundle();
        let md = render_plan_md(&bundle);
        assert!(md.contains("| ID | Criterion | Workstreams | Covered By Beads |"));
        assert!(md.contains(
            "| # | Bead ID | Title | Type | Priority | Acceptance | Dependencies | Flow |"
        ));
        assert!(md.contains("ms-alpha.bead-1"));
        assert!(md.contains("AC-1"));
        assert!(md.contains("Implement data model"));
        assert!(md.contains("Build API endpoints"));
        Ok(())
    }

    #[test]
    fn plan_md_escapes_table_cells() -> Result<(), Box<dyn std::error::Error>> {
        let mut bundle = sample_bundle();
        bundle.acceptance_map[0].description = "Criterion with |\nand newline".to_owned();
        bundle.workstreams[0].name = "Core | Stream".to_owned();
        bundle.workstreams[0].beads[0].title = "Implement |\nmodel".to_owned();
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];

        let md = render_plan_md(&bundle);

        assert!(md.contains("Criterion with \\|<br>and newline"));
        assert!(md.contains("Core \\| Stream"));
        assert!(md.contains("Implement \\|<br>model"));
        assert!(md.contains("ms-alpha.bead-1 (Implement \\|<br>model)"));
        Ok(())
    }

    #[test]
    fn plan_md_disambiguates_duplicate_workstream_names() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut bundle = sample_bundle();
        bundle.workstreams.push(Workstream {
            name: "Core Feature".to_owned(),
            description: Some("Follow-up workstream".to_owned()),
            beads: vec![BeadProposal {
                bead_id: Some("bead-4".to_owned()),
                explicit_id: None,
                title: "Ship polish".to_owned(),
                description: Some(
                    "Polish the shipped milestone after the core work lands.".to_owned(),
                ),
                bead_type: Some("task".to_owned()),
                priority: Some(2),
                labels: vec!["polish".to_owned()],
                depends_on: vec!["bead-3".to_owned()],
                acceptance_criteria: vec!["AC-2".to_owned()],
                flow_override: None,
            }],
        });
        bundle.acceptance_map[1].covered_by = vec!["bead-3".to_owned(), "bead-4".to_owned()];

        let md = render_plan_md(&bundle);

        assert!(md.contains("### Core Feature [1]"));
        assert!(md.contains("### Core Feature [2]"));
        assert!(md.contains("Core Feature [1], Core Feature [2]"));
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

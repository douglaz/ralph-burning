#![forbid(unsafe_code)]

//! Shared template catalog with precedence resolution, placeholder validation,
//! and safe rendering for workflow and requirements prompt surfaces.
//!
//! Template resolution order (deterministic, singular — layers are never merged):
//! 1. Project override: `.ralph-burning/projects/<project-id>/templates/<template-id>.md`
//! 2. Workspace override: `.ralph-burning/templates/<template-id>.md`
//! 3. Built-in default template
//!
//! Override files use `{{placeholder}}` syntax for pre-rendered blocks.
//! Malformed overrides (unknown placeholders, missing required placeholders,
//! unreadable files, non-UTF-8 content) cause a hard failure — no silent fallback.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::adapters::fs::FileSystem;
use crate::shared::domain::{ProjectId, StageId};
use crate::shared::error::{AppError, AppResult};

// ── Template IDs ────────────────────────────────────────────────────────────

/// Workflow stage template IDs (match `StageId::as_str()`).
pub const STAGE_TEMPLATE_IDS: &[&str] = &[
    "prompt_review",
    "planning",
    "implementation",
    "qa",
    "review",
    "completion_panel",
    "acceptance_qa",
    "final_review",
    "plan_and_implement",
    "apply_fixes",
    "docs_plan",
    "docs_update",
    "docs_validation",
    "ci_plan",
    "ci_update",
    "ci_validation",
];

/// Panel template IDs.
pub const PANEL_TEMPLATE_IDS: &[&str] = &[
    "prompt_review_refiner",
    "prompt_review_validator",
    "completion_panel_completer",
    "final_review_reviewer",
    "final_review_voter",
    "final_review_arbiter",
];

/// Requirements template IDs.
pub const REQUIREMENTS_TEMPLATE_IDS: &[&str] = &[
    "requirements_draft",
    "requirements_review",
    "requirements_question_set",
    "requirements_project_seed",
    "requirements_milestone_bundle",
    "requirements_ideation",
    "requirements_research",
    "requirements_synthesis",
    "requirements_implementation_spec",
    "requirements_gap_analysis",
    "requirements_validation",
];

// ── Template manifests ──────────────────────────────────────────────────────

/// Describes a template's placeholder contract.
#[derive(Debug, Clone)]
pub struct TemplateManifest {
    pub template_id: &'static str,
    pub required_placeholders: &'static [&'static str],
    pub optional_placeholders: &'static [&'static str],
    pub built_in_default: &'static str,
}

/// Where a resolved template came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateSource {
    BuiltIn,
    WorkspaceOverride(PathBuf),
    ProjectOverride(PathBuf),
}

/// A resolved template ready for rendering.
#[derive(Debug, Clone)]
pub struct ResolvedTemplate {
    pub template_id: String,
    pub source: TemplateSource,
    pub content: String,
    pub manifest: TemplateManifest,
}

// ── Built-in default templates ──────────────────────────────────────────────

// Stage templates share the same structure.
const STAGE_DEFAULT_TEMPLATE: &str = "\
# Stage Execution Prompt

{{role_instruction}}

{{task_prompt_contract}}\
\n\n\
## Original Project Prompt

{{project_prompt}}\
\n\n{{prior_outputs}}\
\n\n{{remediation}}\
\n\n{{classification_guidance}}\
\n\n## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const PROMPT_REVIEW_REFINER_DEFAULT: &str = "\
# Prompt Review: {{role_label}}

You are a prompt reviewer evaluating a project prompt for clarity, completeness, \
feasibility, and testability.

Your job is to identify gaps and then rewrite the prompt so downstream \
implementation loops can execute with minimal ambiguity.

## Instructions

1. Read the prompt below carefully.
2. Identify issues: vague requirements, missing acceptance criteria, \
untestable claims, implicit assumptions, and scope gaps.
3. For each issue, explain why it matters for downstream implementation.
4. Produce a refined prompt that resolves all identified issues.

Return only JSON. Put the rewritten prompt in `refined_prompt`, summarize \
the changes in `refinement_summary`, and list the key improvements in `improvements`.

{{task_prompt_contract}}\
\n\n\
{{prior_concerns}}
## Prompt to Review

{{prompt_text}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const PROMPT_REVIEW_VALIDATOR_DEFAULT: &str = "\
# Prompt Review Validation: {{role_label}}

You are a prompt review validator deciding whether a refined prompt is acceptable \
for downstream implementation loops.

## Instructions

1. Assess the prompt below for clarity, completeness, feasibility, and actionability.
2. Verify it contains explicit acceptance criteria, has no vague or untestable \
requirements, and could be handed to an implementation agent without further clarification.
3. Check that no critical requirements appear to be missing or under-specified.
4. If rejecting, your reason must be specific and actionable — explain exactly \
what is wrong and how to fix it.

{{task_prompt_contract}}\
\n\n\
## Prompt to Validate

{{prompt_text}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const COMPLETION_PANEL_COMPLETER_DEFAULT: &str = "\
# Completion Vote

You are a project completion validator.

The Planner has suggested the project is complete. Your job is to:
1. Review all requirements in the project prompt below.
2. Assess whether every required feature appears to have been implemented \
based on the context provided.
3. Verify nothing is missing or only partially done.

Approach this as an independent verification — do not simply confirm the \
planning output. Scrutinize each requirement separately and look for gaps \
or partial implementations. If you have access to tools that can inspect \
the repository, use them for grounded evidence.

If all requirements are satisfied, vote COMPLETE with evidence mapping each \
requirement to the feature that satisfies it.

If requirements are missing or incomplete, vote CONTINUE and list what is \
still needed.

{{task_prompt_contract}}\
\n\n\
## Project Prompt

{{prompt_text}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const FINAL_REVIEW_REVIEWER_DEFAULT: &str = "\
# Final Review Proposals

You are a code reviewer. Review the changes in this project for correctness, \
safety, and robustness.

Run `git diff` to see all changes, then read the key implementation files \
end-to-end. For each issue found, cite specific files and line numbers.

Ignore style and cosmetics — only report real bugs, safety problems, \
correctness gaps, and significant maintainability risks.

{{classification_guidance}}\
\n\n{{task_prompt_contract}}\
\n\n\
## Project Prompt

{{project_prompt}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const FINAL_REVIEW_VOTER_DEFAULT: &str = "\
# {{title}}

You are evaluating proposed amendments to this project. Assess each amendment \
on its technical merit — correctness, safety, robustness, and maintainability.

## Instructions

1. Consider each amendment carefully on its own merits.
2. Vote ACCEPT or REJECT for each amendment with a clear rationale.
3. Do NOT reject amendments because they are \"out of scope\" or \"beyond the \
original spec\" — any real bug or safety issue is valid regardless of scope.
4. Do NOT dismiss concurrency or isolation issues as \"theoretical\" just \
because they don't currently cause failures — shared mutable state is a \
defect even if current callers happen to be read-only.

## Proposed Amendments

{{amendments}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const FINAL_REVIEW_ARBITER_DEFAULT: &str = "\
# Final Review Arbiter

You are the arbiter resolving disputed amendments where reviewers disagree.

## Instructions

1. Consider each disputed amendment carefully.
2. Read the reviewer votes for context.
3. Make a final ruling: ACCEPT or REJECT for each disputed amendment.
4. Provide clear rationale for each ruling, citing the specific evidence \
that tips the balance.

## Disputed Amendments

{{amendments}}

## Reviewer Votes

{{reviewer_votes}}

## Authoritative JSON Schema

The JSON schema below is authoritative. Return only JSON that conforms exactly to it.

```json
{{json_schema}}
```";

const REQUIREMENTS_DRAFT_DEFAULT: &str = "\
Draft requirements for the following idea:

{{idea}}
{{answers}}";

const REQUIREMENTS_REVIEW_DEFAULT: &str = "\
Review the following requirements draft:

{{draft_artifact}}";

const REQUIREMENTS_QUESTION_SET_DEFAULT: &str = "\
Generate clarifying questions about the following missing information for the idea:

{{idea}}

Missing information:
{{missing_info}}";

const REQUIREMENTS_PROJECT_SEED_DEFAULT: &str = "\
Generate a project seed from the following requirements:

{{requirements_artifact}}

Follow-ups: {{follow_ups}}";

const REQUIREMENTS_MILESTONE_BUNDLE_DEFAULT: &str = "\
Generate a milestone bundle from the validated planning artifacts:

Produce a single additive planning bundle that preserves the milestone contract exactly as defined by the schema.
Group the plan into cohesive workstreams by theme or layer, such as data model, API, CLI, automation, or testing.
For each workstream, include a short description that explains the theme, sequencing intent, and any intentionally deferred scope or follow-up work that is not part of this milestone.
For each bead proposal:
- assign a non-empty stable bead_id on every bead as authored identity, not a slot placeholder derived from workstream order, and reuse that exact ID anywhere the bead is referenced in depends_on or acceptance_map.covered_by
- write a concrete title
- write a scoped description with rationale, implementation boundary, and any explicit deferred work notes
- choose an appropriate bead_type
- assign numeric priority values: 1 for P1 critical path, 2 for P2 important follow-on work, and 3 for P3 nice-to-have or polish
- add one or more relevant labels for the subsystem, execution mode, or quality area
- include depends_on bead IDs only when a real sequencing constraint exists, and explain the dependency rationale in the description
- map every bead to at least one acceptance criteria ID it materially advances
Acceptance coverage must be bidirectional and exact:
- every bead's acceptance_criteria entries must correspond to milestone acceptance criteria
- every acceptance criterion covered_by entry must list the matching bead IDs, with no omissions or extras
Use dependency hints to reflect actual execution order, not thematic similarity.
Call out intentionally deferred work in workstream or bead descriptions, including why it is deferred.
Keep the plan implementation-ready and deterministic enough for downstream rendering and validation.

Synthesis:
{{synthesis_artifact}}

Implementation Spec:
{{impl_spec_artifact}}

Gap Analysis:
{{gap_artifact}}

Validation:
{{validation_artifact}}";

const REQUIREMENTS_IDEATION_DEFAULT: &str = "\
Explore themes and initial scope for the following idea:

{{base_context}}";

const REQUIREMENTS_RESEARCH_DEFAULT: &str = "\
Research background and technical context for:

{{base_context}}

Ideation output:
{{ideation_artifact}}";

const REQUIREMENTS_SYNTHESIS_DEFAULT: &str = "\
Synthesize requirements from ideation and research:

Idea: {{base_context}}

Ideation:
{{ideation_artifact}}

Research:
{{research_artifact}}";

const REQUIREMENTS_IMPLEMENTATION_SPEC_DEFAULT: &str = "\
Create an implementation specification from the synthesized requirements:

{{synthesis_artifact}}";

const REQUIREMENTS_GAP_ANALYSIS_DEFAULT: &str = "\
Analyze gaps between requirements and implementation spec:

Synthesis:
{{synthesis_artifact}}

Implementation Spec:
{{impl_spec_artifact}}";

const REQUIREMENTS_VALIDATION_DEFAULT: &str = "\
Validate the requirements pipeline output:

Synthesis:
{{synthesis_artifact}}

Implementation Spec:
{{impl_spec_artifact}}

Gap Analysis:
{{gap_artifact}}";

// ── Manifest registry ───────────────────────────────────────────────────────

/// Stage template placeholders (shared by all stage IDs).
const STAGE_REQUIRED: &[&str] = &["role_instruction", "project_prompt", "json_schema"];
const STAGE_OPTIONAL: &[&str] = &[
    "task_prompt_contract",
    "prior_outputs",
    "remediation",
    "classification_guidance",
];

/// Return the manifest for a known template ID, or `None` if unrecognized.
pub fn manifest_for(template_id: &str) -> Option<TemplateManifest> {
    // Stage templates
    if STAGE_TEMPLATE_IDS.contains(&template_id) {
        return Some(TemplateManifest {
            template_id: stage_id_to_static(template_id),
            required_placeholders: STAGE_REQUIRED,
            optional_placeholders: STAGE_OPTIONAL,
            built_in_default: STAGE_DEFAULT_TEMPLATE,
        });
    }

    // Panel templates
    let panel_result = match template_id {
        "prompt_review_refiner" => Some(TemplateManifest {
            template_id: "prompt_review_refiner",
            required_placeholders: &["role_label", "prompt_text", "json_schema"],
            optional_placeholders: &["task_prompt_contract", "prior_concerns"],
            built_in_default: PROMPT_REVIEW_REFINER_DEFAULT,
        }),
        "prompt_review_validator" => Some(TemplateManifest {
            template_id: "prompt_review_validator",
            required_placeholders: &["role_label", "prompt_text", "json_schema"],
            optional_placeholders: &["task_prompt_contract"],
            built_in_default: PROMPT_REVIEW_VALIDATOR_DEFAULT,
        }),
        "completion_panel_completer" => Some(TemplateManifest {
            template_id: "completion_panel_completer",
            required_placeholders: &["prompt_text", "json_schema"],
            optional_placeholders: &["task_prompt_contract"],
            built_in_default: COMPLETION_PANEL_COMPLETER_DEFAULT,
        }),
        "final_review_reviewer" => Some(TemplateManifest {
            template_id: "final_review_reviewer",
            required_placeholders: &["project_prompt", "json_schema"],
            optional_placeholders: &["task_prompt_contract", "classification_guidance"],
            built_in_default: FINAL_REVIEW_REVIEWER_DEFAULT,
        }),
        "final_review_voter" => Some(TemplateManifest {
            template_id: "final_review_voter",
            required_placeholders: &["title", "amendments", "json_schema"],
            optional_placeholders: &[],
            built_in_default: FINAL_REVIEW_VOTER_DEFAULT,
        }),
        "final_review_arbiter" => Some(TemplateManifest {
            template_id: "final_review_arbiter",
            required_placeholders: &["amendments", "reviewer_votes", "json_schema"],
            optional_placeholders: &[],
            built_in_default: FINAL_REVIEW_ARBITER_DEFAULT,
        }),
        _ => None,
    };
    if panel_result.is_some() {
        return panel_result;
    }

    // Requirements templates
    match template_id {
        "requirements_draft" => Some(TemplateManifest {
            template_id: "requirements_draft",
            required_placeholders: &["idea"],
            optional_placeholders: &["answers"],
            built_in_default: REQUIREMENTS_DRAFT_DEFAULT,
        }),
        "requirements_review" => Some(TemplateManifest {
            template_id: "requirements_review",
            required_placeholders: &["draft_artifact"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_REVIEW_DEFAULT,
        }),
        "requirements_question_set" => Some(TemplateManifest {
            template_id: "requirements_question_set",
            required_placeholders: &["idea", "missing_info"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_QUESTION_SET_DEFAULT,
        }),
        "requirements_project_seed" => Some(TemplateManifest {
            template_id: "requirements_project_seed",
            required_placeholders: &["requirements_artifact", "follow_ups"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_PROJECT_SEED_DEFAULT,
        }),
        "requirements_milestone_bundle" => Some(TemplateManifest {
            template_id: "requirements_milestone_bundle",
            required_placeholders: &[
                "synthesis_artifact",
                "impl_spec_artifact",
                "gap_artifact",
                "validation_artifact",
            ],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_MILESTONE_BUNDLE_DEFAULT,
        }),
        "requirements_ideation" => Some(TemplateManifest {
            template_id: "requirements_ideation",
            required_placeholders: &["base_context"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_IDEATION_DEFAULT,
        }),
        "requirements_research" => Some(TemplateManifest {
            template_id: "requirements_research",
            required_placeholders: &["base_context", "ideation_artifact"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_RESEARCH_DEFAULT,
        }),
        "requirements_synthesis" => Some(TemplateManifest {
            template_id: "requirements_synthesis",
            required_placeholders: &["base_context", "ideation_artifact", "research_artifact"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_SYNTHESIS_DEFAULT,
        }),
        "requirements_implementation_spec" => Some(TemplateManifest {
            template_id: "requirements_implementation_spec",
            required_placeholders: &["synthesis_artifact"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_IMPLEMENTATION_SPEC_DEFAULT,
        }),
        "requirements_gap_analysis" => Some(TemplateManifest {
            template_id: "requirements_gap_analysis",
            required_placeholders: &["synthesis_artifact", "impl_spec_artifact"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_GAP_ANALYSIS_DEFAULT,
        }),
        "requirements_validation" => Some(TemplateManifest {
            template_id: "requirements_validation",
            required_placeholders: &["synthesis_artifact", "impl_spec_artifact", "gap_artifact"],
            optional_placeholders: &[],
            built_in_default: REQUIREMENTS_VALIDATION_DEFAULT,
        }),
        _ => None,
    }
}

/// Map a stage template_id string to its `&'static str` constant.
fn stage_id_to_static(id: &str) -> &'static str {
    STAGE_TEMPLATE_IDS
        .iter()
        .find(|&&s| s == id)
        .copied()
        .unwrap_or("unknown") // should never hit; all stage IDs are in the const list
}

/// Return the template ID for a workflow stage.
pub fn stage_template_id(stage_id: StageId) -> &'static str {
    stage_id.as_str()
}

/// Return the template ID for a requirements stage.
pub fn requirements_template_id(stage_id_str: &str) -> String {
    match stage_id_str {
        "requirements_draft" | "requirements_review" => stage_id_str.to_owned(),
        "question_set" => "requirements_question_set".to_owned(),
        "project_seed" => "requirements_project_seed".to_owned(),
        "milestone_bundle" => "requirements_milestone_bundle".to_owned(),
        other => format!("requirements_{other}"),
    }
}

// ── Resolution ──────────────────────────────────────────────────────────────

/// Filesystem path for the preferred workspace-level template override.
pub fn workspace_template_path(base_dir: &Path, template_id: &str) -> PathBuf {
    FileSystem::workspace_root_path(base_dir)
        .join("templates")
        .join(format!("{template_id}.md"))
}

fn workspace_template_candidate_paths(base_dir: &Path, template_id: &str) -> Vec<PathBuf> {
    [
        FileSystem::live_workspace_root_path(base_dir),
        FileSystem::audit_workspace_root_path(base_dir),
    ]
    .into_iter()
    .map(|root| root.join("templates").join(format!("{template_id}.md")))
    .fold(Vec::new(), |mut acc, path| {
        if !acc.contains(&path) {
            acc.push(path);
        }
        acc
    })
}

/// Filesystem path for the preferred project-level template override.
pub fn project_template_path(
    base_dir: &Path,
    project_id: &ProjectId,
    template_id: &str,
) -> PathBuf {
    FileSystem::project_root(base_dir, project_id)
        .join("templates")
        .join(format!("{template_id}.md"))
}

fn project_template_candidate_paths(
    base_dir: &Path,
    project_id: &ProjectId,
    template_id: &str,
) -> Vec<PathBuf> {
    [
        FileSystem::live_workspace_root_path(base_dir)
            .join("projects")
            .join(project_id.as_str()),
        FileSystem::audit_workspace_root_path(base_dir)
            .join("projects")
            .join(project_id.as_str()),
    ]
    .into_iter()
    .map(|root| root.join("templates").join(format!("{template_id}.md")))
    .fold(Vec::new(), |mut acc, path| {
        if !acc.contains(&path) {
            acc.push(path);
        }
        acc
    })
}

fn resolve_override_candidate(
    path: &Path,
    manifest: &TemplateManifest,
) -> AppResult<Option<String>> {
    match path.try_exists() {
        Ok(true) => {
            let content = read_override_file(path)?;
            validate_template(&content, manifest, path)?;
            Ok(Some(content))
        }
        Ok(false) => {
            if path.symlink_metadata().is_ok() {
                Err(AppError::MalformedTemplate {
                    path: path.display().to_string(),
                    reason: "template override is a broken symlink".to_owned(),
                })
            } else {
                Ok(None)
            }
        }
        Err(e) => Err(AppError::MalformedTemplate {
            path: path.display().to_string(),
            reason: format!("template override is inaccessible: {e}"),
        }),
    }
}

/// Resolve a template by precedence: project override → workspace override → built-in.
///
/// Returns `Err` if an override file exists but is unreadable or non-UTF-8.
/// Does **not** silently fall back to a lower layer on malformed overrides.
pub fn resolve(
    template_id: &str,
    base_dir: &Path,
    project_id: Option<&ProjectId>,
) -> AppResult<ResolvedTemplate> {
    let manifest = manifest_for(template_id).ok_or_else(|| AppError::MalformedTemplate {
        path: template_id.to_owned(),
        reason: format!("unknown template ID '{template_id}'"),
    })?;

    // 1. Check project override
    //    Use try_exists() to distinguish "not present" from "inaccessible"
    //    (broken symlink, permission denied). Inaccessible overrides are
    //    hard errors — no silent fallback to lower precedence.
    if let Some(pid) = project_id {
        for path in project_template_candidate_paths(base_dir, pid, template_id) {
            if let Some(content) = resolve_override_candidate(&path, &manifest)? {
                return Ok(ResolvedTemplate {
                    template_id: template_id.to_owned(),
                    source: TemplateSource::ProjectOverride(path),
                    content,
                    manifest,
                });
            }
        }
    }

    // 2. Check workspace override
    for ws_path in workspace_template_candidate_paths(base_dir, template_id) {
        if let Some(content) = resolve_override_candidate(&ws_path, &manifest)? {
            return Ok(ResolvedTemplate {
                template_id: template_id.to_owned(),
                source: TemplateSource::WorkspaceOverride(ws_path),
                content,
                manifest,
            });
        }
    }

    // 3. Built-in default
    Ok(ResolvedTemplate {
        template_id: template_id.to_owned(),
        source: TemplateSource::BuiltIn,
        content: manifest.built_in_default.to_owned(),
        manifest,
    })
}

// ── Validation ──────────────────────────────────────────────────────────────

/// Read an override file, rejecting non-UTF-8 and unreadable files.
fn read_override_file(path: &Path) -> AppResult<String> {
    let bytes = std::fs::read(path).map_err(|e| AppError::MalformedTemplate {
        path: path.display().to_string(),
        reason: format!("cannot read file: {e}"),
    })?;
    String::from_utf8(bytes).map_err(|_| AppError::MalformedTemplate {
        path: path.display().to_string(),
        reason: "file is not valid UTF-8".to_owned(),
    })
}

/// Validate that the template text uses only known placeholders and includes
/// all required ones.
fn validate_template(content: &str, manifest: &TemplateManifest, path: &Path) -> AppResult<()> {
    // Extract ALL marker-shaped tokens (including those with invalid names
    // like hyphens or spaces) so we can reject them.
    let all_tokens = extract_all_marker_tokens(content);
    let valid_placeholders = extract_placeholders(content);

    let allowed: HashSet<&str> = manifest
        .required_placeholders
        .iter()
        .chain(manifest.optional_placeholders.iter())
        .copied()
        .collect();

    // Reject any marker token whose name contains invalid characters
    // (not alphanumeric or underscore).  These are placeholder-shaped but
    // cannot be legitimate placeholders.
    for token in &all_tokens {
        if !valid_placeholders.contains(token) {
            return Err(AppError::MalformedTemplate {
                path: path.display().to_string(),
                reason: format!("unknown placeholder '{{{{{}}}}}' in template", token),
            });
        }
    }

    // Check for unknown placeholders (valid name but not in manifest)
    for ph in &valid_placeholders {
        if !allowed.contains(ph.as_str()) {
            return Err(AppError::MalformedTemplate {
                path: path.display().to_string(),
                reason: format!("unknown placeholder '{{{{{}}}}}' in template", ph),
            });
        }
    }

    // Check for missing required placeholders
    for &req in manifest.required_placeholders {
        if !valid_placeholders.contains(req) {
            return Err(AppError::MalformedTemplate {
                path: path.display().to_string(),
                reason: format!("missing required placeholder '{{{{{}}}}}' in template", req),
            });
        }
    }

    Ok(())
}

/// Extract all `{{placeholder_name}}` occurrences from template text.
///
/// Only returns placeholders with valid names (alphanumeric + underscore).
/// Use [`extract_all_marker_tokens`] to find every `{{...}}` token including
/// those with invalid characters.
pub fn extract_placeholders(content: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    let mut remaining = content;
    while let Some(start) = remaining.find("{{") {
        let after_open = &remaining[start + 2..];
        if let Some(end) = after_open.find("}}") {
            let name = after_open[..end].trim();
            if !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_') {
                result.insert(name.to_owned());
            }
            remaining = &after_open[end + 2..];
        } else {
            break;
        }
    }
    result
}

/// Extract every `{{...}}` token from template text, including those with
/// invalid placeholder names (spaces, hyphens, etc.).  Used by validation
/// to reject any placeholder-shaped marker not in the manifest.
fn extract_all_marker_tokens(content: &str) -> HashSet<String> {
    let mut result = HashSet::new();
    let mut remaining = content;
    while let Some(start) = remaining.find("{{") {
        let after_open = &remaining[start + 2..];
        if let Some(end) = after_open.find("}}") {
            let name = after_open[..end].trim();
            if !name.is_empty() {
                result.insert(name.to_owned());
            }
            remaining = &after_open[end + 2..];
        } else {
            break;
        }
    }
    result
}

// ── Rendering ───────────────────────────────────────────────────────────────

/// Render a resolved template with the given placeholder values.
///
/// Replaces each `{{name}}` with its value. Optional placeholders that are
/// not supplied expand to empty string. Collapses runs of 3+ consecutive
/// newlines to exactly 2.
pub fn render(resolved: &ResolvedTemplate, values: &[(&str, &str)]) -> AppResult<String> {
    let values_map: HashMap<&str, &str> = values.iter().copied().collect();
    let mut output = resolved.content.clone();

    // Replace all placeholders
    let all_placeholders: Vec<&str> = resolved
        .manifest
        .required_placeholders
        .iter()
        .chain(resolved.manifest.optional_placeholders.iter())
        .copied()
        .collect();

    // Single-pass scan: find each `{{...}}` marker, look up its trimmed name
    // in the values map, and replace it. This avoids re-processing content
    // already inserted by earlier replacements (P2) and handles arbitrary
    // internal whitespace (P3) since validate_template() trims names.
    let mut result = String::with_capacity(output.len());
    let mut remaining = output.as_str();
    while let Some(start) = remaining.find("{{") {
        result.push_str(&remaining[..start]);
        let after_open = &remaining[start + 2..];
        if let Some(end) = after_open.find("}}") {
            let raw_name = &after_open[..end];
            let trimmed = raw_name.trim();
            if let Some(value) = values_map.get(trimmed) {
                result.push_str(value);
            } else if all_placeholders.contains(&trimmed) {
                // Known optional placeholder not supplied — expand to empty
            } else {
                // Not a known placeholder — preserve the original marker
                result.push_str("{{");
                result.push_str(raw_name);
                result.push_str("}}");
            }
            remaining = &after_open[end + 2..];
        } else {
            // Unclosed `{{` — preserve and stop
            result.push_str(&remaining[start..]);
            remaining = "";
            break;
        }
    }
    result.push_str(remaining);
    output = result;

    // Collapse 3+ consecutive newlines to 2
    output = collapse_blank_lines(&output);

    Ok(output)
}

/// Collapse runs of 3 or more consecutive newlines to exactly 2 (one blank line).
fn collapse_blank_lines(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut consecutive_newlines = 0u32;

    for ch in text.chars() {
        if ch == '\n' {
            consecutive_newlines += 1;
            if consecutive_newlines <= 2 {
                result.push(ch);
            }
        } else {
            consecutive_newlines = 0;
            result.push(ch);
        }
    }

    result
}

// ── Convenience: resolve and render in one call ─────────────────────────────

/// Resolve a template and render it with the given placeholder values.
///
/// This is the primary entry point for prompt surfaces that need to produce
/// a prompt string from a template ID and placeholder values.
pub fn resolve_and_render(
    template_id: &str,
    base_dir: &Path,
    project_id: Option<&ProjectId>,
    values: &[(&str, &str)],
) -> AppResult<String> {
    let resolved = resolve(template_id, base_dir, project_id)?;
    render(&resolved, values)
}

/// Check whether any override exists for a template ID without reading it.
pub fn has_override(template_id: &str, base_dir: &Path, project_id: Option<&ProjectId>) -> bool {
    if let Some(pid) = project_id {
        if project_template_candidate_paths(base_dir, pid, template_id)
            .iter()
            .any(|path| path.exists())
        {
            return true;
        }
    }
    workspace_template_candidate_paths(base_dir, template_id)
        .iter()
        .any(|path| path.exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_placeholders_basic() {
        let content = "Hello {{name}}, your {{role}} is ready.";
        let phs = extract_placeholders(content);
        assert!(phs.contains("name"));
        assert!(phs.contains("role"));
        assert_eq!(phs.len(), 2);
    }

    #[test]
    fn extract_placeholders_with_underscores() {
        let content = "{{role_instruction}} and {{json_schema}}";
        let phs = extract_placeholders(content);
        assert!(phs.contains("role_instruction"));
        assert!(phs.contains("json_schema"));
    }

    #[test]
    fn extract_placeholders_ignores_invalid_names() {
        let content = "{{}} and {{with spaces}} and {{valid}}";
        let phs = extract_placeholders(content);
        assert_eq!(phs.len(), 1);
        assert!(phs.contains("valid"));
    }

    #[test]
    fn extract_all_marker_tokens_captures_invalid_names() {
        let content = "{{}} and {{with spaces}} and {{valid}} and {{hyphen-name}}";
        let all = extract_all_marker_tokens(content);
        assert_eq!(all.len(), 3);
        assert!(all.contains("with spaces"));
        assert!(all.contains("valid"));
        assert!(all.contains("hyphen-name"));
        // empty `{{}}` is excluded because the trimmed name is empty
    }

    #[test]
    fn collapse_blank_lines_preserves_single() {
        assert_eq!(collapse_blank_lines("a\n\nb"), "a\n\nb");
    }

    #[test]
    fn collapse_blank_lines_collapses_triple() {
        assert_eq!(collapse_blank_lines("a\n\n\nb"), "a\n\nb");
    }

    #[test]
    fn collapse_blank_lines_collapses_many() {
        assert_eq!(collapse_blank_lines("a\n\n\n\n\nb"), "a\n\nb");
    }

    #[test]
    fn manifest_for_known_stage() {
        let m = manifest_for("planning").expect("planning manifest");
        assert_eq!(m.template_id, "planning");
        assert!(m.required_placeholders.contains(&"role_instruction"));
        assert!(m.required_placeholders.contains(&"json_schema"));
        assert!(m.optional_placeholders.contains(&"task_prompt_contract"));
    }

    #[test]
    fn manifest_for_known_panel() {
        let m = manifest_for("completion_panel_completer").expect("completer manifest");
        assert!(m.required_placeholders.contains(&"prompt_text"));
        assert!(m.required_placeholders.contains(&"json_schema"));
        assert!(m.optional_placeholders.contains(&"task_prompt_contract"));
    }

    #[test]
    fn manifest_for_known_requirements() {
        let m = manifest_for("requirements_draft").expect("draft manifest");
        assert!(m.required_placeholders.contains(&"idea"));
    }

    #[test]
    fn manifest_for_unknown_returns_none() {
        assert!(manifest_for("nonexistent_template").is_none());
    }

    #[test]
    fn resolve_built_in_default() {
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve("planning", tmp.path(), None).unwrap();
        assert_eq!(resolved.source, TemplateSource::BuiltIn);
        assert!(resolved.content.contains("{{role_instruction}}"));
    }

    #[test]
    fn render_replaces_placeholders() {
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve("requirements_ideation", tmp.path(), None).unwrap();
        let rendered = render(&resolved, &[("base_context", "Build a widget")]).unwrap();
        assert!(rendered.contains("Build a widget"));
        assert!(!rendered.contains("{{base_context}}"));
    }

    #[test]
    fn render_collapses_empty_optionals() {
        let tmp = tempfile::tempdir().unwrap();
        let resolved = resolve("planning", tmp.path(), None).unwrap();
        let rendered = render(
            &resolved,
            &[
                ("role_instruction", "You are the Planner."),
                ("project_prompt", "Build X."),
                ("json_schema", "{}"),
                // prior_outputs and remediation are optional, not provided
            ],
        )
        .unwrap();
        assert!(!rendered.contains("{{prior_outputs}}"));
        assert!(!rendered.contains("{{remediation}}"));
        // Should not have 3+ consecutive newlines
        assert!(!rendered.contains("\n\n\n"));
    }

    #[test]
    fn workspace_override_used_when_present() {
        let tmp = tempfile::tempdir().unwrap();
        let ws_templates = tmp.path().join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws_templates).unwrap();
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "Custom ideation: {{base_context}}",
        )
        .unwrap();

        let resolved = resolve("requirements_ideation", tmp.path(), None).unwrap();
        assert!(matches!(
            resolved.source,
            TemplateSource::WorkspaceOverride(_)
        ));
        let rendered = render(&resolved, &[("base_context", "test idea")]).unwrap();
        assert_eq!(rendered, "Custom ideation: test idea");
    }

    #[test]
    fn project_override_beats_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let pid = ProjectId::new("myproj".to_owned()).unwrap();

        // Create workspace override
        let ws_templates = tmp.path().join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws_templates).unwrap();
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "Workspace: {{base_context}}",
        )
        .unwrap();

        // Create project override
        let proj_templates = tmp
            .path()
            .join(".ralph-burning")
            .join("projects")
            .join("myproj")
            .join("templates");
        std::fs::create_dir_all(&proj_templates).unwrap();
        std::fs::write(
            proj_templates.join("requirements_ideation.md"),
            "Project: {{base_context}}",
        )
        .unwrap();

        let resolved = resolve("requirements_ideation", tmp.path(), Some(&pid)).unwrap();
        assert!(matches!(
            resolved.source,
            TemplateSource::ProjectOverride(_)
        ));
        let rendered = render(&resolved, &[("base_context", "test")]).unwrap();
        assert_eq!(rendered, "Project: test");
    }

    #[test]
    fn malformed_unknown_placeholder_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let ws_templates = tmp.path().join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws_templates).unwrap();
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "{{base_context}} and {{unknown_field}}",
        )
        .unwrap();

        let result = resolve("requirements_ideation", tmp.path(), None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown placeholder"));
        assert!(err.contains("unknown_field"));
    }

    #[test]
    fn malformed_missing_required_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let ws_templates = tmp.path().join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws_templates).unwrap();
        // Missing the required "base_context" placeholder
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "No placeholders here at all.",
        )
        .unwrap();

        let result = resolve("requirements_ideation", tmp.path(), None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("missing required placeholder"));
    }

    #[test]
    fn non_utf8_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let ws_templates = tmp.path().join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws_templates).unwrap();
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            [0xFF, 0xFE, 0x00, 0x01],
        )
        .unwrap();

        let result = resolve("requirements_ideation", tmp.path(), None);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("UTF-8"));
    }

    #[test]
    fn project_malformed_does_not_fallback_to_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let pid = ProjectId::new("myproj".to_owned()).unwrap();

        // Valid workspace override
        let ws_templates = tmp.path().join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&ws_templates).unwrap();
        std::fs::write(
            ws_templates.join("requirements_ideation.md"),
            "Workspace: {{base_context}}",
        )
        .unwrap();

        // Malformed project override (missing required placeholder)
        let proj_templates = tmp
            .path()
            .join(".ralph-burning")
            .join("projects")
            .join("myproj")
            .join("templates");
        std::fs::create_dir_all(&proj_templates).unwrap();
        std::fs::write(
            proj_templates.join("requirements_ideation.md"),
            "Project template without placeholders.",
        )
        .unwrap();

        // Must fail — must NOT silently fall back to workspace
        let result = resolve("requirements_ideation", tmp.path(), Some(&pid));
        assert!(result.is_err());
    }
}

#![forbid(unsafe_code)]

//! Shared contract metadata for milestone-aware bead execution prompts.
//!
//! The contract is intentionally explicit and versioned so prompt generation
//! and downstream workflow stages can reason about the same stable structure.

use super::fence_util::{closes_fence, opening_fence_delimiter};
use crate::shared::domain::StageId;

/// Approximate prompt budget for nearby bead context. Builders enforce this
/// before rendering so the renderer can print the context without re-truncating.
pub const NEARBY_BEAD_CONTEXT_BYTE_CAP: usize = 2_000;

/// Stable contract identifier for bead-backed execution prompts.
pub const BEAD_TASK_PROMPT_CONTRACT_NAME: &str = "bead_execution_prompt";

/// Current contract version for bead-backed execution prompts.
pub const BEAD_TASK_PROMPT_CONTRACT_VERSION: u32 = 1;

const CONTRACT_MARKER_PREFIX: &str = "<!-- ralph-task-prompt-contract:";

/// Canonical section title for milestone context.
pub const SECTION_MILESTONE_SUMMARY: &str = "Milestone Summary";
/// Canonical section title for current bead metadata.
pub const SECTION_CURRENT_BEAD_DETAILS: &str = "Current Bead Details";
/// Canonical section title for nearby bead graph context.
pub const SECTION_NEARBY_WORK: &str = "Nearby work";
/// Canonical section title for required in-scope work.
pub const SECTION_MUST_DO_SCOPE: &str = "Must-Do Scope";
/// Canonical section title for explicit out-of-scope work.
pub const SECTION_EXPLICIT_NON_GOALS: &str = "Explicit Non-Goals";
/// Canonical section title for the bead acceptance boundary.
pub const SECTION_ACCEPTANCE_CRITERIA: &str = "Acceptance Criteria";
/// Canonical section title for related work that should not be absorbed here.
pub const SECTION_ALREADY_PLANNED_ELSEWHERE: &str = "Already Planned Elsewhere";
/// Canonical section title for stage reviewers and implementers.
pub const SECTION_REVIEW_POLICY: &str = "Review Policy";
/// Canonical section title for repository and AGENTS guidance.
pub const SECTION_AGENTS_REPO_GUIDANCE: &str = "AGENTS / Repo Guidance";

/// Ordered section titles for the canonical prompt contract.
pub const BEAD_TASK_PROMPT_SECTION_TITLES: &[&str] = &[
    SECTION_MILESTONE_SUMMARY,
    SECTION_CURRENT_BEAD_DETAILS,
    SECTION_NEARBY_WORK,
    SECTION_MUST_DO_SCOPE,
    SECTION_EXPLICIT_NON_GOALS,
    SECTION_ACCEPTANCE_CRITERIA,
    SECTION_ALREADY_PLANNED_ELSEWHERE,
    SECTION_REVIEW_POLICY,
    SECTION_AGENTS_REPO_GUIDANCE,
];

fn is_optional_canonical_section(index: usize, allow_legacy_missing_nearby_work: bool) -> bool {
    if !allow_legacy_missing_nearby_work {
        return false;
    }

    BEAD_TASK_PROMPT_SECTION_TITLES
        .get(index)
        .is_some_and(|section| *section == SECTION_NEARBY_WORK)
}

/// Nearby bead graph context pre-truncated by the builder to
/// [`NEARBY_BEAD_CONTEXT_BYTE_CAP`] before prompt rendering.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NearbyBeadContext {
    pub direct_dependencies: Vec<NearbyBeadEntry>,
    pub direct_dependents: Vec<NearbyBeadEntry>,
    pub siblings: Vec<NearbyBeadEntry>,
    pub related_work: Vec<NearbyBeadEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NearbyBeadEntry {
    pub bead_id: String,
    pub title: String,
    pub scope_summary: String,
    pub status: String,
}

impl NearbyBeadContext {
    pub fn is_empty(&self) -> bool {
        self.direct_dependencies.is_empty()
            && self.direct_dependents.is_empty()
            && self.siblings.is_empty()
            && self.related_work.is_empty()
    }
}

pub fn enforce_nearby_context_budget(
    mut ctx: NearbyBeadContext,
    byte_cap: usize,
) -> NearbyBeadContext {
    while nearby_context_rendered_bytes(&ctx) > byte_cap {
        if ctx.related_work.pop().is_some() {
            continue;
        }
        if ctx.siblings.pop().is_some() {
            continue;
        }
        if ctx.direct_dependents.pop().is_some() {
            continue;
        }
        if ctx.direct_dependencies.pop().is_some() {
            continue;
        }
        break;
    }
    ctx
}

pub fn render_nearby_bead_context(ctx: &NearbyBeadContext) -> String {
    if ctx.is_empty() {
        return "No nearby open work was found.".to_owned();
    }

    let mut sections = Vec::new();
    push_nearby_subsection(
        &mut sections,
        "Direct dependencies",
        &ctx.direct_dependencies,
    );
    push_nearby_subsection(&mut sections, "Direct dependents", &ctx.direct_dependents);
    push_nearby_subsection(&mut sections, "Siblings", &ctx.siblings);
    push_nearby_subsection(&mut sections, "Related work", &ctx.related_work);
    sections.join("\n\n")
}

fn nearby_context_rendered_bytes(ctx: &NearbyBeadContext) -> usize {
    render_nearby_bead_context(ctx).len()
}

fn push_nearby_subsection(sections: &mut Vec<String>, title: &str, entries: &[NearbyBeadEntry]) {
    if entries.is_empty() {
        return;
    }

    let mut lines = vec![format!("### {title}")];
    for entry in entries {
        lines.push(format!(
            "- `{}` [{}] {}",
            entry.bead_id, entry.status, entry.title
        ));
        if !entry.scope_summary.is_empty() {
            lines.push(format!("  Scope: {}", entry.scope_summary));
        }
    }
    sections.push(lines.join("\n"));
}

/// Machine-readable marker embedded in canonical bead task prompts.
pub fn contract_marker() -> String {
    format!(
        "{CONTRACT_MARKER_PREFIX} {BEAD_TASK_PROMPT_CONTRACT_NAME}/{} -->",
        BEAD_TASK_PROMPT_CONTRACT_VERSION
    )
}

fn contract_identifier() -> String {
    format!(
        "{BEAD_TASK_PROMPT_CONTRACT_NAME}/{}",
        BEAD_TASK_PROMPT_CONTRACT_VERSION
    )
}

fn markdown_canonical_section_heading(line: &str) -> Option<(usize, usize)> {
    let trimmed_end = line.trim_end();
    let leading_spaces = trimmed_end.chars().take_while(|ch| *ch == ' ').count();
    if leading_spaces > 3 {
        return None;
    }

    let title = trimmed_end[leading_spaces..].strip_prefix("## ")?;
    let section_index = BEAD_TASK_PROMPT_SECTION_TITLES
        .iter()
        .position(|section| *section == title)?;
    Some((section_index, leading_spaces))
}

fn consumer_guidance_body(scope_boundary_guidance: &str, section_titles: &[&str]) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "The project prompt below uses `{}`.\n\n",
        contract_identifier()
    ));
    out.push_str("Treat these sections as authoritative:\n");
    for section in section_titles {
        out.push_str(&format!("- `{section}`\n"));
    }
    out.push('\n');
    out.push_str(scope_boundary_guidance);
    out
}

fn prompt_authoritative_section_titles(prompt: &str) -> Vec<&'static str> {
    if validate_current_canonical_prompt_shape(prompt).is_ok() {
        return BEAD_TASK_PROMPT_SECTION_TITLES.to_vec();
    }

    if validate_canonical_prompt_shape(prompt).is_ok() {
        return BEAD_TASK_PROMPT_SECTION_TITLES
            .iter()
            .copied()
            .filter(|section| *section != SECTION_NEARBY_WORK)
            .collect();
    }

    BEAD_TASK_PROMPT_SECTION_TITLES.to_vec()
}

fn top_level_lines(prompt: &str) -> impl Iterator<Item = &str> {
    let mut active_fence = None;
    prompt.lines().filter(move |line| {
        if let Some(opening) = active_fence {
            if closes_fence(line, opening) {
                active_fence = None;
            }
            return false;
        }

        if let Some(opening) = opening_fence_delimiter(line) {
            active_fence = Some(opening);
            return false;
        }

        true
    })
}

fn has_top_level_contract_marker(prompt: &str) -> bool {
    let marker = contract_marker();
    top_level_lines(prompt).any(|line| line.trim_start() == line && line.trim_end() == marker)
}

/// Return whether the prompt declares the canonical contract marker as a
/// top-level line outside fenced blocks.
pub fn prompt_declares_contract(prompt: &str) -> bool {
    has_top_level_contract_marker(prompt)
}

/// Return whether the prompt declares the canonical bead task prompt contract.
pub fn prompt_uses_contract(prompt: &str) -> bool {
    has_top_level_contract_marker(prompt)
}

/// Guidance injected into workflow stage prompts when the project prompt uses
/// the canonical bead task prompt contract.
pub fn stage_consumer_guidance() -> String {
    stage_consumer_guidance_with_sections(BEAD_TASK_PROMPT_SECTION_TITLES)
}

fn stage_consumer_guidance_with_sections(section_titles: &[&str]) -> String {
    let mut out = String::from("## Task Prompt Contract\n\n");
    out.push_str(&consumer_guidance_body(
        "Use `Must-Do Scope` plus `Acceptance Criteria` as the in-scope boundary. Treat `Explicit Non-Goals` and `Already Planned Elsewhere` as out-of-scope unless the work is strictly required to satisfy the active bead.",
        section_titles,
    ));
    out
}

/// Guidance injected into planning-oriented workflow stage prompts when the
/// project prompt uses the canonical bead task prompt contract.
pub fn planning_stage_consumer_guidance() -> String {
    planning_stage_consumer_guidance_with_sections(BEAD_TASK_PROMPT_SECTION_TITLES)
}

fn planning_stage_consumer_guidance_with_sections(section_titles: &[&str]) -> String {
    let mut out = String::from("## Task Prompt Contract\n\n");
    out.push_str(&consumer_guidance_body(
        "Use `Must-Do Scope` plus `Acceptance Criteria` as the in-scope boundary. Treat `Explicit Non-Goals` as out-of-scope. Do not absorb work owned by `Already Planned Elsewhere`; leave it deferred or referenced as related follow-up instead of pulling it into the active bead.",
        section_titles,
    ));
    out
}

/// Guidance injected into prompt-review templates when the prompt uses the
/// canonical bead task prompt contract.
pub fn prompt_review_consumer_guidance() -> String {
    prompt_review_consumer_guidance_with_sections(BEAD_TASK_PROMPT_SECTION_TITLES, false)
}

fn prompt_review_consumer_guidance_with_sections(
    section_titles: &[&str],
    legacy_missing_nearby_work: bool,
) -> String {
    let mut out = stage_consumer_guidance_with_sections(section_titles);
    out.push_str(&format!(
        "\n\nIf you rewrite the prompt, preserve the exact contract marker line `{}` and keep the canonical section headings in the same order.",
        contract_marker()
    ));
    if legacy_missing_nearby_work {
        out.push_str(
            " This prompt uses the legacy v1 shape without `Nearby work`; do not add that section unless explicitly modernizing the prompt.",
        );
    }
    out.push_str(
        "\n\nPreserve milestone-provided `AGENTS / Repo Guidance` verbatim instead of rewriting it into synthesized bullets.",
    );
    out
}

/// Return stage-consumer guidance when the prompt uses the canonical contract.
pub fn stage_consumer_guidance_for_prompt(prompt: &str) -> String {
    if prompt_uses_contract(prompt) {
        let section_titles = prompt_authoritative_section_titles(prompt);
        stage_consumer_guidance_with_sections(&section_titles)
    } else {
        String::new()
    }
}

/// Return stage-consumer guidance tailored to the consuming stage when the
/// prompt uses the canonical contract.
pub fn stage_consumer_guidance_for_stage_prompt(stage_id: StageId, prompt: &str) -> String {
    if !prompt_uses_contract(prompt) {
        return String::new();
    }

    match stage_id {
        StageId::Planning | StageId::PlanAndImplement => {
            let section_titles = prompt_authoritative_section_titles(prompt);
            planning_stage_consumer_guidance_with_sections(&section_titles)
        }
        _ => {
            let section_titles = prompt_authoritative_section_titles(prompt);
            stage_consumer_guidance_with_sections(&section_titles)
        }
    }
}

/// Return prompt-review guidance when the prompt uses the canonical contract.
pub fn prompt_review_consumer_guidance_for_prompt(prompt: &str) -> String {
    if prompt_uses_contract(prompt) {
        let section_titles = prompt_authoritative_section_titles(prompt);
        let legacy_missing_nearby_work = !section_titles.contains(&SECTION_NEARBY_WORK);
        prompt_review_consumer_guidance_with_sections(&section_titles, legacy_missing_nearby_work)
    } else {
        String::new()
    }
}

/// Validate that a prompt preserves the canonical marker and section order.
pub fn validate_canonical_prompt_shape(prompt: &str) -> Result<(), Vec<String>> {
    validate_canonical_prompt_shape_with_options(prompt, true)
}

/// Validate the current canonical prompt shape for newly generated or refined
/// prompts. Unlike [`validate_canonical_prompt_shape`], this compatibility-free
/// check requires `Nearby work` so prompt review cannot silently drop it.
pub fn validate_current_canonical_prompt_shape(prompt: &str) -> Result<(), Vec<String>> {
    validate_canonical_prompt_shape_with_options(prompt, false)
}

fn validate_canonical_prompt_shape_with_options(
    prompt: &str,
    allow_legacy_missing_nearby_work: bool,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    let marker = contract_marker();
    let mut marker_line_index = None;
    let mut first_canonical_heading_line_index = None;
    let mut seen_positions = vec![None; BEAD_TASK_PROMPT_SECTION_TITLES.len()];
    let mut reported_missing = vec![false; BEAD_TASK_PROMPT_SECTION_TITLES.len()];
    let mut expected_index = 0usize;
    let mut active_fence = None;

    for (line_index, line) in prompt.lines().enumerate() {
        if let Some(opening) = active_fence {
            if closes_fence(line, opening) {
                active_fence = None;
            }
            continue;
        }

        if let Some(opening) = opening_fence_delimiter(line) {
            active_fence = Some(opening);
            continue;
        }

        if line.trim_start() == line && line.trim_end() == marker {
            marker_line_index.get_or_insert(line_index);
            continue;
        }

        let Some((found_index, leading_spaces)) = markdown_canonical_section_heading(line) else {
            continue;
        };

        first_canonical_heading_line_index.get_or_insert(line_index);

        if leading_spaces > 0 {
            let heading = format!("## {}", BEAD_TASK_PROMPT_SECTION_TITLES[found_index]);
            errors.push(format!(
                "canonical heading `{heading}` must start at column 1; found {leading_spaces} leading space(s)"
            ));
            continue;
        }

        if expected_index < BEAD_TASK_PROMPT_SECTION_TITLES.len() && found_index == expected_index {
            seen_positions[found_index] = Some(line_index);
            expected_index += 1;
            continue;
        }

        if found_index > expected_index && expected_index < BEAD_TASK_PROMPT_SECTION_TITLES.len() {
            for missing_index in expected_index..found_index {
                if is_optional_canonical_section(missing_index, allow_legacy_missing_nearby_work) {
                    reported_missing[missing_index] = true;
                    continue;
                }
                errors.push(format!(
                    "missing section heading `## {}`",
                    BEAD_TASK_PROMPT_SECTION_TITLES[missing_index]
                ));
                reported_missing[missing_index] = true;
            }
            seen_positions[found_index] = Some(line_index);
            expected_index = found_index + 1;
            continue;
        }

        let heading = format!("## {}", BEAD_TASK_PROMPT_SECTION_TITLES[found_index]);
        if expected_index >= BEAD_TASK_PROMPT_SECTION_TITLES.len() {
            errors.push(format!(
                "unexpected extra canonical heading `{heading}` after `## {}`",
                SECTION_AGENTS_REPO_GUIDANCE
            ));
            continue;
        }

        if let Some(expected_section) = BEAD_TASK_PROMPT_SECTION_TITLES.get(expected_index) {
            let expected_heading = format!("## {expected_section}");
            errors.push(format!(
                "unexpected canonical heading `{heading}` before `{expected_heading}`"
            ));
        }
    }

    if marker_line_index.is_none() {
        errors.push(format!("missing exact contract marker `{}`", marker));
    } else if matches!(
        (marker_line_index, first_canonical_heading_line_index),
        (Some(marker_line_index), Some(first_heading_line_index)) if marker_line_index > first_heading_line_index
    ) {
        errors.push(format!(
            "contract marker `{}` must appear before the canonical section block",
            marker
        ));
    }

    for (index, section) in BEAD_TASK_PROMPT_SECTION_TITLES.iter().enumerate() {
        if seen_positions[index].is_none()
            && !reported_missing[index]
            && !is_optional_canonical_section(index, allow_legacy_missing_nearby_work)
        {
            errors.push(format!("missing section heading `## {section}`"));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn extract_pe_bead_ids_internal(
    prompt: &str,
    include_short_form_aliases: bool,
) -> std::collections::HashSet<String> {
    extract_bullet_bead_ids_from_section(
        prompt,
        SECTION_ALREADY_PLANNED_ELSEWHERE,
        include_short_form_aliases,
    )
}

fn extract_bullet_bead_ids_from_section(
    prompt: &str,
    section_title: &str,
    include_short_form_aliases: bool,
) -> std::collections::HashSet<String> {
    let mut result = std::collections::HashSet::new();
    let section_header = format!("## {section_title}");
    let mut in_section = false;

    for line in prompt.lines() {
        if in_section {
            // Stop at the next `## ` heading.
            if line.starts_with("## ") {
                break;
            }
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("- ") {
                // The bead ID is the first token before space or '('.
                let id = rest
                    .split(|c: char| c.is_whitespace() || c == '(')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .trim_matches('`');
                if !id.is_empty() {
                    result.insert(id.to_owned());
                    // Review-time matching accepts a short-form alias by
                    // stripping the milestone prefix (everything up to and
                    // including the first dot).
                    if include_short_form_aliases {
                        if let Some(short) = strip_milestone_prefix(id) {
                            result.insert(short.to_owned());
                        }
                    }
                }
            }
        } else if line.trim() == section_header {
            in_section = true;
        }
    }

    result
}

fn extract_nearby_bead_ids_internal(
    prompt: &str,
    include_short_form_aliases: bool,
) -> std::collections::HashSet<String> {
    extract_bullet_bead_ids_from_section(prompt, SECTION_NEARBY_WORK, include_short_form_aliases)
}

/// Extract the canonical bead IDs from the "Already Planned Elsewhere"
/// section of a canonical bead execution prompt.
///
/// Each bullet line in the section starts with `- {bead_id} (...)`.
/// The bead ID is the first whitespace-delimited token after `- `.
pub fn extract_pe_canonical_bead_ids(prompt: &str) -> std::collections::HashSet<String> {
    extract_pe_bead_ids_internal(prompt, false)
}

/// Extract bead IDs from the "Already Planned Elsewhere" section of a
/// canonical bead execution prompt. Returns the set of bead IDs that reviewers
/// may reference as `mapped_to_bead_id`.
///
/// In addition to canonical (qualified) bead IDs like `milestone.short_id`,
/// this helper also includes the short-form alias `short_id` so review-stage
/// matching can accept either form. This mirrors
/// `planned_bead_membership_refs` in bundle.rs.
pub fn extract_pe_bead_ids(prompt: &str) -> std::collections::HashSet<String> {
    extract_pe_bead_ids_internal(prompt, true)
}

/// Extract bead IDs that review stages may route `planned-elsewhere`
/// findings to. This includes explicit "Already Planned Elsewhere" items and
/// focused "Nearby work" entries because both sections identify work owned by
/// beads adjacent to the active task.
pub fn extract_planned_elsewhere_routing_bead_ids(
    prompt: &str,
) -> std::collections::HashSet<String> {
    let mut ids = extract_pe_bead_ids_internal(prompt, true);
    ids.extend(extract_nearby_bead_ids_internal(prompt, true));
    ids
}

/// Extract canonical bead IDs from the prompt sections that identify work
/// outside the active bead. Unlike [`extract_planned_elsewhere_routing_bead_ids`],
/// this omits short-form aliases because it is intended for human guidance.
pub fn extract_planned_elsewhere_canonical_routing_bead_ids(
    prompt: &str,
) -> std::collections::HashSet<String> {
    let mut ids = extract_pe_bead_ids_internal(prompt, false);
    ids.extend(extract_nearby_bead_ids_internal(prompt, false));
    ids
}

/// Strip the milestone-id prefix from a canonical bead ID.
///
/// Canonical bead IDs have the form `{milestone_id}.{short_id}` where
/// `milestone_id` does not contain dots.  Returns the `short_id` portion
/// if a dot separator is found, or `None` if the ID has no prefix.
pub fn strip_milestone_prefix(canonical_id: &str) -> Option<&str> {
    let idx = canonical_id.find('.')?;
    let short = &canonical_id[idx + 1..];
    if short.is_empty() {
        None
    } else {
        Some(short)
    }
}

/// Extract the milestone prefix from a canonical bead ID.
///
/// Canonical bead IDs have the form `{milestone_prefix}.{short_id}` where the
/// milestone prefix is a short alphanumeric string (e.g. `9ni`).  Returns the
/// prefix portion before the first dot, or `None` if the ID contains no dot
/// or the prefix is empty.
pub fn milestone_prefix_of(canonical_id: &str) -> Option<&str> {
    let idx = canonical_id.find('.')?;
    let prefix = &canonical_id[..idx];
    if prefix.is_empty() {
        None
    } else {
        Some(prefix)
    }
}

/// Default review policy for canonical bead execution prompts.
pub fn default_review_policy() -> Vec<String> {
    vec![
        "Treat `Must-Do Scope` and `Acceptance Criteria` as the fix-now boundary.".to_owned(),
        "If related work is already covered by another bead or milestone item, note it as already planned elsewhere instead of expanding this bead.".to_owned(),
        "If new required work falls outside this bead and is not already planned elsewhere, call it out as a follow-up rather than silently absorbing it.".to_owned(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_marker_is_stable() {
        assert_eq!(
            contract_marker(),
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->"
        );
    }

    #[test]
    fn prompt_detection_requires_exact_marker() {
        assert!(prompt_uses_contract(
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA"
        ));
        assert!(!prompt_uses_contract("# Ralph Task Prompt\nNo marker"));
    }

    #[test]
    fn prompt_detection_allows_title_and_metadata_before_first_canonical_section() {
        let prompt = format!(
            "# Ralph Task Prompt\n\n{}\n\n- Contract: `bead_execution_prompt`\n\n## Milestone Summary\n\nA",
            contract_marker()
        );
        assert!(prompt_uses_contract(&prompt));
    }

    #[test]
    fn prompt_detection_ignores_marker_inside_fenced_block() {
        let prompt = format!("# Generic Prompt\n\n```md\n{}\n```", contract_marker());
        assert!(!prompt_uses_contract(&prompt));
    }

    #[test]
    fn prompt_detection_treats_any_top_level_marker_as_contract_opt_in() {
        let prompt = format!(
            "# Generic Prompt\n\n## AGENTS / Repo Guidance\n\n{}",
            contract_marker()
        );
        assert!(prompt_uses_contract(&prompt));
    }

    #[test]
    fn stage_guidance_is_still_injected_for_marker_only_drifted_prompts() {
        let prompt = format!(
            "# Drifted Prompt\n\n{}\n\n## Acceptance Criteria\n\nLater sections only.",
            contract_marker()
        );
        let guidance = stage_consumer_guidance_for_prompt(&prompt);

        assert!(guidance.contains("## Task Prompt Contract"));
        assert!(guidance.contains("`Milestone Summary`"));
    }

    #[test]
    fn stage_guidance_references_all_canonical_sections() {
        let guidance = stage_consumer_guidance();
        for section in BEAD_TASK_PROMPT_SECTION_TITLES {
            assert!(guidance.contains(section));
        }
        assert!(guidance.contains("`bead_execution_prompt/1`."));
    }

    #[test]
    fn planning_stage_guidance_disallows_absorbing_planned_elsewhere_work() {
        let guidance = planning_stage_consumer_guidance();

        assert!(guidance.contains("Do not absorb work owned by `Already Planned Elsewhere`"));
        assert!(!guidance.contains("strictly required to satisfy the active bead"));
    }

    #[test]
    fn prompt_review_guidance_requires_exact_marker_preservation() {
        let guidance = prompt_review_consumer_guidance();
        assert!(guidance.contains(&contract_marker()));
        assert!(guidance.contains("keep the canonical section headings in the same order"));
    }

    #[test]
    fn stage_guidance_for_legacy_prompt_omits_nearby_work_section() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let guidance = stage_consumer_guidance_for_prompt(&prompt);

        assert!(guidance.contains("`Milestone Summary`"));
        assert!(!guidance.contains("`Nearby work`"));
    }

    #[test]
    fn prompt_review_guidance_for_legacy_prompt_does_not_require_nearby_work() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let guidance = prompt_review_consumer_guidance_for_prompt(&prompt);

        assert!(guidance.contains("legacy v1 shape without `Nearby work`"));
        assert!(!guidance.contains("- `Nearby work`"));
    }

    #[test]
    fn canonical_prompt_shape_requires_marker_and_all_sections() {
        let valid_prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );
        assert!(validate_canonical_prompt_shape(&valid_prompt).is_ok());

        let invalid_prompt = "# Ralph Task Prompt\n\n## Milestone Summary\n\nA".to_owned();
        let errors =
            validate_canonical_prompt_shape(&invalid_prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| error.contains("missing exact contract marker")));
        assert!(errors
            .iter()
            .any(|error| error.contains("## Current Bead Details")));
    }

    #[test]
    fn canonical_prompt_shape_allows_legacy_v1_prompt_without_nearby_work() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }

    #[test]
    fn current_canonical_prompt_shape_requires_nearby_work() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors =
            validate_current_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| error.contains("missing section heading `## Nearby work`")));
    }

    #[test]
    fn canonical_prompt_shape_rejects_nearby_work_after_later_sections() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Nearby work\n\nN\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| error.contains("unexpected canonical heading `## Nearby work`")));
    }

    #[test]
    fn canonical_prompt_shape_rejects_marker_after_canonical_section_block() {
        let prompt = format!(
            "# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH\n\n{}",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| { error.contains("must appear before the canonical section block") }));
    }

    #[test]
    fn canonical_prompt_shape_rejects_duplicate_canonical_headings() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nGoal:\nShip the thing.\n\n## Acceptance Criteria\n\nEmbedded bead marker that should stay inside the body.\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| error.contains("unexpected canonical heading `## Acceptance Criteria`")));
    }

    #[test]
    fn canonical_prompt_shape_rejects_out_of_order_headings() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Acceptance Criteria\n\nE\n\n## Explicit Non-Goals\n\nD\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| error.contains("missing section heading `## Explicit Non-Goals`")));
        assert!(errors.iter().any(|error| {
            error.contains("unexpected canonical heading `## Explicit Non-Goals`")
        }));
    }

    #[test]
    fn canonical_prompt_shape_reports_missing_section_without_cascading_later_errors() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert_eq!(
            errors,
            vec!["missing section heading `## Explicit Non-Goals`"]
        );
    }

    #[test]
    fn canonical_prompt_shape_reports_missing_early_sections_without_marker_noise() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors
            .iter()
            .any(|error| error.contains("missing section heading `## Milestone Summary`")));
        assert!(errors
            .iter()
            .any(|error| error.contains("missing section heading `## Current Bead Details`")));
        assert!(!errors
            .iter()
            .any(|error| error.contains("must appear before the canonical section block")));
    }

    #[test]
    fn canonical_prompt_shape_rejects_extra_canonical_headings_after_agents_guidance() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nFollow repo guidance verbatim.\n\n## Review Policy\n\nThis heading belongs to the embedded AGENTS snippet, not the canonical contract.",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors.iter().any(|error| {
            error.contains("unexpected extra canonical heading `## Review Policy`")
        }));
    }

    #[test]
    fn canonical_prompt_shape_allows_escaped_canonical_heading_lines_after_agents_guidance() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nFollow repo guidance verbatim.\n\n    ## Review Policy\n\nThis heading belongs to the embedded AGENTS snippet, not the canonical contract.",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }

    #[test]
    fn canonical_prompt_shape_ignores_canonical_headings_inside_fenced_examples() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\n```md\n## Acceptance Criteria\n```\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }

    #[test]
    fn canonical_prompt_shape_keeps_longer_opening_fence_active_until_matching_close() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\n- Summary: first line\n  ````md\n  ## Acceptance Criteria\n  ```\n  ````\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }

    #[test]
    fn canonical_prompt_shape_rejects_markdown_valid_indented_canonical_heading_lines() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\n- Summary: first line\n ## Acceptance Criteria\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        let errors = validate_canonical_prompt_shape(&prompt).expect_err("shape should fail");
        assert!(errors.iter().any(|error| {
            error.contains("canonical heading `## Acceptance Criteria` must start at column 1")
        }));
    }

    #[test]
    fn canonical_prompt_shape_allows_four_space_indented_heading_like_lines_in_section_bodies() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\n- Summary: first line\n    ## Acceptance Criteria\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }

    #[test]
    fn canonical_prompt_shape_allows_trailing_whitespace_on_canonical_headings() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary \n\nA\n\n## Current Bead Details\t\n\nB\n\n## Nearby work \n\nN\n\n## Must-Do Scope \n\nC\n\n## Explicit Non-Goals \n\nD\n\n## Acceptance Criteria \n\nE\n\n## Already Planned Elsewhere \n\nF\n\n## Review Policy \n\nG\n\n## AGENTS / Repo Guidance \n\nH",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }

    #[test]
    fn extract_pe_bead_ids_normal_bullets() {
        let prompt = "## Already Planned Elsewhere\n\n- bead-A (some title) - relationship\n- bead-B (another) - relationship\n";
        let ids = extract_pe_bead_ids(prompt);
        assert_eq!(ids.len(), 2);
        assert!(ids.contains("bead-A"));
        assert!(ids.contains("bead-B"));
    }

    #[test]
    fn extract_pe_bead_ids_no_pe_section_returns_empty() {
        let prompt = "## Milestone Summary\n\nSome text\n\n## Acceptance Criteria\n\nMore text\n";
        let ids = extract_pe_bead_ids(prompt);
        assert!(ids.is_empty());
    }

    #[test]
    fn extract_pe_bead_ids_pe_section_no_bullets_returns_empty() {
        let prompt =
            "## Already Planned Elsewhere\n\nNo explicit planned-elsewhere items were supplied.\n";
        let ids = extract_pe_bead_ids(prompt);
        assert!(ids.is_empty());
    }

    #[test]
    fn extract_pe_bead_ids_multiple_beads_captured() {
        let prompt = "## Already Planned Elsewhere\n\n- alpha (Title A) - covers X\n- beta (Title B) - covers Y\n- gamma (Title C) - covers Z\n";
        let ids = extract_pe_bead_ids(prompt);
        assert_eq!(ids.len(), 3);
        assert!(ids.contains("alpha"));
        assert!(ids.contains("beta"));
        assert!(ids.contains("gamma"));
    }

    #[test]
    fn extract_pe_bead_ids_stops_at_next_heading() {
        let prompt = "## Already Planned Elsewhere\n\n- bead-X (title) - rel\n\n## Review Policy\n\n- bead-Y (should not be captured)\n";
        let ids = extract_pe_bead_ids(prompt);
        assert_eq!(ids.len(), 1);
        assert!(ids.contains("bead-X"));
        assert!(!ids.contains("bead-Y"));
    }

    #[test]
    fn extract_pe_bead_ids_includes_short_form_aliases() {
        let prompt = "## Already Planned Elsewhere\n\n- 9ni.8.5.3 (Handle missing work) - downstream\n- plain-id (No prefix) - adjacent\n";
        let ids = extract_pe_bead_ids(prompt);
        // 9ni.8.5.3 produces both canonical and short form; plain-id has no dot prefix
        assert!(ids.contains("9ni.8.5.3"));
        assert!(ids.contains("8.5.3"));
        assert!(ids.contains("plain-id"));
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn extract_planned_elsewhere_routing_bead_ids_includes_nearby_work() {
        let prompt = "## Nearby work\n\n### Direct dependents\n- `9ni.6.5` [open] Consume nearby IDs\n  Scope: Route findings to the dependent.\n\n### Related work\n- `plain-related` [in_progress] Related item\n\n## Must-Do Scope\n\nCurrent bead only.\n\n## Already Planned Elsewhere\n\n- 9ni.7.1 (Later work) - planned\n";

        let ids = extract_planned_elsewhere_routing_bead_ids(prompt);

        assert!(ids.contains("9ni.6.5"));
        assert!(ids.contains("6.5"));
        assert!(ids.contains("plain-related"));
        assert!(ids.contains("9ni.7.1"));
        assert!(ids.contains("7.1"));
        assert_eq!(ids.len(), 5);
    }

    #[test]
    fn extract_planned_elsewhere_canonical_routing_bead_ids_omits_short_aliases() {
        let prompt = "## Nearby work\n\n### Direct dependents\n- `9ni.6.5` [open] Consume nearby IDs\n\n## Already Planned Elsewhere\n\n- 9ni.7.1 (Later work) - planned\n";

        let ids = extract_planned_elsewhere_canonical_routing_bead_ids(prompt);

        assert!(ids.contains("9ni.6.5"));
        assert!(ids.contains("9ni.7.1"));
        assert!(!ids.contains("6.5"));
        assert!(!ids.contains("7.1"));
        assert_eq!(ids.len(), 2);
    }

    #[test]
    fn extract_pe_canonical_bead_ids_preserves_only_canonical_ids() {
        let prompt = "## Already Planned Elsewhere\n\n- ms-alpha.alpha (Alpha) - downstream\n- ms-beta.alpha (Also Alpha) - adjacent\n- plain-id (No prefix) - adjacent\n";
        let ids = extract_pe_canonical_bead_ids(prompt);
        assert!(ids.contains("ms-alpha.alpha"));
        assert!(ids.contains("ms-beta.alpha"));
        assert!(ids.contains("plain-id"));
        assert!(!ids.contains("alpha"));
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn strip_milestone_prefix_extracts_short_id() {
        assert_eq!(strip_milestone_prefix("9ni.8.5.3"), Some("8.5.3"));
        assert_eq!(strip_milestone_prefix("ms-alpha.bead-4"), Some("bead-4"));
        assert_eq!(strip_milestone_prefix("no-dots"), None);
        assert_eq!(strip_milestone_prefix("trailing."), None);
    }

    #[test]
    fn milestone_prefix_of_extracts_prefix() {
        assert_eq!(milestone_prefix_of("9ni.8.5.3"), Some("9ni"));
        assert_eq!(milestone_prefix_of("ms-alpha.bead-4"), Some("ms-alpha"));
        assert_eq!(milestone_prefix_of("no-dots"), None);
        assert_eq!(milestone_prefix_of(".leading-dot"), None);
        assert_eq!(milestone_prefix_of("trailing."), Some("trailing"));
    }
}

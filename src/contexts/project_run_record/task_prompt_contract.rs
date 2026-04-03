#![forbid(unsafe_code)]

//! Shared contract metadata for milestone-aware bead execution prompts.
//!
//! The contract is intentionally explicit and versioned so prompt generation
//! and downstream workflow stages can reason about the same stable structure.

/// Stable contract identifier for bead-backed execution prompts.
pub const BEAD_TASK_PROMPT_CONTRACT_NAME: &str = "bead_execution_prompt";

/// Current contract version for bead-backed execution prompts.
pub const BEAD_TASK_PROMPT_CONTRACT_VERSION: u32 = 1;

const CONTRACT_MARKER_PREFIX: &str = "<!-- ralph-task-prompt-contract:";

/// Canonical section title for milestone context.
pub const SECTION_MILESTONE_SUMMARY: &str = "Milestone Summary";
/// Canonical section title for current bead metadata.
pub const SECTION_CURRENT_BEAD_DETAILS: &str = "Current Bead Details";
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
    SECTION_MUST_DO_SCOPE,
    SECTION_EXPLICIT_NON_GOALS,
    SECTION_ACCEPTANCE_CRITERIA,
    SECTION_ALREADY_PLANNED_ELSEWHERE,
    SECTION_REVIEW_POLICY,
    SECTION_AGENTS_REPO_GUIDANCE,
];

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

fn consumer_guidance_body() -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "The project prompt below uses `{}`.\n\n",
        contract_identifier()
    ));
    out.push_str("Treat these sections as authoritative:\n");
    for section in BEAD_TASK_PROMPT_SECTION_TITLES {
        out.push_str(&format!("- `{section}`\n"));
    }
    out.push_str(
        "\nUse `Must-Do Scope` plus `Acceptance Criteria` as the in-scope boundary. Treat `Explicit Non-Goals` and `Already Planned Elsewhere` as out-of-scope unless the work is strictly required to satisfy the active bead.",
    );
    out
}

/// Return whether the prompt declares the canonical bead task prompt contract.
pub fn prompt_uses_contract(prompt: &str) -> bool {
    prompt.lines().any(|line| line.trim() == contract_marker())
}

/// Guidance injected into workflow stage prompts when the project prompt uses
/// the canonical bead task prompt contract.
pub fn stage_consumer_guidance() -> String {
    let mut out = String::from("## Task Prompt Contract\n\n");
    out.push_str(&consumer_guidance_body());
    out
}

/// Guidance injected into prompt-review templates when the prompt uses the
/// canonical bead task prompt contract.
pub fn prompt_review_consumer_guidance() -> String {
    let mut out = stage_consumer_guidance();
    out.push_str(&format!(
        "\n\nIf you rewrite the prompt, preserve the exact contract marker line `{}` and keep the canonical section headings in the same order.",
        contract_marker()
    ));
    out.push_str(
        "\n\nPreserve milestone-provided `AGENTS / Repo Guidance` verbatim instead of rewriting it into synthesized bullets.",
    );
    out
}

/// Return stage-consumer guidance when the prompt uses the canonical contract.
pub fn stage_consumer_guidance_for_prompt(prompt: &str) -> String {
    if prompt_uses_contract(prompt) {
        stage_consumer_guidance()
    } else {
        String::new()
    }
}

/// Return prompt-review guidance when the prompt uses the canonical contract.
pub fn prompt_review_consumer_guidance_for_prompt(prompt: &str) -> String {
    if prompt_uses_contract(prompt) {
        prompt_review_consumer_guidance()
    } else {
        String::new()
    }
}

/// Validate that a prompt preserves the canonical marker and section order.
pub fn validate_canonical_prompt_shape(prompt: &str) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    if !prompt_uses_contract(prompt) {
        errors.push(format!(
            "missing exact contract marker `{}`",
            contract_marker()
        ));
    }

    let trimmed_lines: Vec<&str> = prompt.lines().map(str::trim).collect();
    let mut next_search_start = 0usize;
    for section in BEAD_TASK_PROMPT_SECTION_TITLES {
        let heading = format!("## {section}");
        match trimmed_lines[next_search_start..]
            .iter()
            .position(|line| *line == heading)
            .map(|offset| next_search_start + offset)
        {
            Some(index) => {
                next_search_start = index + 1;
            }
            None => errors.push(format!("missing section heading `{heading}`")),
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
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
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt"
        ));
        assert!(!prompt_uses_contract("# Ralph Task Prompt\nNo marker"));
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
    fn prompt_review_guidance_requires_exact_marker_preservation() {
        let guidance = prompt_review_consumer_guidance();
        assert!(guidance.contains(&contract_marker()));
        assert!(guidance.contains("keep the canonical section headings in the same order"));
    }

    #[test]
    fn canonical_prompt_shape_requires_marker_and_all_sections() {
        let valid_prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
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
    fn canonical_prompt_shape_ignores_duplicate_headings_inside_section_bodies() {
        let prompt = format!(
            "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nGoal:\nShip the thing.\n\n## Acceptance Criteria\n\nEmbedded bead marker that should stay inside the body.\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            contract_marker()
        );

        assert!(validate_canonical_prompt_shape(&prompt).is_ok());
    }
}

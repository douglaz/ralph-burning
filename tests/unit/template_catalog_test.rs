//! Unit tests for the template catalog: ID mapping, precedence, placeholder
//! validation, malformed-override handling, and rendering.

use ralph_burning::contexts::workspace_governance::template_catalog::{self, TemplateSource};
use ralph_burning::shared::domain::ProjectId;
use tempfile::tempdir;

// ── Template ID coverage ────────────────────────────────────────────────

#[test]
fn all_stage_ids_have_manifests() {
    for &id in template_catalog::STAGE_TEMPLATE_IDS {
        let m = template_catalog::manifest_for(id);
        assert!(m.is_some(), "missing manifest for stage template ID '{id}'");
    }
}

#[test]
fn all_panel_ids_have_manifests() {
    for &id in template_catalog::PANEL_TEMPLATE_IDS {
        let m = template_catalog::manifest_for(id);
        assert!(m.is_some(), "missing manifest for panel template ID '{id}'");
    }
}

#[test]
fn all_requirements_ids_have_manifests() {
    for &id in template_catalog::REQUIREMENTS_TEMPLATE_IDS {
        let m = template_catalog::manifest_for(id);
        assert!(
            m.is_some(),
            "missing manifest for requirements template ID '{id}'"
        );
    }
}

#[test]
fn unknown_template_id_returns_none() {
    assert!(template_catalog::manifest_for("nonexistent_xyz").is_none());
}

// ── Precedence resolution ───────────────────────────────────────────────

#[test]
fn resolves_built_in_default_when_no_overrides() {
    let tmp = tempdir().unwrap();
    let resolved = template_catalog::resolve("planning", tmp.path(), None).unwrap();
    assert_eq!(resolved.source, TemplateSource::BuiltIn);
    assert!(resolved.content.contains("{{role_instruction}}"));
}

#[test]
fn workspace_override_takes_precedence_over_built_in() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("planning.md"),
        "Custom: {{role_instruction}} | {{task_prompt_contract}} | {{project_prompt}} | {{json_schema}}",
    )
    .unwrap();

    let resolved = template_catalog::resolve("planning", tmp.path(), None).unwrap();
    assert!(matches!(
        resolved.source,
        TemplateSource::WorkspaceOverride(_)
    ));
    assert!(resolved.content.contains("Custom:"));
}

#[test]
fn project_override_takes_precedence_over_workspace() {
    let tmp = tempdir().unwrap();
    let pid = ProjectId::new("testproj".to_owned()).unwrap();

    // Workspace override
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("planning.md"),
        "Workspace: {{role_instruction}} | {{task_prompt_contract}} | {{project_prompt}} | {{json_schema}}",
    )
    .unwrap();

    // Project override
    let proj = tmp
        .path()
        .join(".ralph-burning")
        .join("projects")
        .join("testproj")
        .join("templates");
    std::fs::create_dir_all(&proj).unwrap();
    std::fs::write(
        proj.join("planning.md"),
        "Project: {{role_instruction}} | {{task_prompt_contract}} | {{project_prompt}} | {{json_schema}}",
    )
    .unwrap();

    let resolved = template_catalog::resolve("planning", tmp.path(), Some(&pid)).unwrap();
    assert!(matches!(
        resolved.source,
        TemplateSource::ProjectOverride(_)
    ));
    assert!(resolved.content.starts_with("Project:"));
}

#[test]
fn workspace_override_used_when_no_project_override() {
    let tmp = tempdir().unwrap();
    let pid = ProjectId::new("testproj".to_owned()).unwrap();

    // Only workspace override
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("planning.md"),
        "WS: {{role_instruction}} | {{task_prompt_contract}} | {{project_prompt}} | {{json_schema}}",
    )
    .unwrap();

    let resolved = template_catalog::resolve("planning", tmp.path(), Some(&pid)).unwrap();
    assert!(matches!(
        resolved.source,
        TemplateSource::WorkspaceOverride(_)
    ));
}

// ── Placeholder validation ──────────────────────────────────────────────

#[test]
fn unknown_placeholder_rejected() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("requirements_ideation.md"),
        "{{base_context}} and {{unknown_field}}",
    )
    .unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), None);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("unknown placeholder"));
    assert!(err.contains("unknown_field"));
}

#[test]
fn placeholder_with_hyphens_rejected_as_malformed() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("requirements_ideation.md"),
        "{{base_context}} and {{invented-placeholder}}",
    )
    .unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), None);
    assert!(result.is_err(), "hyphened placeholder must be rejected");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("unknown placeholder"));
    assert!(err.contains("invented-placeholder"));
}

#[test]
fn placeholder_with_spaces_rejected_as_malformed() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("requirements_ideation.md"),
        "{{base_context}} and {{with spaces}}",
    )
    .unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), None);
    assert!(result.is_err(), "spaced placeholder must be rejected");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("unknown placeholder"));
    assert!(err.contains("with spaces"));
}

#[test]
fn missing_required_placeholder_rejected() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("requirements_ideation.md"), "No placeholders here.").unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), None);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("missing required placeholder"));
    assert!(err.contains("base_context"));
}

#[test]
fn stage_override_missing_task_prompt_contract_placeholder_rejected() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("planning.md"),
        "{{role_instruction}} and {{project_prompt}} and {{json_schema}}",
    )
    .unwrap();

    let result = template_catalog::resolve("planning", tmp.path(), None);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("missing required placeholder"));
    assert!(err.contains("task_prompt_contract"));
}

#[test]
fn panel_override_missing_task_prompt_contract_placeholder_rejected() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("completion_panel_completer.md"),
        "{{prompt_text}} and {{json_schema}}",
    )
    .unwrap();

    let result = template_catalog::resolve("completion_panel_completer", tmp.path(), None);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("missing required placeholder"));
    assert!(err.contains("task_prompt_contract"));
}

#[test]
fn optional_placeholder_not_required() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    // requirements_draft has required: idea, optional: answers
    std::fs::write(ws.join("requirements_draft.md"), "Just the idea: {{idea}}").unwrap();

    let result = template_catalog::resolve("requirements_draft", tmp.path(), None);
    assert!(result.is_ok());
}

// ── Malformed override failure invariants ────────────────────────────────

#[test]
fn non_utf8_file_rejected() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("requirements_ideation.md"),
        &[0xFF, 0xFE, 0x00, 0x01],
    )
    .unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("UTF-8"));
}

#[test]
fn malformed_project_override_does_not_fallback_to_workspace() {
    let tmp = tempdir().unwrap();
    let pid = ProjectId::new("myproj".to_owned()).unwrap();

    // Valid workspace override
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("requirements_ideation.md"),
        "WS valid: {{base_context}}",
    )
    .unwrap();

    // Malformed project override (missing required placeholder)
    let proj = tmp
        .path()
        .join(".ralph-burning")
        .join("projects")
        .join("myproj")
        .join("templates");
    std::fs::create_dir_all(&proj).unwrap();
    std::fs::write(
        proj.join("requirements_ideation.md"),
        "Project malformed — no placeholders.",
    )
    .unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), Some(&pid));
    assert!(result.is_err(), "must not silently fall back to workspace");
}

#[test]
fn malformed_project_override_does_not_fallback_to_built_in() {
    let tmp = tempdir().unwrap();
    let pid = ProjectId::new("myproj".to_owned()).unwrap();

    // Malformed project override (unknown placeholder)
    let proj = tmp
        .path()
        .join(".ralph-burning")
        .join("projects")
        .join("myproj")
        .join("templates");
    std::fs::create_dir_all(&proj).unwrap();
    std::fs::write(
        proj.join("requirements_ideation.md"),
        "{{base_context}} {{invented_placeholder}}",
    )
    .unwrap();

    let result = template_catalog::resolve("requirements_ideation", tmp.path(), Some(&pid));
    assert!(result.is_err(), "must not silently fall back to built-in");
}

// ── Rendering ───────────────────────────────────────────────────────────

#[test]
fn render_replaces_all_placeholders() {
    let tmp = tempdir().unwrap();
    let resolved = template_catalog::resolve("requirements_research", tmp.path(), None).unwrap();
    let rendered = template_catalog::render(
        &resolved,
        &[
            ("base_context", "Build a widget"),
            ("ideation_artifact", "Widget themes identified"),
        ],
    )
    .unwrap();
    assert!(rendered.contains("Build a widget"));
    assert!(rendered.contains("Widget themes identified"));
    assert!(!rendered.contains("{{base_context}}"));
    assert!(!rendered.contains("{{ideation_artifact}}"));
}

#[test]
fn render_collapses_empty_optionals() {
    let tmp = tempdir().unwrap();
    let resolved = template_catalog::resolve("planning", tmp.path(), None).unwrap();
    let rendered = template_catalog::render(
        &resolved,
        &[
            ("role_instruction", "You are the Planner."),
            ("project_prompt", "Build X."),
            ("json_schema", "{}"),
            // prior_outputs and remediation are optional, omitted
        ],
    )
    .unwrap();
    assert!(!rendered.contains("{{prior_outputs}}"));
    assert!(!rendered.contains("{{remediation}}"));
    assert!(!rendered.contains("\n\n\n"));
}

#[test]
fn render_with_override_produces_custom_output() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("requirements_ideation.md"),
        "CUSTOM IDEATION\n\nContext: {{base_context}}\n\nEnd.",
    )
    .unwrap();

    let rendered = template_catalog::resolve_and_render(
        "requirements_ideation",
        tmp.path(),
        None,
        &[("base_context", "test idea")],
    )
    .unwrap();
    assert!(rendered.starts_with("CUSTOM IDEATION"));
    assert!(rendered.contains("test idea"));
}

// ── Verbatim block preservation ─────────────────────────────────────────

#[test]
fn render_preserves_verbatim_pre_rendered_blocks() {
    // Pre-rendered blocks (JSON schemas, multi-line artifacts) must survive
    // substitution intact. The blank-line normalizer should not corrupt
    // content that naturally contains consecutive blank lines within a
    // placeholder value, though runs of 3+ newlines are collapsed to 2.
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(
        ws.join("planning.md"),
        "HEADER\n\n{{role_instruction}}\n\n{{task_prompt_contract}}\n\n{{project_prompt}}\n\n{{json_schema}}",
    )
    .unwrap();

    let json_block = "{\n  \"type\": \"object\",\n  \"properties\": {\n    \"plan\": { \"type\": \"string\" }\n  }\n}";
    let multi_line_prompt = "Line one.\nLine two.\n\nParagraph two with a gap.";

    let rendered = template_catalog::resolve_and_render(
        "planning",
        tmp.path(),
        None,
        &[
            ("role_instruction", "You are the Planner."),
            ("task_prompt_contract", "Contract guidance"),
            ("project_prompt", multi_line_prompt),
            ("json_schema", json_block),
        ],
    )
    .unwrap();

    // JSON structure preserved
    assert!(
        rendered.contains(json_block),
        "JSON block must be preserved verbatim"
    );
    // Multi-line prompt preserved
    assert!(
        rendered.contains(multi_line_prompt),
        "multi-line prompt content must be preserved verbatim"
    );
    // No triple newlines after normalization
    assert!(
        !rendered.contains("\n\n\n"),
        "runs of 3+ newlines should be collapsed"
    );
}

// ── Resolve and render convenience ──────────────────────────────────────

#[test]
fn resolve_and_render_built_in_stage() {
    let tmp = tempdir().unwrap();
    let rendered = template_catalog::resolve_and_render(
        "implementation",
        tmp.path(),
        None,
        &[
            ("role_instruction", "You are the Implementer."),
            ("project_prompt", "Implement X"),
            ("json_schema", "{\"type\":\"object\"}"),
        ],
    )
    .unwrap();
    assert!(rendered.contains("You are the Implementer."));
    assert!(rendered.contains("Implement X"));
    assert!(rendered.contains("{\"type\":\"object\"}"));
}

#[test]
fn resolve_and_render_requirements_validation() {
    let tmp = tempdir().unwrap();
    let rendered = template_catalog::resolve_and_render(
        "requirements_validation",
        tmp.path(),
        None,
        &[
            ("synthesis_artifact", "Synthesis output"),
            ("impl_spec_artifact", "Spec output"),
            ("gap_artifact", "Gap output"),
        ],
    )
    .unwrap();
    assert!(rendered.contains("Synthesis output"));
    assert!(rendered.contains("Spec output"));
    assert!(rendered.contains("Gap output"));
}

// ── has_override ────────────────────────────────────────────────────────

#[test]
fn has_override_false_when_no_files() {
    let tmp = tempdir().unwrap();
    assert!(!template_catalog::has_override(
        "planning",
        tmp.path(),
        None
    ));
}

#[test]
fn has_override_true_when_workspace_exists() {
    let tmp = tempdir().unwrap();
    let ws = tmp.path().join(".ralph-burning").join("templates");
    std::fs::create_dir_all(&ws).unwrap();
    std::fs::write(ws.join("planning.md"), "test").unwrap();
    assert!(template_catalog::has_override("planning", tmp.path(), None));
}

// ── extract_placeholders ────────────────────────────────────────────────

#[test]
fn extract_handles_repeated_placeholder() {
    let phs = template_catalog::extract_placeholders("{{a}} and {{a}} and {{b}}");
    assert_eq!(phs.len(), 2);
    assert!(phs.contains("a"));
    assert!(phs.contains("b"));
}

#[test]
fn extract_handles_unclosed_braces() {
    // Greedy left-to-right: first {{ pairs with first }}, yielding an invalid
    // name ("unclosed and {{valid") that is rejected, so no placeholders found.
    let phs = template_catalog::extract_placeholders("{{unclosed and {{valid}}");
    assert_eq!(phs.len(), 0);
    // But a truly unclosed {{ at the end doesn't break preceding extractions.
    let phs2 = template_catalog::extract_placeholders("{{valid}} and {{unclosed");
    assert_eq!(phs2.len(), 1);
    assert!(phs2.contains("valid"));
}

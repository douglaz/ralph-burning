use ralph_burning::shared::domain::{
    BackendFamily, PanelBackendSpec, ProjectId, StageCursor, StageId,
};
use ralph_burning::shared::error::AppError;

#[test]
fn stage_cursor_rejects_zero_values() {
    assert!(StageCursor::new(StageId::Planning, 0, 1, 1).is_err());
    assert!(StageCursor::new(StageId::Planning, 1, 0, 1).is_err());
    assert!(StageCursor::new(StageId::Planning, 1, 1, 0).is_err());
}

#[test]
fn stage_cursor_cycle_and_completion_round_are_monotonic_and_independent() {
    let initial = StageCursor::initial(StageId::Planning);

    let next_cycle = initial.advance_cycle(StageId::Implementation).unwrap();
    assert_eq!(2, next_cycle.cycle);
    assert_eq!(1, next_cycle.completion_round);
    assert_eq!(1, next_cycle.attempt);

    let retry = next_cycle.retry().unwrap();
    assert_eq!(2, retry.cycle);
    assert_eq!(1, retry.completion_round);
    assert_eq!(2, retry.attempt);

    let next_completion_round = retry.advance_completion_round(StageId::Review).unwrap();
    assert_eq!(2, next_completion_round.cycle);
    assert_eq!(2, next_completion_round.completion_round);
    assert_eq!(1, next_completion_round.attempt);
}

#[test]
fn stage_cursor_retry_reports_attempt_overflow() {
    let cursor = StageCursor::new(StageId::Planning, 1, u32::MAX, 1).unwrap();

    let error = cursor.retry().expect_err("retry should overflow");

    assert!(matches!(
        error,
        AppError::StageCursorOverflow {
            field: "attempt",
            value: u32::MAX
        }
    ));
}

#[test]
fn stage_cursor_advance_cycle_reports_cycle_overflow() {
    let cursor = StageCursor::new(StageId::Planning, u32::MAX, 1, 1).unwrap();

    let error = cursor
        .advance_cycle(StageId::Implementation)
        .expect_err("cycle advance should overflow");

    assert!(matches!(
        error,
        AppError::StageCursorOverflow {
            field: "cycle",
            value: u32::MAX
        }
    ));
}

#[test]
fn stage_cursor_advance_completion_round_reports_round_overflow() {
    let cursor = StageCursor::new(StageId::Planning, 1, 1, u32::MAX).unwrap();

    let error = cursor
        .advance_completion_round(StageId::Review)
        .expect_err("completion round advance should overflow");

    assert!(matches!(
        error,
        AppError::StageCursorOverflow {
            field: "completion_round",
            value: u32::MAX
        }
    ));
}

#[test]
fn project_id_rejects_path_like_values() {
    for value in [
        "",
        ".",
        "..",
        "../escape",
        "nested/project",
        r"nested\project",
    ] {
        let error = ProjectId::new(value).expect_err("path-like project id should fail");
        assert!(matches!(error, AppError::InvalidIdentifier { .. }));
    }
}

#[test]
fn panel_backend_spec_parses_inline_model_overrides_and_optional_marker() {
    let required = "codex/gpt-5.4-xhigh"
        .parse::<PanelBackendSpec>()
        .expect("parse required panel backend");
    let optional = "?openrouter/openai/gpt-5.4"
        .parse::<PanelBackendSpec>()
        .expect("parse optional panel backend");

    assert_eq!(BackendFamily::Codex, required.selection().family);
    assert_eq!(Some("gpt-5.4-xhigh"), required.selection().model.as_deref());
    assert!(!required.is_optional());
    assert_eq!("codex/gpt-5.4-xhigh", required.to_string());

    assert_eq!(BackendFamily::OpenRouter, optional.selection().family);
    assert_eq!(
        Some("openai/gpt-5.4"),
        optional.selection().model.as_deref()
    );
    assert!(optional.is_optional());
    assert_eq!("?openrouter/openai/gpt-5.4", optional.to_string());
}

#[test]
fn panel_backend_spec_serde_round_trips_inline_model_overrides() {
    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    struct Wrapper {
        spec: PanelBackendSpec,
    }

    let spec = "?codex/gpt-5.3-codex-spark-xhigh"
        .parse::<PanelBackendSpec>()
        .expect("parse panel backend");
    let wrapper = Wrapper { spec };

    let json = serde_json::to_string(&wrapper).expect("serialize panel backend to json");
    let from_json: Wrapper =
        serde_json::from_str(&json).expect("deserialize panel backend from json");
    assert_eq!(wrapper, from_json);

    let toml = toml::to_string(&wrapper).expect("serialize panel backend to toml");
    let from_toml: Wrapper = toml::from_str(&toml).expect("deserialize panel backend from toml");
    assert_eq!(wrapper, from_toml);
}

#[test]
fn panel_backend_spec_parses_legacy_parenthesized_model_overrides_with_slashes() {
    let required = "openrouter(openai/gpt-5.4)"
        .parse::<PanelBackendSpec>()
        .expect("parse required legacy panel backend");
    let optional = "?openrouter(openai/gpt-5.4)"
        .parse::<PanelBackendSpec>()
        .expect("parse optional legacy panel backend");

    assert_eq!(BackendFamily::OpenRouter, required.selection().family);
    assert_eq!(
        Some("openai/gpt-5.4"),
        required.selection().model.as_deref()
    );
    assert!(!required.is_optional());

    assert_eq!(BackendFamily::OpenRouter, optional.selection().family);
    assert_eq!(
        Some("openai/gpt-5.4"),
        optional.selection().model.as_deref()
    );
    assert!(optional.is_optional());
}

#[test]
fn panel_backend_spec_serde_accepts_legacy_parenthesized_model_overrides() {
    #[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
    struct Wrapper {
        spec: PanelBackendSpec,
    }

    let from_json: Wrapper = serde_json::from_str(r#"{"spec":"?openrouter(openai/gpt-5.4)"}"#)
        .expect("deserialize legacy panel backend from json");
    assert_eq!(BackendFamily::OpenRouter, from_json.spec.selection().family);
    assert_eq!(
        Some("openai/gpt-5.4"),
        from_json.spec.selection().model.as_deref()
    );
    assert_eq!(
        r#"{"spec":"?openrouter/openai/gpt-5.4"}"#,
        serde_json::to_string(&from_json).expect("serialize normalized panel backend to json")
    );

    let from_toml: Wrapper = toml::from_str("spec = \"?openrouter(openai/gpt-5.4)\"")
        .expect("deserialize legacy panel backend from toml");
    assert_eq!(from_json, from_toml);
    assert!(toml::to_string(&from_toml)
        .expect("serialize normalized panel backend to toml")
        .contains("?openrouter/openai/gpt-5.4"));
}

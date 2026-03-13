use ralph_burning::contexts::conformance_spec::catalog;
use ralph_burning::contexts::conformance_spec::cutover_guard;
use ralph_burning::contexts::conformance_spec::model::{IdSource, ScenarioKind, ScenarioMeta};
use ralph_burning::contexts::conformance_spec::runner;
use ralph_burning::contexts::conformance_spec::scenarios;
use std::collections::HashMap;

// ===========================================================================
// Scenario ID extraction
// ===========================================================================

#[test]
fn discover_scenarios_finds_all_feature_files() {
    let scenarios = catalog::discover_scenarios().expect("discover scenarios");
    assert!(
        scenarios.len() >= 100,
        "expected at least 100 scenarios, got {}",
        scenarios.len()
    );
}

#[test]
fn discover_scenarios_deterministic_order() {
    let first = catalog::discover_scenarios().expect("first discovery");
    let second = catalog::discover_scenarios().expect("second discovery");

    assert_eq!(first.len(), second.len());
    for (a, b) in first.iter().zip(second.iter()) {
        assert_eq!(a.id, b.id, "deterministic order violated");
        assert_eq!(a.source_file, b.source_file);
        assert_eq!(a.source_line, b.source_line);
    }
}

#[test]
fn discover_scenarios_extracts_comment_ids() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    let sc_start = scenarios.iter().find(|s| s.id == "SC-START-001");
    assert!(sc_start.is_some(), "SC-START-001 should be discovered");
    let meta = sc_start.unwrap();
    assert_eq!(meta.id_source, IdSource::Comment);
    assert_eq!(meta.kind, ScenarioKind::Scenario);
}

#[test]
fn discover_scenarios_extracts_tag_ids() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    let ws_init = scenarios.iter().find(|s| s.id == "workspace-init-fresh");
    assert!(ws_init.is_some(), "workspace-init-fresh should be discovered");
    let meta = ws_init.unwrap();
    assert_eq!(meta.id_source, IdSource::Tag);
    assert_eq!(meta.kind, ScenarioKind::Scenario);
}

#[test]
fn discover_scenarios_handles_scenario_outlines() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    let outline = scenarios.iter().find(|s| s.id == "flow-show-each-preset");
    assert!(
        outline.is_some(),
        "flow-show-each-preset should be discovered"
    );
    let meta = outline.unwrap();
    assert_eq!(meta.kind, ScenarioKind::ScenarioOutline);
}

// ===========================================================================
// ID validation
// ===========================================================================

#[test]
fn validate_ids_accepts_unique_ids() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    assert!(catalog::validate_ids(&scenarios).is_ok());
}

#[test]
fn validate_ids_rejects_duplicates() {
    let scenarios = vec![
        ScenarioMeta {
            id: "DUP-001".to_owned(),
            feature_title: "Test".to_owned(),
            scenario_title: "First".to_owned(),
            source_file: "test.feature".to_owned(),
            source_line: 1,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
        ScenarioMeta {
            id: "DUP-001".to_owned(),
            feature_title: "Test".to_owned(),
            scenario_title: "Second".to_owned(),
            source_file: "test.feature".to_owned(),
            source_line: 5,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
    ];
    let result = catalog::validate_ids(&scenarios);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("duplicate"),
        "error should mention duplicate: {err}"
    );
}

#[test]
fn validate_ids_rejects_empty_ids() {
    let scenarios = vec![ScenarioMeta {
        id: "".to_owned(),
        feature_title: "Test".to_owned(),
        scenario_title: "Empty ID".to_owned(),
        source_file: "test.feature".to_owned(),
        source_line: 1,
        kind: ScenarioKind::Scenario,
        id_source: IdSource::Comment,
    }];
    assert!(catalog::validate_ids(&scenarios).is_err());
}

// ===========================================================================
// Deterministic ordering
// ===========================================================================

#[test]
fn scenarios_ordered_by_file_then_line() {
    let scenarios = catalog::discover_scenarios().expect("discover");

    let mut prev_file = "";
    let mut prev_line = 0;

    for s in &scenarios {
        if s.source_file == prev_file {
            assert!(
                s.source_line > prev_line,
                "within file {}, line {} should come after {}",
                s.source_file,
                s.source_line,
                prev_line
            );
        }
        prev_file = &s.source_file;
        prev_line = s.source_line;
    }
}

// ===========================================================================
// Registry completeness
// ===========================================================================

#[test]
fn registry_matches_discovered_scenarios() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    let registry = scenarios::build_registry();
    let result = runner::validate_registry(&scenarios, &registry);
    assert!(
        result.is_ok(),
        "registry drift: {}",
        result.unwrap_err()
    );
}

#[test]
fn registry_drift_detected_for_missing_executor() {
    let scenarios = vec![ScenarioMeta {
        id: "MISSING-EXECUTOR-999".to_owned(),
        feature_title: "Test".to_owned(),
        scenario_title: "Missing".to_owned(),
        source_file: "test.feature".to_owned(),
        source_line: 1,
        kind: ScenarioKind::Scenario,
        id_source: IdSource::Comment,
    }];
    let registry = HashMap::new();
    let result = runner::validate_registry(&scenarios, &registry);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("drift"));
}

#[test]
fn registry_drift_detected_for_orphan_executor() {
    let scenarios: Vec<ScenarioMeta> = vec![];
    let mut registry: HashMap<String, runner::ScenarioExecutor> = HashMap::new();
    registry.insert(
        "ORPHAN-001".to_owned(),
        Box::new(|| Ok(())),
    );
    let result = runner::validate_registry(&scenarios, &registry);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("drift"));
}

// ===========================================================================
// Fail-fast accounting
// ===========================================================================

#[test]
fn runner_fail_fast_stops_after_first_failure() {
    let scenarios = vec![
        ScenarioMeta {
            id: "PASS-1".to_owned(),
            feature_title: "T".to_owned(),
            scenario_title: "T".to_owned(),
            source_file: "t.feature".to_owned(),
            source_line: 1,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
        ScenarioMeta {
            id: "FAIL-1".to_owned(),
            feature_title: "T".to_owned(),
            scenario_title: "T".to_owned(),
            source_file: "t.feature".to_owned(),
            source_line: 2,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
        ScenarioMeta {
            id: "SKIP-1".to_owned(),
            feature_title: "T".to_owned(),
            scenario_title: "T".to_owned(),
            source_file: "t.feature".to_owned(),
            source_line: 3,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
    ];

    let mut registry: HashMap<String, runner::ScenarioExecutor> = HashMap::new();
    registry.insert("PASS-1".to_owned(), Box::new(|| Ok(())));
    registry.insert(
        "FAIL-1".to_owned(),
        Box::new(|| Err("intentional failure".to_owned())),
    );
    registry.insert("SKIP-1".to_owned(), Box::new(|| Ok(())));

    let refs: Vec<&ScenarioMeta> = scenarios.iter().collect();
    let report = runner::run_scenarios(&refs, &registry);

    assert_eq!(report.selected, 3);
    assert_eq!(report.passed, 1);
    assert_eq!(report.failed, 1);
    assert_eq!(report.not_run, 1);
}

#[test]
fn runner_all_pass_reports_correctly() {
    let scenarios = vec![ScenarioMeta {
        id: "PASS-ONLY".to_owned(),
        feature_title: "T".to_owned(),
        scenario_title: "T".to_owned(),
        source_file: "t.feature".to_owned(),
        source_line: 1,
        kind: ScenarioKind::Scenario,
        id_source: IdSource::Comment,
    }];

    let mut registry: HashMap<String, runner::ScenarioExecutor> = HashMap::new();
    registry.insert("PASS-ONLY".to_owned(), Box::new(|| Ok(())));

    let refs: Vec<&ScenarioMeta> = scenarios.iter().collect();
    let report = runner::run_scenarios(&refs, &registry);

    assert_eq!(report.selected, 1);
    assert_eq!(report.passed, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(report.not_run, 0);
}

// ===========================================================================
// Temp workspace isolation
// ===========================================================================

#[test]
fn runner_catches_panics_without_leaking() {
    let scenarios = vec![
        ScenarioMeta {
            id: "PANIC-1".to_owned(),
            feature_title: "T".to_owned(),
            scenario_title: "T".to_owned(),
            source_file: "t.feature".to_owned(),
            source_line: 1,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
        ScenarioMeta {
            id: "AFTER-PANIC".to_owned(),
            feature_title: "T".to_owned(),
            scenario_title: "T".to_owned(),
            source_file: "t.feature".to_owned(),
            source_line: 2,
            kind: ScenarioKind::Scenario,
            id_source: IdSource::Comment,
        },
    ];

    let mut registry: HashMap<String, runner::ScenarioExecutor> = HashMap::new();
    registry.insert(
        "PANIC-1".to_owned(),
        Box::new(|| panic!("test panic")),
    );
    registry.insert("AFTER-PANIC".to_owned(), Box::new(|| Ok(())));

    let refs: Vec<&ScenarioMeta> = scenarios.iter().collect();
    let report = runner::run_scenarios(&refs, &registry);

    assert_eq!(report.failed, 1);
    assert_eq!(report.not_run, 1);
}

// ===========================================================================
// Cutover guard
// ===========================================================================

#[test]
fn cutover_guard_passes_on_clean_source() {
    let src_dir = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
    let result = cutover_guard::check_cutover_guard(src_dir);
    assert!(
        result.is_ok(),
        "cutover guard should pass on production source: {}",
        result.unwrap_err()
    );
}

#[test]
fn cutover_guard_fails_on_legacy_pattern() {
    let tmp = std::env::temp_dir().join(format!("cutover-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).unwrap();
    std::fs::write(
        tmp.join("bad.rs"),
        "fn legacy() { let path = \".ralph/state\"; }\n",
    )
    .unwrap();

    let result = cutover_guard::check_cutover_guard(&tmp);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains(".ralph/"), "error should mention legacy pattern: {err}");

    let _ = std::fs::remove_dir_all(&tmp);
}

// ===========================================================================
// Filter resolution
// ===========================================================================

#[test]
fn filter_resolves_known_id() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    let result = runner::resolve_filter(&scenarios, "SC-START-001");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
}

#[test]
fn filter_rejects_unknown_id() {
    let scenarios = catalog::discover_scenarios().expect("discover");
    let result = runner::resolve_filter(&scenarios, "NONEXISTENT-999");
    assert!(result.is_err());
}

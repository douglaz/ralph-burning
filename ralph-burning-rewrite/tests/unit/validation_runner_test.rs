use std::path::Path;
use std::time::Duration;

use ralph_burning::adapters::validation_runner::{
    render_validation_group, run_command_group, run_pre_commit_checks, ValidationCommandResult,
    ValidationGroupResult, DEFAULT_VALIDATION_COMMAND_TIMEOUT_SECS,
};
use ralph_burning::contexts::workflow_composition::validation;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ── ValidationCommandResult / ValidationGroupResult unit tests ─────────────

#[test]
fn empty_command_list_is_noop_pass() {
    let rt = rt();
    let result = rt.block_on(run_command_group("test", &[], Path::new("/tmp"), Duration::from_secs(10)));
    assert!(result.passed);
    assert!(result.commands.is_empty());
    assert_eq!(result.group_name, "test");
}

#[test]
fn single_passing_command() {
    let rt = rt();
    let commands = vec!["true".to_owned()];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_secs(10),
    ));
    assert!(result.passed);
    assert_eq!(result.commands.len(), 1);
    assert!(result.commands[0].passed);
    assert_eq!(result.commands[0].exit_code, Some(0));
}

#[test]
fn single_failing_command() {
    let rt = rt();
    let commands = vec!["false".to_owned()];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_secs(10),
    ));
    assert!(!result.passed);
    assert_eq!(result.commands.len(), 1);
    assert!(!result.commands[0].passed);
    assert_eq!(result.commands[0].exit_code, Some(1));
}

#[test]
fn command_failure_stops_group() {
    let rt = rt();
    let commands = vec![
        "true".to_owned(),
        "false".to_owned(),
        "echo should-not-run".to_owned(),
    ];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_secs(10),
    ));
    assert!(!result.passed);
    // Only first two commands should be present (pass + fail).
    assert_eq!(result.commands.len(), 2);
    assert!(result.commands[0].passed);
    assert!(!result.commands[1].passed);
}

#[test]
fn command_captures_stdout_stderr() {
    let rt = rt();
    let commands = vec!["echo hello-stdout && echo hello-stderr >&2".to_owned()];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_secs(10),
    ));
    assert!(result.passed);
    assert!(result.commands[0].stdout.contains("hello-stdout"));
    assert!(result.commands[0].stderr.contains("hello-stderr"));
}

#[test]
fn command_timeout() {
    let rt = rt();
    let commands = vec!["sleep 60".to_owned()];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_millis(100),
    ));
    assert!(!result.passed);
    assert_eq!(result.commands.len(), 1);
    assert!(!result.commands[0].passed);
    assert!(result.commands[0].exit_code.is_none());
    assert!(result.commands[0].stderr.contains("timed out"));
}

#[test]
fn command_timeout_preserves_partial_output() {
    let rt = rt();
    // Emit output before sleeping so partial capture is possible.
    let commands = vec![
        "echo partial-stdout && echo partial-stderr >&2 && sleep 60".to_owned(),
    ];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_millis(500),
    ));
    assert!(!result.passed);
    let cmd = &result.commands[0];
    assert!(!cmd.passed);
    assert!(cmd.exit_code.is_none());
    // Partial stdout captured before timeout.
    assert!(
        cmd.stdout.contains("partial-stdout"),
        "expected partial stdout to be preserved, got: {:?}",
        cmd.stdout
    );
    // Partial stderr captured before timeout, plus the timeout message.
    assert!(
        cmd.stderr.contains("partial-stderr"),
        "expected partial stderr to be preserved, got: {:?}",
        cmd.stderr
    );
    assert!(
        cmd.stderr.contains("timed out"),
        "expected timeout message in stderr, got: {:?}",
        cmd.stderr
    );
}

#[test]
fn command_records_duration() {
    let rt = rt();
    let commands = vec!["true".to_owned()];
    let result = rt.block_on(run_command_group(
        "test",
        &commands,
        Path::new("/tmp"),
        Duration::from_secs(10),
    ));
    // Duration should be non-negative (it's a u64).
    assert!(result.commands[0].duration_ms < 10000);
}

#[test]
fn evidence_summary_format() {
    let result = ValidationGroupResult {
        group_name: "test".to_owned(),
        commands: vec![
            ValidationCommandResult {
                command: "echo ok".to_owned(),
                exit_code: Some(0),
                stdout: "ok".to_owned(),
                stderr: String::new(),
                duration_ms: 50,
                passed: true,
            },
            ValidationCommandResult {
                command: "false".to_owned(),
                exit_code: Some(1),
                stdout: String::new(),
                stderr: "err".to_owned(),
                duration_ms: 100,
                passed: false,
            },
        ],
        passed: false,
    };
    let evidence = result.evidence_summary();
    assert_eq!(evidence.len(), 2);
    assert!(evidence[0].contains("passed"));
    assert!(evidence[1].contains("failed"));
}

#[test]
fn failing_excerpts_from_stderr() {
    let result = ValidationGroupResult {
        group_name: "test".to_owned(),
        commands: vec![ValidationCommandResult {
            command: "cargo clippy".to_owned(),
            exit_code: Some(1),
            stdout: String::new(),
            stderr: "warning: unused variable".to_owned(),
            duration_ms: 100,
            passed: false,
        }],
        passed: false,
    };
    let excerpts = result.failing_excerpts();
    assert_eq!(excerpts.len(), 1);
    assert!(excerpts[0].contains("stderr"));
    assert!(excerpts[0].contains("unused variable"));
}

#[test]
fn render_validation_group_output() {
    let result = ValidationGroupResult {
        group_name: "docs_validation".to_owned(),
        commands: vec![ValidationCommandResult {
            command: "echo ok".to_owned(),
            exit_code: Some(0),
            stdout: "ok".to_owned(),
            stderr: String::new(),
            duration_ms: 50,
            passed: true,
        }],
        passed: true,
    };
    let rendered = render_validation_group(&result);
    assert!(rendered.contains("PASSED"));
    assert!(rendered.contains("docs_validation"));
    assert!(rendered.contains("echo ok"));
}

#[test]
fn default_timeout_is_900_seconds() {
    assert_eq!(DEFAULT_VALIDATION_COMMAND_TIMEOUT_SECS, 900);
}

// ── Pre-commit check tests ─────────────────────────────────────────────────

#[test]
fn pre_commit_all_disabled_is_noop() {
    let rt = rt();
    let result = rt.block_on(run_pre_commit_checks(
        Path::new("/tmp"),
        false, // fmt
        false, // clippy
        false, // nix_build
        false, // fmt_auto_fix
        Duration::from_secs(10),
    ));
    assert!(result.passed);
    assert!(result.commands.is_empty());
}

#[test]
fn pre_commit_no_cargo_toml_skips_cargo_checks() {
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();
    // No Cargo.toml in tmp dir.
    let result = rt.block_on(run_pre_commit_checks(
        tmp.path(),
        true,  // fmt enabled but no Cargo.toml
        true,  // clippy enabled but no Cargo.toml
        false, // nix_build
        false, // fmt_auto_fix
        Duration::from_secs(10),
    ));
    assert!(result.passed);
    assert!(result.commands.is_empty());
}

// ── Validation module: local validation ────────────────────────────────────

#[test]
fn local_validation_empty_commands_passes() {
    let rt = rt();
    let (payload, group) = rt.block_on(validation::run_local_validation(
        ralph_burning::shared::domain::StageId::DocsValidation,
        &[],
        Path::new("/tmp"),
    ));
    assert!(group.passed);
    assert!(
        payload.outcome
            == ralph_burning::contexts::workflow_composition::payloads::ReviewOutcome::Approved
    );
}

#[test]
fn local_validation_failing_command_requests_changes() {
    let rt = rt();
    let (payload, group) = rt.block_on(validation::run_local_validation(
        ralph_burning::shared::domain::StageId::CiValidation,
        &["false".to_owned()],
        Path::new("/tmp"),
    ));
    assert!(!group.passed);
    assert!(
        payload.outcome
            == ralph_burning::contexts::workflow_composition::payloads::ReviewOutcome::RequestChanges
    );
    assert!(!payload.findings_or_gaps.is_empty());
    assert!(!payload.follow_up_or_amendments.is_empty());
}

#[test]
fn local_validation_passing_command_approves() {
    let rt = rt();
    let (payload, group) = rt.block_on(validation::run_local_validation(
        ralph_burning::shared::domain::StageId::DocsValidation,
        &["true".to_owned()],
        Path::new("/tmp"),
    ));
    assert!(group.passed);
    assert!(
        payload.outcome
            == ralph_burning::contexts::workflow_composition::payloads::ReviewOutcome::Approved
    );
    assert!(payload.findings_or_gaps.is_empty());
}

// ── Pre-commit remediation context ─────────────────────────────────────────

#[test]
fn pre_commit_remediation_context_has_source_stage() {
    let result = ValidationGroupResult {
        group_name: "pre_commit".to_owned(),
        commands: vec![ValidationCommandResult {
            command: "cargo fmt --check".to_owned(),
            exit_code: Some(1),
            stdout: String::new(),
            stderr: "Diff in file.rs".to_owned(),
            duration_ms: 100,
            passed: false,
        }],
        passed: false,
    };

    let context = validation::pre_commit_remediation_context(&result);
    assert_eq!(
        context.get("source_stage").and_then(|v| v.as_str()),
        Some("pre_commit")
    );
    assert!(context.get("findings_or_gaps").is_some());
    assert!(context.get("follow_up_or_amendments").is_some());
}

// ── Build local validation context ─────────────────────────────────────────

#[test]
fn build_local_validation_context_structure() {
    let result = ValidationGroupResult {
        group_name: "standard_validation".to_owned(),
        commands: vec![ValidationCommandResult {
            command: "echo ok".to_owned(),
            exit_code: Some(0),
            stdout: "ok".to_owned(),
            stderr: String::new(),
            duration_ms: 50,
            passed: true,
        }],
        passed: true,
    };

    let ctx = validation::build_local_validation_context(&result);
    let lv = ctx.get("local_validation").unwrap();
    assert_eq!(lv.get("group").and_then(|v| v.as_str()), Some("standard_validation"));
    assert_eq!(lv.get("passed").and_then(|v| v.as_bool()), Some(true));
}

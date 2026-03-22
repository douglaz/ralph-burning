use std::path::Path;
use std::time::Duration;

use ralph_burning::adapters::validation_runner::{
    render_validation_group, run_command_group, run_pre_commit_checks, ValidationCommandResult,
    ValidationGroupResult, DEFAULT_VALIDATION_COMMAND_TIMEOUT_SECS,
};
use ralph_burning::contexts::workflow_composition::validation;

use super::env_test_support::{lock_path_mutex, PathGuard};

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
    let result = rt.block_on(run_command_group(
        "test",
        &[],
        Path::new("/tmp"),
        Duration::from_secs(10),
    ));
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
    let commands = vec!["echo partial-stdout && echo partial-stderr >&2 && sleep 60".to_owned()];
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

// ── Pre-commit: failed auto-fix keeps group failed ─────────────────────────

#[test]
fn pre_commit_fmt_auto_fix_failure_keeps_group_failed() {
    // Regression: if the auto-fix attempt (`cargo fmt`) itself fails, the
    // pre-commit group must remain failed even if the recheck somehow passes.
    // We simulate this with a stateful fake `cargo` script that:
    //   call 1 (fmt --check): fail
    //   call 2 (fmt):         fail  (repair fails)
    //   call 3 (fmt --check): pass  (recheck passes despite failed repair)
    let rt = rt();
    let tmp = tempfile::tempdir().unwrap();

    // Cargo.toml must exist for cargo checks to run.
    std::fs::write(tmp.path().join("Cargo.toml"), "[package]\nname = \"t\"\n").unwrap();

    // Create a fake `cargo` script that uses a counter file to vary behavior.
    let bin_dir = tmp.path().join("fake_bin");
    std::fs::create_dir(&bin_dir).unwrap();
    let counter_path = tmp.path().join("cargo_call_count");
    let fake_cargo = bin_dir.join("cargo");
    std::fs::write(
        &fake_cargo,
        format!(
            "#!/bin/sh\n\
             CF=\"{counter}\"\n\
             count=$(cat \"$CF\" 2>/dev/null || echo 0)\n\
             count=$((count + 1))\n\
             echo $count > \"$CF\"\n\
             # call 1: fmt --check fails; call 2: fmt fails; call 3: fmt --check passes\n\
             if [ \"$count\" -le 2 ]; then exit 1; fi\n\
             exit 0\n",
            counter = counter_path.display()
        ),
    )
    .unwrap();

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_cargo, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Serialize with other PATH-mutating tests to avoid race conditions.
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(&bin_dir);

    let result = rt.block_on(run_pre_commit_checks(
        tmp.path(),
        true,  // fmt
        false, // clippy
        false, // nix_build
        true,  // fmt_auto_fix
        Duration::from_secs(10),
    ));

    // The group must be failed because the repair attempt itself failed,
    // even though the recheck passed.
    assert!(
        !result.passed,
        "group should fail when auto-fix attempt itself fails"
    );
    // 3 commands: original fmt check, fmt fix attempt, fmt recheck.
    assert_eq!(
        result.commands.len(),
        3,
        "expected 3 commands: original check, fix attempt, recheck"
    );
    assert!(!result.commands[0].passed, "original fmt check should fail");
    assert!(!result.commands[1].passed, "fmt fix attempt should fail");
    // The recheck passes, but the group is still failed due to fix failure.
    assert!(
        result.commands[2].passed,
        "fmt recheck should pass (but group still fails)"
    );
}

// ── UTF-8 safe truncation ──────────────────────────────────────────────────

#[test]
fn failing_excerpts_handles_non_ascii_output() {
    // Regression: truncate_excerpt must not panic when multibyte UTF-8
    // characters cross the truncation boundary.
    let long_non_ascii = "日本語のエラーメッセージ".repeat(100); // ~3600 chars, ~10800 bytes
    let result = ValidationGroupResult {
        group_name: "test".to_owned(),
        commands: vec![ValidationCommandResult {
            command: "check".to_owned(),
            exit_code: Some(1),
            stdout: long_non_ascii.clone(),
            stderr: long_non_ascii,
            duration_ms: 50,
            passed: false,
        }],
        passed: false,
    };
    // This must not panic.
    let excerpts = result.failing_excerpts();
    assert_eq!(excerpts.len(), 1);
    // The excerpt should be truncated (contains the ellipsis marker).
    assert!(excerpts[0].contains('…'), "expected truncation marker");
    // And the excerpt should be valid UTF-8 (it compiled and didn't panic).
    assert!(excerpts[0].contains("stderr"));
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
    assert_eq!(
        lv.get("group").and_then(|v| v.as_str()),
        Some("standard_validation")
    );
    assert_eq!(lv.get("passed").and_then(|v| v.as_bool()), Some(true));
}

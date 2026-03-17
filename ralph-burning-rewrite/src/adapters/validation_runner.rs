#![forbid(unsafe_code)]

//! Local command runner for validation stages.
//!
//! Executes command groups via `sh -lc` from the repo root, captures structured
//! results (stdout, stderr, exit code, duration, pass/fail), and enforces a
//! per-command timeout. Never mutates run state directly.

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tokio::process::Command;

/// Default per-command timeout in seconds (15 minutes).
pub const DEFAULT_VALIDATION_COMMAND_TIMEOUT_SECS: u64 = 900;

// ── Structured result types ─────────────────────────────────────────────────

/// Result of executing a single validation command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationCommandResult {
    pub command: String,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub duration_ms: u64,
    pub passed: bool,
}

/// Result of executing a group of validation commands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationGroupResult {
    pub group_name: String,
    pub commands: Vec<ValidationCommandResult>,
    pub passed: bool,
}

impl ValidationGroupResult {
    /// Build a summary of evidence from the command results.
    pub fn evidence_summary(&self) -> Vec<String> {
        self.commands
            .iter()
            .map(|cmd| {
                if cmd.passed {
                    format!("`{}`: passed ({}ms)", cmd.command, cmd.duration_ms)
                } else if cmd.exit_code.is_none() {
                    format!("`{}`: timed out ({}ms)", cmd.command, cmd.duration_ms)
                } else {
                    format!(
                        "`{}`: failed (exit={}, {}ms)",
                        cmd.command,
                        cmd.exit_code.unwrap_or(-1),
                        cmd.duration_ms
                    )
                }
            })
            .collect()
    }

    /// Build excerpts from failing commands for follow-up items.
    pub fn failing_excerpts(&self) -> Vec<String> {
        self.commands
            .iter()
            .filter(|cmd| !cmd.passed)
            .map(|cmd| {
                let stderr_excerpt = truncate_excerpt(&cmd.stderr, 500);
                let stdout_excerpt = truncate_excerpt(&cmd.stdout, 500);
                if !stderr_excerpt.is_empty() {
                    format!("`{}` stderr: {}", cmd.command, stderr_excerpt)
                } else if !stdout_excerpt.is_empty() {
                    format!("`{}` stdout: {}", cmd.command, stdout_excerpt)
                } else {
                    format!("`{}`: no output captured", cmd.command)
                }
            })
            .collect()
    }
}

fn truncate_excerpt(text: &str, max_len: usize) -> String {
    let trimmed = text.trim();
    if trimmed.len() <= max_len {
        trimmed.to_owned()
    } else {
        // Find the last char boundary at or before max_len so that multibyte
        // UTF-8 sequences are never split, which would panic on slicing.
        let mut end = max_len;
        while !trimmed.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &trimmed[..end])
    }
}

// ── Command execution ───────────────────────────────────────────────────────

/// Execute a group of commands sequentially from `repo_root`.
///
/// If a command fails or times out, the group result is marked as failed.
/// Completed commands plus the failing command are preserved; later commands
/// are not executed.
pub async fn run_command_group(
    group_name: &str,
    commands: &[String],
    repo_root: &Path,
    timeout: Duration,
) -> ValidationGroupResult {
    if commands.is_empty() {
        return ValidationGroupResult {
            group_name: group_name.to_owned(),
            commands: vec![],
            passed: true,
        };
    }

    let mut results = Vec::with_capacity(commands.len());
    let mut group_passed = true;

    for cmd_str in commands {
        let result = run_single_command(cmd_str, repo_root, timeout).await;
        let passed = result.passed;
        results.push(result);
        if !passed {
            group_passed = false;
            break;
        }
    }

    ValidationGroupResult {
        group_name: group_name.to_owned(),
        commands: results,
        passed: group_passed,
    }
}

/// Execute a single command via `sh -lc` with a timeout.
async fn run_single_command(
    command: &str,
    repo_root: &Path,
    timeout: Duration,
) -> ValidationCommandResult {
    let start = Instant::now();

    let child_result = Command::new("sh")
        .arg("-lc")
        .arg(command)
        .current_dir(repo_root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();

    let mut child = match child_result {
        Ok(child) => child,
        Err(error) => {
            let duration_ms = start.elapsed().as_millis() as u64;
            return ValidationCommandResult {
                command: command.to_owned(),
                exit_code: None,
                stdout: String::new(),
                stderr: format!("failed to spawn command: {error}"),
                duration_ms,
                passed: false,
            };
        }
    };

    // Take stdout/stderr handles before the timeout so that read tasks survive
    // a timeout and we can preserve any partial output captured before the
    // child is killed.
    let stdout_handle = child.stdout.take();
    let stderr_handle = child.stderr.take();

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut stdout) = stdout_handle {
            let _ = stdout.read_to_end(&mut buf).await;
        }
        buf
    });
    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut stderr) = stderr_handle {
            let _ = stderr.read_to_end(&mut buf).await;
        }
        buf
    });

    let wait_result = tokio::time::timeout(timeout, child.wait()).await;
    let duration_ms = start.elapsed().as_millis() as u64;

    match wait_result {
        Ok(Ok(status)) => {
            let stdout_buf = stdout_task.await.unwrap_or_default();
            let stderr_buf = stderr_task.await.unwrap_or_default();
            let exit_code = status.code();
            let passed = status.success();
            ValidationCommandResult {
                command: command.to_owned(),
                exit_code,
                stdout: String::from_utf8_lossy(&stdout_buf).to_string(),
                stderr: String::from_utf8_lossy(&stderr_buf).to_string(),
                duration_ms,
                passed,
            }
        }
        Ok(Err(error)) => {
            let stdout_buf = stdout_task.await.unwrap_or_default();
            let stderr_buf = stderr_task.await.unwrap_or_default();
            ValidationCommandResult {
                command: command.to_owned(),
                exit_code: None,
                stdout: String::from_utf8_lossy(&stdout_buf).to_string(),
                stderr: format!(
                    "{}\nwait error: {error}",
                    String::from_utf8_lossy(&stderr_buf)
                ),
                duration_ms,
                passed: false,
            }
        }
        Err(_timeout) => {
            // Kill the child on timeout; this closes pipes so the read tasks
            // complete with whatever was captured so far.
            let _ = child.kill().await;
            let stdout_buf = stdout_task.await.unwrap_or_default();
            let stderr_buf = stderr_task.await.unwrap_or_default();
            let partial_stderr = String::from_utf8_lossy(&stderr_buf).to_string();
            let timeout_msg = format!("command timed out after {}s", timeout.as_secs());
            ValidationCommandResult {
                command: command.to_owned(),
                exit_code: None,
                stdout: String::from_utf8_lossy(&stdout_buf).to_string(),
                stderr: if partial_stderr.trim().is_empty() {
                    timeout_msg
                } else {
                    format!("{}\n{}", partial_stderr.trim(), timeout_msg)
                },
                duration_ms,
                passed: false,
            }
        }
    }
}

// ── Pre-commit check helpers ────────────────────────────────────────────────

/// Run pre-commit checks equivalent to old P0 behavior.
///
/// Checks are controlled by config booleans. Cargo-based checks are skipped
/// if `Cargo.toml` is not present in `repo_root`.
pub async fn run_pre_commit_checks(
    repo_root: &Path,
    pre_commit_fmt: bool,
    pre_commit_clippy: bool,
    pre_commit_nix_build: bool,
    pre_commit_fmt_auto_fix: bool,
    timeout: Duration,
) -> ValidationGroupResult {
    let has_cargo_toml = repo_root.join("Cargo.toml").is_file();
    let mut results = Vec::new();
    let mut group_passed = true;

    // cargo fmt --check
    if pre_commit_fmt && has_cargo_toml {
        let fmt_result = run_single_command("cargo fmt --check", repo_root, timeout).await;
        if !fmt_result.passed && pre_commit_fmt_auto_fix {
            // Record the original failure.
            let original_failure = fmt_result;
            results.push(original_failure);

            // Attempt auto-fix.
            let fix_result = run_single_command("cargo fmt", repo_root, timeout).await;
            let fix_passed = fix_result.passed;
            results.push(fix_result);

            // Rerun check.
            let recheck_result = run_single_command("cargo fmt --check", repo_root, timeout).await;
            let recheck_passed = recheck_result.passed;
            results.push(recheck_result);

            // The fmt sequence passes only when both the auto-fix command
            // succeeds AND the rerun of `cargo fmt --check` succeeds.
            if !fix_passed || !recheck_passed {
                group_passed = false;
            }
        } else if !fmt_result.passed {
            group_passed = false;
            results.push(fmt_result);
        } else {
            results.push(fmt_result);
        }
    }

    if !group_passed {
        return ValidationGroupResult {
            group_name: "pre_commit".to_owned(),
            commands: results,
            passed: false,
        };
    }

    // cargo clippy
    if pre_commit_clippy && has_cargo_toml {
        let clippy_result = run_single_command(
            "cargo clippy --all-targets -- -D warnings",
            repo_root,
            timeout,
        )
        .await;
        if !clippy_result.passed {
            group_passed = false;
        }
        results.push(clippy_result);
        if !group_passed {
            return ValidationGroupResult {
                group_name: "pre_commit".to_owned(),
                commands: results,
                passed: false,
            };
        }
    }

    // nix build
    if pre_commit_nix_build {
        let nix_result = run_single_command("nix build", repo_root, timeout).await;
        if !nix_result.passed {
            group_passed = false;
        }
        results.push(nix_result);
    }

    ValidationGroupResult {
        group_name: "pre_commit".to_owned(),
        commands: results,
        passed: group_passed,
    }
}

// ── Render helpers ──────────────────────────────────────────────────────────

/// Render a validation group result to deterministic Markdown for artifact
/// persistence.
pub fn render_validation_group(result: &ValidationGroupResult) -> String {
    use std::fmt::Write;

    let mut out = String::new();
    let status = if result.passed { "PASSED" } else { "FAILED" };
    writeln!(
        out,
        "# Local Validation: {} ({})",
        result.group_name, status
    )
    .unwrap();
    writeln!(out).unwrap();

    if result.commands.is_empty() {
        writeln!(out, "No commands configured.").unwrap();
        return out;
    }

    for cmd in &result.commands {
        let cmd_status = if cmd.passed {
            "PASS"
        } else if cmd.exit_code.is_none() {
            "TIMEOUT"
        } else {
            "FAIL"
        };
        writeln!(out, "## `{}` — {}", cmd.command, cmd_status).unwrap();
        writeln!(out).unwrap();
        if let Some(code) = cmd.exit_code {
            writeln!(out, "- Exit code: {code}").unwrap();
        }
        writeln!(out, "- Duration: {}ms", cmd.duration_ms).unwrap();
        writeln!(out).unwrap();

        if !cmd.stdout.trim().is_empty() {
            writeln!(out, "### stdout").unwrap();
            writeln!(out).unwrap();
            writeln!(out, "```").unwrap();
            writeln!(out, "{}", cmd.stdout.trim()).unwrap();
            writeln!(out, "```").unwrap();
            writeln!(out).unwrap();
        }
        if !cmd.stderr.trim().is_empty() {
            writeln!(out, "### stderr").unwrap();
            writeln!(out).unwrap();
            writeln!(out, "```").unwrap();
            writeln!(out, "{}", cmd.stderr.trim()).unwrap();
            writeln!(out, "```").unwrap();
            writeln!(out).unwrap();
        }
    }

    out
}

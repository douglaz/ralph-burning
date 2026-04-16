#![forbid(unsafe_code)]

//! Safety checks for `.beads/issues.jsonl` before mutating bead state.

use std::path::Path;

/// Operator-facing health classification for the local beads export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeadsHealthStatus {
    Healthy,
    ConflictMarkers,
    MissingFile,
    MalformedJsonl(String),
    BrUnavailable,
    IoError(String),
}

/// Inspect the local beads export and the `br` binary before mutation flows.
pub fn check_beads_health(base_dir: &Path) -> BeadsHealthStatus {
    check_beads_health_with(base_dir, br_available_for_health_check)
}

/// Return an operator-facing explanation when bead mutations must be blocked.
pub fn beads_health_failure_details(status: &BeadsHealthStatus) -> Option<String> {
    match status {
        BeadsHealthStatus::Healthy => None,
        BeadsHealthStatus::ConflictMarkers => Some(
            "detected git conflict markers in .beads/issues.jsonl; resolve the conflict before mutating beads"
                .to_owned(),
        ),
        BeadsHealthStatus::MissingFile => Some(
            "missing .beads/issues.jsonl; restore the beads export before mutating beads"
                .to_owned(),
        ),
        BeadsHealthStatus::MalformedJsonl(details) => Some(format!(
            "{details}; repair .beads/issues.jsonl before mutating beads"
        )),
        BeadsHealthStatus::BrUnavailable => Some(
            "the `br` binary is unavailable; restore `br` in PATH before mutating beads"
                .to_owned(),
        ),
        BeadsHealthStatus::IoError(details) => Some(format!(
            "{details}; restore readable bead state before mutating beads"
        )),
    }
}

fn check_beads_health_with<F>(base_dir: &Path, br_available: F) -> BeadsHealthStatus
where
    F: FnOnce() -> bool,
{
    let issues_path = base_dir.join(".beads/issues.jsonl");
    let issues_text = match std::fs::read_to_string(&issues_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return BeadsHealthStatus::MissingFile;
        }
        Err(error) => {
            return BeadsHealthStatus::IoError(format!(
                "failed to read {}: {error}",
                issues_path.display()
            ));
        }
    };

    if has_git_conflict_markers(&issues_text) {
        return BeadsHealthStatus::ConflictMarkers;
    }

    if let Err(details) = validate_jsonl_records(&issues_text) {
        return BeadsHealthStatus::MalformedJsonl(details);
    }

    if !br_available() {
        return BeadsHealthStatus::BrUnavailable;
    }

    BeadsHealthStatus::Healthy
}

fn has_git_conflict_markers(contents: &str) -> bool {
    contents.lines().any(is_git_conflict_marker_line)
}

fn is_git_conflict_marker_line(line: &str) -> bool {
    let trimmed = line.trim();
    trimmed == "======="
        || trimmed == "<<<<<<<"
        || trimmed.starts_with("<<<<<<< ")
        || trimmed == ">>>>>>>"
        || trimmed.starts_with(">>>>>>> ")
}

fn validate_jsonl_records(contents: &str) -> Result<(), String> {
    for (index, line) in contents.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let value = serde_json::from_str::<serde_json::Value>(trimmed).map_err(|error| {
            format!("malformed .beads/issues.jsonl line {}: {error}", index + 1)
        })?;
        let record = value.as_object().ok_or_else(|| {
            format!(
                "malformed .beads/issues.jsonl line {}: expected object record with non-empty string `id` field",
                index + 1
            )
        })?;
        let id = record.get("id").and_then(serde_json::Value::as_str);
        if id.is_none_or(|id| id.trim().is_empty()) {
            return Err(format!(
                "malformed .beads/issues.jsonl line {}: expected object record with non-empty string `id` field",
                index + 1
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
fn br_available_for_health_check() -> bool {
    true
}

#[cfg(not(test))]
fn br_available_for_health_check() -> bool {
    crate::adapters::br_process::BrAdapter::new()
        .check_available()
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_issues(base_dir: &Path, contents: &str) {
        let beads_dir = base_dir.join(".beads");
        std::fs::create_dir_all(&beads_dir).expect("create .beads dir");
        std::fs::write(beads_dir.join("issues.jsonl"), contents).expect("write issues.jsonl");
    }

    #[test]
    fn check_beads_health_detects_conflict_markers() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues(
            tmp.path(),
            r#"<<<<<<< HEAD
{"id":"bead-1"}
=======
{"id":"bead-2"}
>>>>>>> branch
"#,
        );

        let status = check_beads_health_with(tmp.path(), || true);

        assert_eq!(status, BeadsHealthStatus::ConflictMarkers);
        Ok(())
    }

    #[test]
    fn check_beads_health_reports_healthy_file() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues(tmp.path(), "{\"id\":\"bead-1\"}\n");

        let status = check_beads_health_with(tmp.path(), || true);

        assert_eq!(status, BeadsHealthStatus::Healthy);
        Ok(())
    }

    #[test]
    fn check_beads_health_reports_malformed_jsonl() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues(tmp.path(), "{\"id\":\"bead-1\"}\n{\"id\": }\n");

        let status = check_beads_health_with(tmp.path(), || true);

        match status {
            BeadsHealthStatus::MalformedJsonl(details) => {
                assert!(details.contains("line 2"), "details: {details}");
            }
            other => panic!("expected malformed jsonl status, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn check_beads_health_rejects_non_object_jsonl_records(
    ) -> Result<(), Box<dyn std::error::Error>> {
        for contents in ["[]\n", "42\n", "\"bead-1\"\n"] {
            let tmp = tempfile::tempdir()?;
            write_issues(tmp.path(), contents);

            let status = check_beads_health_with(tmp.path(), || true);

            match status {
                BeadsHealthStatus::MalformedJsonl(details) => {
                    assert!(
                        details.contains("expected object record with non-empty string `id` field"),
                        "details: {details}"
                    );
                }
                other => panic!("expected malformed jsonl status, got {other:?}"),
            }
        }
        Ok(())
    }

    #[test]
    fn check_beads_health_rejects_object_records_without_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues(tmp.path(), "{\"title\":\"missing id\"}\n");

        let status = check_beads_health_with(tmp.path(), || true);

        match status {
            BeadsHealthStatus::MalformedJsonl(details) => {
                assert!(
                    details.contains("expected object record with non-empty string `id` field"),
                    "details: {details}"
                );
            }
            other => panic!("expected malformed jsonl status, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn check_beads_health_reports_missing_file() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        std::fs::create_dir_all(tmp.path().join(".beads"))?;

        let status = check_beads_health_with(tmp.path(), || true);

        assert_eq!(status, BeadsHealthStatus::MissingFile);
        Ok(())
    }

    #[test]
    fn check_beads_health_ignores_marker_substrings_inside_json(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues(
            tmp.path(),
            r#"{"id":"bead-1","summary":"contains <<<<<<< text but no merge conflict"}
"#,
        );

        let status = check_beads_health_with(tmp.path(), || true);

        assert_eq!(status, BeadsHealthStatus::Healthy);
        Ok(())
    }

    #[test]
    fn check_beads_health_reports_io_errors_distinctly() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let beads_dir = tmp.path().join(".beads");
        std::fs::create_dir_all(&beads_dir)?;
        std::fs::create_dir(beads_dir.join("issues.jsonl"))?;

        let status = check_beads_health_with(tmp.path(), || true);

        assert!(
            matches!(status, BeadsHealthStatus::IoError(_)),
            "expected io error status, got {status:?}"
        );
        Ok(())
    }
}

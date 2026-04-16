#![forbid(unsafe_code)]

//! Safety checks for `.beads/issues.jsonl` before mutating bead state.

use std::path::Path;

/// Operator-facing health classification for the local beads export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeadsHealthStatus {
    Healthy,
    ConflictMarkers,
    MissingFile,
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

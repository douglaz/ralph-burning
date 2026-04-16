#![forbid(unsafe_code)]

//! Safety checks for `.beads/issues.jsonl` before mutating bead state.

use std::path::Path;

/// Operator-facing health classification for the local beads export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeadsHealthStatus {
    Healthy,
    ConflictMarkers,
    MissingFile,
    BrUnavailable,
}

/// Inspect the local beads export and the `br` binary before mutation flows.
pub fn check_beads_health(base_dir: &Path) -> BeadsHealthStatus {
    check_beads_health_with(base_dir, br_available_for_health_check)
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
        Err(_) => return BeadsHealthStatus::ConflictMarkers,
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
    contents.contains("<<<<<<<") || contents.contains("=======") || contents.contains(">>>>>>>")
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
}

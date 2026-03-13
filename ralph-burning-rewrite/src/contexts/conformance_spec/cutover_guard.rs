use std::path::Path;

use crate::shared::error::{AppError, AppResult};

/// Legacy runtime patterns that must not appear in production source code.
const LEGACY_PATTERNS: &[&str] = &[
    ".ralph/",
    "multibackend-orchestration",
    "multibackend_orchestration",
];

/// Scans production source code under `src/` for legacy runtime references.
///
/// Returns an error on the first violation found, including file path,
/// line number, and the offending pattern.
pub fn check_cutover_guard(src_dir: &Path) -> AppResult<()> {
    if !src_dir.is_dir() {
        return Err(AppError::ConformanceDiscoveryFailed {
            details: format!("source directory not found: {}", src_dir.display()),
        });
    }

    scan_directory(src_dir)
}

fn scan_directory(dir: &Path) -> AppResult<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| AppError::ConformanceDiscoveryFailed {
            details: format!("cannot read directory {}: {e}", dir.display()),
        })?
        .filter_map(|e| e.ok())
        .collect();

    entries.sort_by_key(|e| e.path());

    for entry in entries {
        let path = entry.path();
        if path.is_dir() {
            // Skip the conformance_spec context itself — it legitimately references
            // legacy patterns for the purpose of the cutover guard.
            let dir_name = path.file_name().unwrap_or_default().to_string_lossy();
            if dir_name == "conformance_spec" {
                continue;
            }
            scan_directory(&path)?;
        } else if path.extension().map_or(false, |ext| ext == "rs") {
            scan_file(&path)?;
        }
    }

    Ok(())
}

fn scan_file(path: &Path) -> AppResult<()> {
    let content = std::fs::read_to_string(path).map_err(|e| {
        AppError::ConformanceDiscoveryFailed {
            details: format!("cannot read {}: {e}", path.display()),
        }
    })?;

    let file_display = path.display().to_string();

    for (line_idx, line) in content.lines().enumerate() {
        let line_number = line_idx + 1;
        let trimmed = line.trim();

        // Skip comments — legacy references in comments are informational only
        if trimmed.starts_with("//") {
            continue;
        }

        for pattern in LEGACY_PATTERNS {
            if line.contains(pattern) {
                return Err(AppError::ConformanceCutoverViolation {
                    file: file_display,
                    line: line_number,
                    pattern: pattern.to_string(),
                });
            }
        }
    }

    Ok(())
}

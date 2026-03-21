use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::shared::error::{AppError, AppResult};

use super::model::{ConformanceReport, ScenarioMeta, ScenarioOutcome, ScenarioResult};

/// Type alias for a scenario executor function.
pub type ScenarioExecutor = Box<dyn Fn() -> Result<(), String> + Send + Sync>;

/// Validate that the registry and discovered scenarios are in one-to-one correspondence.
pub fn validate_registry(
    scenarios: &[ScenarioMeta],
    registry: &HashMap<String, ScenarioExecutor>,
) -> AppResult<()> {
    let mut errors = Vec::new();

    // Check every discovered scenario has a registered executor
    for scenario in scenarios {
        if !registry.contains_key(&scenario.id) {
            errors.push(format!(
                "discovered scenario '{}' ({}) has no registered executor",
                scenario.id, scenario.source_file
            ));
        }
    }

    // Check every registered executor maps to a discovered scenario
    let discovered_ids: std::collections::HashSet<&str> =
        scenarios.iter().map(|s| s.id.as_str()).collect();

    let mut orphan_ids: Vec<&str> = registry
        .keys()
        .filter(|id| !discovered_ids.contains(id.as_str()))
        .map(|s| s.as_str())
        .collect();
    orphan_ids.sort();

    for id in orphan_ids {
        errors.push(format!(
            "registered executor '{id}' has no corresponding discovered scenario"
        ));
    }

    if !errors.is_empty() {
        return Err(AppError::ConformanceRegistryDrift {
            details: errors.join("; "),
        });
    }

    Ok(())
}

/// Resolve the filter against discovered scenarios.
///
/// Returns the filtered subset in discovery order, or an error for unknown IDs.
pub fn resolve_filter<'a>(
    scenarios: &'a [ScenarioMeta],
    filter: &str,
) -> AppResult<Vec<&'a ScenarioMeta>> {
    let matched: Vec<&ScenarioMeta> = scenarios.iter().filter(|s| s.id == filter).collect();

    if matched.is_empty() {
        return Err(AppError::ConformanceFilterFailed {
            scenario_id: filter.to_owned(),
        });
    }

    Ok(matched)
}

/// Execute selected scenarios with fail-fast semantics.
///
/// Runs scenarios in the order given. On the first failure, stops immediately,
/// marks remaining scenarios as not-run, and returns the full report.
pub fn run_scenarios(
    selected: &[&ScenarioMeta],
    registry: &HashMap<String, ScenarioExecutor>,
) -> ConformanceReport {
    let wall_start = Instant::now();
    let total = selected.len();
    let mut results = Vec::with_capacity(total);
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut hit_failure = false;

    for (idx, scenario) in selected.iter().enumerate() {
        if hit_failure {
            // Mark remaining as not-run
            results.push(ScenarioResult {
                id: scenario.id.clone(),
                outcome: ScenarioOutcome::NotRun,
                duration: Duration::ZERO,
            });
            continue;
        }

        let executor = registry.get(&scenario.id).expect("validated registry");

        let scenario_start = Instant::now();

        // Test-only seam: force a specific executor to fail for CLI fail-fast testing.
        let forced_fail = std::env::var("RALPH_BURNING_TEST_CONFORMANCE_FAIL_EXECUTOR")
            .ok()
            .map_or(false, |id| id == scenario.id);

        // Execute with panic catching for isolation
        let exec_result = if forced_fail {
            Ok(Err(
                "forced failure via RALPH_BURNING_TEST_CONFORMANCE_FAIL_EXECUTOR".to_owned(),
            ))
        } else {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| executor()))
        };
        let scenario_duration = scenario_start.elapsed();

        match exec_result {
            Ok(Ok(())) => {
                eprintln!(
                    "  [{}/{}] {} ... PASS ({:.2}s)",
                    idx + 1,
                    total,
                    scenario.id,
                    scenario_duration.as_secs_f64()
                );
                results.push(ScenarioResult {
                    id: scenario.id.clone(),
                    outcome: ScenarioOutcome::Passed,
                    duration: scenario_duration,
                });
                passed += 1;
            }
            Ok(Err(reason)) => {
                eprintln!(
                    "  [{}/{}] {} ... FAIL ({:.2}s)",
                    idx + 1,
                    total,
                    scenario.id,
                    scenario_duration.as_secs_f64()
                );
                eprintln!("    Reason: {reason}");
                results.push(ScenarioResult {
                    id: scenario.id.clone(),
                    outcome: ScenarioOutcome::Failed(reason),
                    duration: scenario_duration,
                });
                failed += 1;
                hit_failure = true;
            }
            Err(panic_info) => {
                let reason = if let Some(msg) = panic_info.downcast_ref::<String>() {
                    msg.clone()
                } else if let Some(msg) = panic_info.downcast_ref::<&str>() {
                    msg.to_string()
                } else {
                    "panic (no message)".to_owned()
                };
                eprintln!(
                    "  [{}/{}] {} ... PANIC ({:.2}s)",
                    idx + 1,
                    total,
                    scenario.id,
                    scenario_duration.as_secs_f64()
                );
                eprintln!("    Reason: {reason}");
                results.push(ScenarioResult {
                    id: scenario.id.clone(),
                    outcome: ScenarioOutcome::Failed(format!("panic: {reason}")),
                    duration: scenario_duration,
                });
                failed += 1;
                hit_failure = true;
            }
        }
    }

    let not_run = total - passed - failed;

    ConformanceReport {
        selected: total,
        passed,
        failed,
        not_run,
        wall_clock: wall_start.elapsed(),
        results,
    }
}

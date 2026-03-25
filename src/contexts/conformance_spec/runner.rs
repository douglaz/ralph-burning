use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::shared::error::{AppError, AppResult};

use super::model::{ConformanceReport, ScenarioMeta, ScenarioOutcome, ScenarioResult};

/// Outcome returned by a scenario executor.
#[derive(Debug, Clone)]
pub enum ExecOutcome {
    /// The scenario executed and passed.
    Passed,
    /// The scenario was intentionally skipped (with reason).
    Skipped(String),
}

/// Type alias for a scenario executor function.
pub type ScenarioExecutor = Box<dyn Fn() -> Result<ExecOutcome, String> + Send + Sync>;

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
/// Runs scenarios in parallel across threads. On the first failure, signals
/// remaining in-flight scenarios to skip (they complete their current work
/// but no new scenarios are started), marks unstarted scenarios as not-run,
/// and returns the full report with results in the original discovery order.
pub fn run_scenarios(
    selected: &[&ScenarioMeta],
    registry: &HashMap<String, ScenarioExecutor>,
) -> ConformanceReport {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let wall_start = Instant::now();
    let total = selected.len();
    let failed_flag = Arc::new(AtomicBool::new(false));

    // Build work items preserving discovery order index.
    let work: Vec<(usize, &ScenarioMeta, &ScenarioExecutor)> = selected
        .iter()
        .enumerate()
        .map(|(idx, scenario)| {
            let executor = registry.get(&scenario.id).expect("validated registry");
            (idx, *scenario, executor)
        })
        .collect();

    // Execute in parallel using a scoped thread pool.
    let parallelism = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let mut indexed_results: Vec<(usize, ScenarioResult)> = Vec::with_capacity(total);

    std::thread::scope(|scope| {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut _spawned = 0;

        // Chunk work into batches to limit concurrency.
        for chunk in work.chunks(parallelism) {
            let mut handles = Vec::new();
            for &(idx, scenario, executor) in chunk {
                if failed_flag.load(Ordering::Relaxed) {
                    // Fail-fast: don't start new scenarios after a failure.
                    let _ = tx.send((
                        idx,
                        ScenarioResult {
                            id: scenario.id.clone(),
                            outcome: ScenarioOutcome::NotRun,
                            duration: Duration::ZERO,
                        },
                    ));
                    _spawned += 1;
                    continue;
                }

                let flag = failed_flag.clone();
                let tx = tx.clone();
                handles.push(scope.spawn(move || {
                    let scenario_start = Instant::now();

                    let forced_fail = std::env::var("RALPH_BURNING_TEST_CONFORMANCE_FAIL_EXECUTOR")
                        .ok()
                        .is_some_and(|id| id == scenario.id);

                    let exec_result = if forced_fail {
                        Ok(Err(
                            "forced failure via RALPH_BURNING_TEST_CONFORMANCE_FAIL_EXECUTOR"
                                .to_owned(),
                        ))
                    } else {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(executor))
                    };
                    let duration = scenario_start.elapsed();

                    let result = match exec_result {
                        Ok(Ok(ExecOutcome::Passed)) => ScenarioResult {
                            id: scenario.id.clone(),
                            outcome: ScenarioOutcome::Passed,
                            duration,
                        },
                        Ok(Ok(ExecOutcome::Skipped(reason))) => {
                            eprintln!("    Reason: {reason}");
                            ScenarioResult {
                                id: scenario.id.clone(),
                                outcome: ScenarioOutcome::NotRun,
                                duration,
                            }
                        }
                        Ok(Err(reason)) => {
                            flag.store(true, Ordering::Relaxed);
                            ScenarioResult {
                                id: scenario.id.clone(),
                                outcome: ScenarioOutcome::Failed(reason),
                                duration,
                            }
                        }
                        Err(panic_info) => {
                            flag.store(true, Ordering::Relaxed);
                            let reason = if let Some(msg) = panic_info.downcast_ref::<String>() {
                                msg.clone()
                            } else if let Some(msg) = panic_info.downcast_ref::<&str>() {
                                msg.to_string()
                            } else {
                                "panic (no message)".to_owned()
                            };
                            ScenarioResult {
                                id: scenario.id.clone(),
                                outcome: ScenarioOutcome::Failed(format!("panic: {reason}")),
                                duration,
                            }
                        }
                    };
                    let _ = tx.send((idx, result));
                }));
                _spawned += 1;
            }

            // Wait for this batch to complete before starting the next.
            for handle in handles {
                let _ = handle.join();
            }
        }

        drop(tx);

        // Collect results from channel.
        for received in rx {
            indexed_results.push(received);
        }
    });

    // Sort by original discovery order.
    indexed_results.sort_by_key(|(idx, _)| *idx);

    let mut passed = 0usize;
    let mut failed = 0usize;

    for (idx, result) in &indexed_results {
        let label = match &result.outcome {
            ScenarioOutcome::Passed => {
                passed += 1;
                "PASS"
            }
            ScenarioOutcome::Failed(reason) => {
                failed += 1;
                eprintln!("    Reason: {reason}");
                "FAIL"
            }
            ScenarioOutcome::NotRun => "SKIP",
        };
        eprintln!(
            "  [{}/{}] {} ... {} ({:.2}s)",
            idx + 1,
            total,
            result.id,
            label,
            result.duration.as_secs_f64()
        );
    }

    let results: Vec<ScenarioResult> = indexed_results.into_iter().map(|(_, r)| r).collect();
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

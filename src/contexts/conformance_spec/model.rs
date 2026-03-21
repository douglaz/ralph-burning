use std::fmt;
use std::time::Duration;

/// Whether the Gherkin entry is a plain Scenario or a Scenario Outline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioKind {
    Scenario,
    ScenarioOutline,
}

impl fmt::Display for ScenarioKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scenario => f.write_str("Scenario"),
            Self::ScenarioOutline => f.write_str("Scenario Outline"),
        }
    }
}

/// Where the scenario ID was extracted from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdSource {
    Comment,
    Tag,
}

/// Metadata for a single discovered conformance scenario.
#[derive(Debug, Clone)]
pub struct ScenarioMeta {
    pub id: String,
    pub feature_title: String,
    pub scenario_title: String,
    pub source_file: String,
    pub source_line: usize,
    pub kind: ScenarioKind,
    pub id_source: IdSource,
}

/// Outcome of a single scenario execution.
#[derive(Debug, Clone)]
pub enum ScenarioOutcome {
    Passed,
    Failed(String),
    NotRun,
}

impl fmt::Display for ScenarioOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Passed => f.write_str("PASS"),
            Self::Failed(reason) => write!(f, "FAIL: {reason}"),
            Self::NotRun => f.write_str("NOT RUN"),
        }
    }
}

/// Result of executing a single conformance scenario.
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    pub id: String,
    pub outcome: ScenarioOutcome,
    pub duration: Duration,
}

/// Summary report from a conformance run.
#[derive(Debug)]
pub struct ConformanceReport {
    pub selected: usize,
    pub passed: usize,
    pub failed: usize,
    pub not_run: usize,
    pub wall_clock: Duration,
    pub results: Vec<ScenarioResult>,
}

impl fmt::Display for ConformanceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f)?;
        writeln!(f, "Conformance Summary")?;
        writeln!(f, "  Selected:  {}", self.selected)?;
        writeln!(f, "  Passed:    {}", self.passed)?;
        writeln!(f, "  Failed:    {}", self.failed)?;
        writeln!(f, "  Not run:   {}", self.not_run)?;
        writeln!(f, "  Duration:  {:.2}s", self.wall_clock.as_secs_f64())?;
        Ok(())
    }
}

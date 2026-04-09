#![forbid(unsafe_code)]

//! Planned-elsewhere reconciliation handler.
//!
//! When the review stage classifies a finding as "planned-elsewhere"
//! (valid concern but already covered by another bead), this handler:
//! 1. Verifies the mapped-to bead exists (falls back gracefully if stale)
//! 2. Records a `PlannedElsewhereMapping` in milestone state
//! 3. Emits a journal event for audit
//! 4. Optionally adds a `br comment` on the mapped-to bead
//! 5. Allows the active bead to proceed without reopen/fix loops

use std::path::Path;

use chrono::{DateTime, Utc};

use crate::adapters::br_models::BeadStatus;
use crate::adapters::br_process::{BrAdapter, BrCommand, BrMutationAdapter, ProcessRunner};
use crate::adapters::fs::{FsMilestoneJournalStore, FsPlannedElsewhereMappingStore};
use crate::contexts::milestone_record::model::{MilestoneId, PlannedElsewhereMapping};
use crate::contexts::milestone_record::service as milestone_service;
use crate::shared::error::AppResult;

/// Input for recording a single planned-elsewhere finding.
#[derive(Debug, Clone)]
pub struct PlannedElsewhereInput {
    /// The bead whose review produced the finding.
    pub active_bead_id: String,
    /// Human-readable summary of the finding.
    pub finding_summary: String,
    /// Bead ID that already owns the concern.
    pub mapped_to_bead_id: String,
}

/// Outcome of processing a single planned-elsewhere finding.
#[derive(Debug, Clone)]
pub struct PlannedElsewhereOutcome {
    /// The mapping that was persisted.
    pub mapping: PlannedElsewhereMapping,
    /// Whether the mapped-to bead was verified to exist.
    pub bead_verified: bool,
    /// Whether the optional comment was posted.
    pub comment_posted: bool,
    /// Warning if the mapped-to bead is stale (doesn't exist or closed).
    pub stale_warning: Option<String>,
}

/// Check whether a bead exists and is not closed.
async fn verify_bead_exists<R: ProcessRunner>(
    br_read: &BrAdapter<R>,
    bead_id: &str,
) -> (bool, Option<String>) {
    use crate::adapters::br_models::BeadDetail;
    let cmd = BrCommand::show(bead_id);
    match br_read.exec_json::<BeadDetail>(&cmd).await {
        Ok(detail) => {
            if detail.status == BeadStatus::Closed {
                (
                    false,
                    Some(format!(
                        "mapped-to bead '{bead_id}' exists but is already closed"
                    )),
                )
            } else {
                (true, None)
            }
        }
        Err(e) => (
            false,
            Some(format!(
                "mapped-to bead '{bead_id}' could not be verified: {e}"
            )),
        ),
    }
}

/// Process a single planned-elsewhere finding:
/// - Verify the mapped-to bead exists
/// - Record the mapping in milestone state
/// - Optionally post a comment on the mapped-to bead
pub async fn reconcile_planned_elsewhere<R: ProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    br_read: &BrAdapter<R>,
    base_dir: &Path,
    milestone_id_str: &str,
    input: &PlannedElsewhereInput,
    post_comment: bool,
    now: DateTime<Utc>,
) -> AppResult<PlannedElsewhereOutcome> {
    let milestone_id = MilestoneId::new(milestone_id_str)?;

    // Step 1: Verify the mapped-to bead exists.
    let (bead_verified, stale_warning) =
        verify_bead_exists(br_read, &input.mapped_to_bead_id).await;

    if let Some(ref warning) = stale_warning {
        tracing::warn!(
            active_bead_id = input.active_bead_id.as_str(),
            mapped_to_bead_id = input.mapped_to_bead_id.as_str(),
            warning = warning.as_str(),
            "planned-elsewhere mapping references stale bead"
        );
    }

    // Step 2: Build and persist the mapping.
    let mapping = PlannedElsewhereMapping {
        active_bead_id: input.active_bead_id.clone(),
        finding_summary: input.finding_summary.clone(),
        mapped_to_bead_id: input.mapped_to_bead_id.clone(),
        recorded_at: now,
        mapped_bead_verified: bead_verified,
    };

    milestone_service::record_planned_elsewhere_mapping(
        &FsMilestoneJournalStore,
        &FsPlannedElsewhereMappingStore,
        base_dir,
        &milestone_id,
        &mapping,
    )?;

    // Step 3: Optionally post a comment on the mapped-to bead.
    let comment_posted = if post_comment && bead_verified {
        let comment_text = format!(
            "Planned-elsewhere mapping from {}: {}",
            input.active_bead_id, input.finding_summary
        );
        match br_mutation
            .comment_bead(&input.mapped_to_bead_id, &comment_text)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    mapped_to_bead_id = input.mapped_to_bead_id.as_str(),
                    active_bead_id = input.active_bead_id.as_str(),
                    "posted planned-elsewhere comment on mapped-to bead"
                );
                true
            }
            Err(e) => {
                tracing::warn!(
                    mapped_to_bead_id = input.mapped_to_bead_id.as_str(),
                    error = %e,
                    "failed to post planned-elsewhere comment (non-blocking)"
                );
                false
            }
        }
    } else {
        false
    };

    Ok(PlannedElsewhereOutcome {
        mapping,
        bead_verified,
        comment_posted,
        stale_warning,
    })
}

/// Process a batch of planned-elsewhere findings for a single bead.
/// Returns outcomes for each finding. Failures in individual mappings
/// do not prevent the active bead from proceeding.
pub async fn reconcile_planned_elsewhere_batch<R: ProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    br_read: &BrAdapter<R>,
    base_dir: &Path,
    milestone_id_str: &str,
    inputs: &[PlannedElsewhereInput],
    post_comments: bool,
    now: DateTime<Utc>,
) -> Vec<Result<PlannedElsewhereOutcome, String>> {
    let mut outcomes = Vec::with_capacity(inputs.len());
    for input in inputs {
        match reconcile_planned_elsewhere(
            br_mutation,
            br_read,
            base_dir,
            milestone_id_str,
            input,
            post_comments,
            now,
        )
        .await
        {
            Ok(outcome) => outcomes.push(Ok(outcome)),
            Err(e) => {
                tracing::warn!(
                    active_bead_id = input.active_bead_id.as_str(),
                    mapped_to_bead_id = input.mapped_to_bead_id.as_str(),
                    error = %e,
                    "failed to record planned-elsewhere mapping (non-blocking)"
                );
                outcomes.push(Err(e.to_string()));
            }
        }
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::br_process::{BrError, BrOutput, ProcessRunner};
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::Duration;

    use crate::adapters::fs::{FsMilestoneJournalStore, FsMilestoneStore};
    use crate::contexts::milestone_record::model::{
        MilestoneEventType, MilestoneRecord, MilestoneSnapshot,
    };
    use crate::contexts::milestone_record::service::{MilestoneJournalPort, MilestoneStorePort};

    struct MockBrRunner {
        responses: Mutex<Vec<Result<BrOutput, BrError>>>,
    }

    impl MockBrRunner {
        fn new(responses: Vec<Result<BrOutput, BrError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }

        fn success(stdout: &str) -> Result<BrOutput, BrError> {
            Ok(BrOutput {
                stdout: stdout.to_owned(),
                stderr: String::new(),
                exit_code: 0,
            })
        }

        fn error(exit_code: i32, stderr: &str) -> Result<BrOutput, BrError> {
            Err(BrError::BrExitError {
                exit_code,
                stdout: String::new(),
                stderr: stderr.to_owned(),
                command: "br mock".to_owned(),
            })
        }
    }

    impl ProcessRunner for MockBrRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&Path>,
        ) -> Result<BrOutput, BrError> {
            self.responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| panic!("MockBrRunner: no more responses"))
        }
    }

    fn setup_milestone(base_dir: &Path, milestone_id: &MilestoneId) {
        let now = Utc::now();
        let record = MilestoneRecord::new(
            milestone_id.clone(),
            "test-ms".to_owned(),
            "test milestone".to_owned(),
            now,
        );
        let snapshot = MilestoneSnapshot::initial(now);
        let event = crate::contexts::milestone_record::model::MilestoneJournalEvent::new(
            MilestoneEventType::Created,
            now,
        );
        let journal_line = event.to_ndjson_line().unwrap();
        FsMilestoneStore
            .create_milestone_atomic(base_dir, &record, &snapshot, &journal_line)
            .unwrap();
    }

    #[tokio::test]
    async fn successful_mapping_with_verified_bead() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base_dir = tmp.path();
        let milestone_id = MilestoneId::new("test-ms")?;
        setup_milestone(base_dir, &milestone_id);

        // br show returns an open bead
        let show_json = r#"{"id":"other-bead","title":"Other","status":"open","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let input = PlannedElsewhereInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Should handle edge case X".to_owned(),
            mapped_to_bead_id: "other-bead".to_owned(),
        };

        let now = Utc::now();
        let outcome = reconcile_planned_elsewhere(
            &br_mutation,
            &br_read,
            base_dir,
            "test-ms",
            &input,
            false,
            now,
        )
        .await?;

        assert!(outcome.bead_verified);
        assert!(outcome.stale_warning.is_none());
        assert!(!outcome.comment_posted);
        assert_eq!(outcome.mapping.active_bead_id, "active-bead");
        assert_eq!(outcome.mapping.mapped_to_bead_id, "other-bead");
        assert_eq!(outcome.mapping.finding_summary, "Should handle edge case X");

        // Verify persistence: read back mappings
        let mappings = milestone_service::load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            base_dir,
            &milestone_id,
        )?;
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0].active_bead_id, "active-bead");
        assert!(mappings[0].mapped_bead_verified);

        // Verify journal event was recorded
        let journal = FsMilestoneJournalStore.read_journal(base_dir, &milestone_id)?;
        let pe_events: Vec<_> = journal
            .iter()
            .filter(|e| e.event_type == MilestoneEventType::PlannedElsewhereMapped)
            .collect();
        assert_eq!(pe_events.len(), 1);
        assert_eq!(pe_events[0].bead_id.as_deref(), Some("active-bead"));
        assert_eq!(
            pe_events[0].details.as_deref(),
            Some("Should handle edge case X")
        );
        let metadata = pe_events[0].metadata.as_ref().unwrap();
        assert_eq!(metadata["mapped_to_bead_id"], "other-bead");
        assert_eq!(metadata["mapped_bead_verified"], true);

        Ok(())
    }

    #[tokio::test]
    async fn stale_bead_fallback_with_warning() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base_dir = tmp.path();
        let milestone_id = MilestoneId::new("test-ms")?;
        setup_milestone(base_dir, &milestone_id);

        // br show returns an error (bead doesn't exist)
        let read_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "bead not found")]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let input = PlannedElsewhereInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Edge case Y".to_owned(),
            mapped_to_bead_id: "nonexistent-bead".to_owned(),
        };

        let now = Utc::now();
        let outcome = reconcile_planned_elsewhere(
            &br_mutation,
            &br_read,
            base_dir,
            "test-ms",
            &input,
            false,
            now,
        )
        .await?;

        assert!(!outcome.bead_verified);
        assert!(outcome.stale_warning.is_some());
        assert!(outcome.stale_warning.unwrap().contains("nonexistent-bead"));
        assert!(!outcome.comment_posted);

        // Mapping is still persisted with verified=false
        let mappings = milestone_service::load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            base_dir,
            &milestone_id,
        )?;
        assert_eq!(mappings.len(), 1);
        assert!(!mappings[0].mapped_bead_verified);

        Ok(())
    }

    #[tokio::test]
    async fn closed_bead_is_stale() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base_dir = tmp.path();
        let milestone_id = MilestoneId::new("test-ms")?;
        setup_milestone(base_dir, &milestone_id);

        // br show returns a closed bead
        let show_json = r#"{"id":"closed-bead","title":"Done","status":"closed","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let input = PlannedElsewhereInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Edge case Z".to_owned(),
            mapped_to_bead_id: "closed-bead".to_owned(),
        };

        let now = Utc::now();
        let outcome = reconcile_planned_elsewhere(
            &br_mutation,
            &br_read,
            base_dir,
            "test-ms",
            &input,
            false,
            now,
        )
        .await?;

        assert!(!outcome.bead_verified);
        assert!(outcome
            .stale_warning
            .as_deref()
            .unwrap()
            .contains("already closed"));

        Ok(())
    }

    #[tokio::test]
    async fn comment_posted_on_verified_bead() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base_dir = tmp.path();
        let milestone_id = MilestoneId::new("test-ms")?;
        setup_milestone(base_dir, &milestone_id);

        // br show returns open bead
        let show_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_read = BrAdapter::with_runner(read_runner);

        // comment mutation succeeds
        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let input = PlannedElsewhereInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Handle retry logic".to_owned(),
            mapped_to_bead_id: "target-bead".to_owned(),
        };

        let now = Utc::now();
        let outcome = reconcile_planned_elsewhere(
            &br_mutation,
            &br_read,
            base_dir,
            "test-ms",
            &input,
            true,
            now,
        )
        .await?;

        assert!(outcome.bead_verified);
        assert!(outcome.comment_posted);

        Ok(())
    }

    #[tokio::test]
    async fn comment_skipped_for_stale_bead() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base_dir = tmp.path();
        let milestone_id = MilestoneId::new("test-ms")?;
        setup_milestone(base_dir, &milestone_id);

        // br show fails (bead doesn't exist)
        let read_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "not found")]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let input = PlannedElsewhereInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Edge case".to_owned(),
            mapped_to_bead_id: "ghost-bead".to_owned(),
        };

        let now = Utc::now();
        let outcome = reconcile_planned_elsewhere(
            &br_mutation,
            &br_read,
            base_dir,
            "test-ms",
            &input,
            true, // post_comment=true, but should be skipped for stale bead
            now,
        )
        .await?;

        assert!(!outcome.bead_verified);
        assert!(!outcome.comment_posted);

        Ok(())
    }

    #[tokio::test]
    async fn persistence_survives_reload() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base_dir = tmp.path();
        let milestone_id = MilestoneId::new("test-ms")?;
        setup_milestone(base_dir, &milestone_id);

        // Record two mappings
        let show_json1 =
            r#"{"id":"bead-a","title":"A","status":"open","priority":2,"bead_type":"task"}"#;
        let show_json2 =
            r#"{"id":"bead-b","title":"B","status":"open","priority":2,"bead_type":"task"}"#;

        let now = Utc::now();

        // First mapping
        let read_runner1 = MockBrRunner::new(vec![MockBrRunner::success(show_json1)]);
        let br_read1 = BrAdapter::with_runner(read_runner1);
        let mutation_runner1 = MockBrRunner::new(vec![]);
        let br_mutation1 =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner1));

        reconcile_planned_elsewhere(
            &br_mutation1,
            &br_read1,
            base_dir,
            "test-ms",
            &PlannedElsewhereInput {
                active_bead_id: "active-1".to_owned(),
                finding_summary: "Finding 1".to_owned(),
                mapped_to_bead_id: "bead-a".to_owned(),
            },
            false,
            now,
        )
        .await?;

        // Second mapping
        let read_runner2 = MockBrRunner::new(vec![MockBrRunner::success(show_json2)]);
        let br_read2 = BrAdapter::with_runner(read_runner2);
        let mutation_runner2 = MockBrRunner::new(vec![]);
        let br_mutation2 =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner2));

        reconcile_planned_elsewhere(
            &br_mutation2,
            &br_read2,
            base_dir,
            "test-ms",
            &PlannedElsewhereInput {
                active_bead_id: "active-1".to_owned(),
                finding_summary: "Finding 2".to_owned(),
                mapped_to_bead_id: "bead-b".to_owned(),
            },
            false,
            now,
        )
        .await?;

        // "Restart": read back from fresh store instance
        let mappings = milestone_service::load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            base_dir,
            &milestone_id,
        )?;
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].mapped_to_bead_id, "bead-a");
        assert_eq!(mappings[1].mapped_to_bead_id, "bead-b");

        // Journal also has both events
        let journal = FsMilestoneJournalStore.read_journal(base_dir, &milestone_id)?;
        let pe_events: Vec<_> = journal
            .iter()
            .filter(|e| e.event_type == MilestoneEventType::PlannedElsewhereMapped)
            .collect();
        assert_eq!(pe_events.len(), 2);

        Ok(())
    }
}

//! Deterministic drain-loop harness for integration-style tests.
//!
//! The harness creates a real temporary `.beads/issues.jsonl` fixture, then
//! drives `drain_bead_queue` through in-memory run, PR, watch, and git stubs.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use serde_json::Value;

use crate::adapters::br_models::{
    BeadDetail, BeadPriority, BeadStatus, BeadType, DepTreeNode, ReadyBead,
};
use crate::adapters::fs::{
    FsJournalStore, FsProjectStore, FsRunSnapshotStore, FsRunSnapshotWriteStore,
};
use crate::contexts::bead_workflow::create_project::{
    create_project_from_bead, BeadProjectBrPort, CreateProjectFromBeadInput, FeatureBranchPort,
};
use crate::contexts::bead_workflow::drain::{
    DrainCreatedProject, DrainError, DrainPort, DrainPrOpenOutcome, DrainPrOpenOutput,
    DrainRunCompletion, DrainRunOutcome,
};
use crate::contexts::bead_workflow::drain_failure::{
    FailureObservation, FailureObservationKind, RecoveryAction,
};
use crate::contexts::bead_workflow::pr_open::{
    open_pr_for_completed_run, DiffStat, Gate, PrOpenRequest, PrOpenStores, PrToolError, PrToolPort,
};
use crate::contexts::bead_workflow::pr_watch::{
    watch_pr, BotBodyReaction, BotBodyReview, BotLineComment, CiState, MergeableState,
    PrMergeOutput, PrStatusSnapshot, PrWatchClock, PrWatchRequest, PrWatchToolPort, WatchOutcome,
    KNOWN_CI_FLAKES,
};
use crate::contexts::bead_workflow::project_prompt::{BeadPromptBrPort, BeadPromptReadError};
use crate::contexts::project_run_record::model::{RunSnapshot, RunStatus};
use crate::contexts::project_run_record::service::RunSnapshotWritePort;
use crate::shared::domain::{FailureClass, FlowPreset};
use crate::test_support::fixtures::{
    BeadGraphFixtureBuilder, BeadGraphIssue, TempWorkspace, TempWorkspaceBuilder,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainHarnessScenario {
    Happy,
    KnownFlakeThenMerge,
    PermanentCiFailure,
    BackendExhaustedSkip,
    InterruptedThenResume,
    BotRejected,
    ForceComplete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainHarnessEvent {
    GitSyncMaster,
    GitSyncMasterPreservingResume {
        bead_id: String,
    },
    GitPrepareBeadMutationBase,
    GitPersistBeadMutations,
    ProjectCreated {
        bead_id: String,
        branch_name: String,
    },
    RunStarted {
        bead_id: String,
    },
    RunResumed {
        bead_id: String,
    },
    RunStopped {
        bead_id: String,
    },
    PrOpened {
        bead_id: String,
        pr_number: u64,
    },
    PrWatched {
        bead_id: String,
        pr_number: u64,
    },
    KnownFlakeRerun {
        bead_id: String,
        pr_number: u64,
    },
    BeadClosed {
        bead_id: String,
        reason: String,
    },
    FollowUpFiled {
        bead_id: String,
    },
}

#[derive(Debug)]
pub struct ScratchDrainHarness {
    workspace: TempWorkspace,
    bead_order: Vec<String>,
    scenarios: HashMap<String, DrainHarnessScenario>,
    statuses: HashMap<String, BeadStatus>,
    active_project: Option<crate::shared::domain::ProjectId>,
    pr_numbers: HashMap<String, u64>,
    pub git: MockDrainGitPort,
    pub runs: MockDrainRunPort,
    pub pr_tool: MockDrainPrToolPort,
    pub pr_watch: MockDrainPrWatchPort,
    pub events: Vec<DrainHarnessEvent>,
}

#[derive(Debug, Default)]
pub struct MockDrainGitPort {
    pub syncs: u32,
    pub preserving_resume_syncs: Vec<String>,
    pub prepared_mutation_bases: u32,
    pub persisted_mutations: u32,
}

#[derive(Debug, Default)]
pub struct MockDrainRunPort {
    pub starts: Vec<String>,
    pub resumes: Vec<String>,
    pub stops: Vec<String>,
}

#[derive(Debug)]
pub struct MockDrainPrToolPort {
    pub opened: Vec<(String, u64)>,
    branch: Mutex<String>,
    next_pr_number: Mutex<u64>,
    pub calls: Mutex<Vec<String>>,
}

impl Default for MockDrainPrToolPort {
    fn default() -> Self {
        Self {
            opened: Vec::new(),
            branch: Mutex::new("feat/drain-harness".to_owned()),
            next_pr_number: Mutex::new(1),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[derive(Debug)]
pub struct MockDrainPrWatchPort {
    pub watched: Vec<(String, u64)>,
    pub known_flake_reruns: Vec<(String, u64)>,
    scenarios: Mutex<HashMap<u64, (String, DrainHarnessScenario)>>,
    poll_counts: Mutex<HashMap<u64, u32>>,
    reruns: Mutex<Vec<(String, u64)>>,
}

impl Default for MockDrainPrWatchPort {
    fn default() -> Self {
        Self {
            watched: Vec::new(),
            known_flake_reruns: Vec::new(),
            scenarios: Mutex::new(HashMap::new()),
            poll_counts: Mutex::new(HashMap::new()),
            reruns: Mutex::new(Vec::new()),
        }
    }
}

#[derive(Debug)]
struct MockDrainBrPort {
    issues_path: PathBuf,
}

#[derive(Debug, Default)]
struct MockDrainFeatureBranchPort {
    created: Mutex<Vec<String>>,
}

#[derive(Debug, Default)]
struct MockDrainPrWatchClock {
    elapsed: Mutex<Duration>,
}

impl ScratchDrainHarness {
    pub fn new(beads: impl IntoIterator<Item = (impl Into<String>, DrainHarnessScenario)>) -> Self {
        let mut builder = BeadGraphFixtureBuilder::new();
        let mut bead_order = Vec::new();
        let mut scenarios = HashMap::new();
        let mut statuses = HashMap::new();

        for (id, scenario) in beads {
            let id = id.into();
            builder = builder.with_issue(
                BeadGraphIssue::open_task(id.clone(), format!("Drain harness {id}"))
                    .with_acceptance_criteria([format!("Harness scenario {scenario:?} passes")]),
            );
            statuses.insert(id.clone(), BeadStatus::Open);
            scenarios.insert(id.clone(), scenario);
            bead_order.push(id);
        }

        let workspace = TempWorkspaceBuilder::new()
            .with_bead_graph(builder)
            .build()
            .expect("build drain harness workspace");

        Self {
            workspace,
            bead_order,
            scenarios,
            statuses,
            active_project: None,
            pr_numbers: HashMap::new(),
            git: MockDrainGitPort::default(),
            runs: MockDrainRunPort::default(),
            pr_tool: MockDrainPrToolPort::default(),
            pr_watch: MockDrainPrWatchPort::default(),
            events: Vec::new(),
        }
    }

    pub fn workspace_path(&self) -> &Path {
        self.workspace.path()
    }

    pub fn issues_path(&self) -> PathBuf {
        self.workspace.beads_root().join("issues.jsonl")
    }

    pub fn closed_bead_ids_on_disk(&self) -> Vec<String> {
        self.issue_values()
            .into_iter()
            .filter(|issue| issue["status"] == "closed")
            .filter_map(|issue| issue["id"].as_str().map(ToOwned::to_owned))
            .collect()
    }

    pub fn follow_up_count_on_disk(&self) -> usize {
        self.issue_values()
            .into_iter()
            .filter(|issue| {
                issue["labels"]
                    .as_array()
                    .is_some_and(|labels| labels.iter().any(|label| label == "drain-follow-up"))
            })
            .count()
    }

    pub fn run_attempts(&self, bead_id: &str) -> usize {
        self.runs
            .starts
            .iter()
            .filter(|started| started.as_str() == bead_id)
            .count()
            + self
                .runs
                .resumes
                .iter()
                .filter(|resumed| resumed.as_str() == bead_id)
                .count()
    }

    fn issue_values(&self) -> Vec<Value> {
        fs::read_to_string(self.issues_path())
            .expect("read harness issues")
            .lines()
            .map(|line| serde_json::from_str(line).expect("parse harness issue"))
            .collect()
    }

    fn write_issue_values(&self, issues: &[Value]) -> Result<(), DrainError> {
        let mut content = String::new();
        for issue in issues {
            content.push_str(&serde_json::to_string(issue).map_err(|error| {
                DrainError::operation("serialize harness issue", error.to_string())
            })?);
            content.push('\n');
        }
        fs::write(self.issues_path(), content)
            .map_err(|error| DrainError::operation("write harness issues", error.to_string()))
    }

    fn scenario(&self, bead_id: &str) -> DrainHarnessScenario {
        self.scenarios
            .get(bead_id)
            .cloned()
            .unwrap_or(DrainHarnessScenario::Happy)
    }

    fn br_port(&self) -> MockDrainBrPort {
        MockDrainBrPort {
            issues_path: self.issues_path(),
        }
    }

    fn record_completed_run(&self) -> Result<DrainRunCompletion, DrainError> {
        let project_id = self.active_project.as_ref().ok_or_else(|| {
            DrainError::operation("record harness run", "no active drain project")
        })?;
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Completed;
        snapshot.completion_rounds = 1;
        snapshot.status_summary = "completed".to_owned();
        FsRunSnapshotWriteStore
            .write_run_snapshot(self.workspace.path(), project_id, &snapshot)
            .map_err(|error| {
                DrainError::operation("write harness run snapshot", error.to_string())
            })?;
        Ok(DrainRunCompletion {
            convergence_pattern: "harness-complete".to_owned(),
        })
    }

    fn close_issue_on_disk(&self, bead_id: &str, reason: &str) -> Result<(), DrainError> {
        let mut issues = self.issue_values();
        let closed_at = Utc
            .with_ymd_and_hms(2026, 4, 30, 12, 0, 0)
            .single()
            .expect("valid harness timestamp")
            .to_rfc3339();
        for issue in &mut issues {
            if issue["id"] == bead_id {
                issue["status"] = Value::String("closed".to_owned());
                issue["closed_at"] = Value::String(closed_at.clone());
                issue["close_reason"] = Value::String(reason.to_owned());
                issue["updated_at"] = Value::String(closed_at.clone());
            }
        }
        self.write_issue_values(&issues)
    }

    fn append_follow_up_on_disk(&self, bead_id: &str) -> Result<(), DrainError> {
        let mut issues = self.issue_values();
        let created_at = Utc
            .with_ymd_and_hms(2026, 4, 30, 12, 1, 0)
            .single()
            .expect("valid harness timestamp")
            .to_rfc3339();
        issues.push(serde_json::json!({
            "id": format!("{bead_id}.follow-up"),
            "title": format!("Drain follow-up for {bead_id}"),
            "description": format!("Filed by drain harness for {bead_id}."),
            "status": "open",
            "priority": 1,
            "issue_type": "bug",
            "created_at": created_at,
            "created_by": "drain-harness",
            "updated_at": created_at,
            "source_repo": ".",
            "compaction_level": 0,
            "original_size": 0,
            "labels": ["drain-follow-up"],
            "dependencies": []
        }));
        self.write_issue_values(&issues)
    }
}

impl MockDrainPrToolPort {
    fn set_branch(&self, branch_name: &str) {
        *self.branch.lock().expect("branch mutex") = branch_name.to_owned();
    }

    fn record(&self, call: &str) {
        self.calls
            .lock()
            .expect("calls mutex")
            .push(call.to_owned());
    }
}

impl PrToolPort for MockDrainPrToolPort {
    async fn run_gate(&self, _repo_root: &Path, gate: Gate) -> Result<(), PrToolError> {
        self.record(gate.name());
        Ok(())
    }

    async fn current_branch(&self, _repo_root: &Path) -> Result<String, PrToolError> {
        self.record("current_branch");
        Ok(self.branch.lock().expect("branch mutex").clone())
    }

    async fn fetch_origin_master(&self, _repo_root: &Path) -> Result<(), PrToolError> {
        self.record("fetch_origin_master");
        Ok(())
    }

    async fn origin_master_is_ancestor_of_head(
        &self,
        _repo_root: &Path,
    ) -> Result<bool, PrToolError> {
        self.record("origin_master_is_ancestor_of_head");
        Ok(true)
    }

    async fn commit_messages_since_origin_master(
        &self,
        _repo_root: &Path,
    ) -> Result<Vec<String>, PrToolError> {
        self.record("commit_messages_since_origin_master");
        Ok(Vec::new())
    }

    async fn diff_stats_since_origin_master(
        &self,
        _repo_root: &Path,
    ) -> Result<Vec<DiffStat>, PrToolError> {
        self.record("diff_stats_since_origin_master");
        Ok(vec![harness_diff_stat()])
    }

    async fn unstaged_real_code_paths(
        &self,
        _repo_root: &Path,
    ) -> Result<Vec<String>, PrToolError> {
        self.record("unstaged_real_code_paths");
        Ok(Vec::new())
    }

    async fn staged_real_code_paths(&self, _repo_root: &Path) -> Result<Vec<String>, PrToolError> {
        self.record("staged_real_code_paths");
        Ok(Vec::new())
    }

    async fn soft_reset_origin_master(&self, _repo_root: &Path) -> Result<(), PrToolError> {
        self.record("soft_reset_origin_master");
        Ok(())
    }

    async fn staged_real_code_diff_stats(
        &self,
        _repo_root: &Path,
    ) -> Result<Vec<DiffStat>, PrToolError> {
        self.record("staged_real_code_diff_stats");
        Ok(vec![harness_diff_stat()])
    }

    async fn staged_commit_paths(&self, _repo_root: &Path) -> Result<Vec<String>, PrToolError> {
        self.record("staged_commit_paths");
        Ok(vec!["src/test_support/drain_harness.rs".to_owned()])
    }

    async fn commit(
        &self,
        _repo_root: &Path,
        _message: &str,
        _paths: &[String],
    ) -> Result<(), PrToolError> {
        self.record("commit");
        Ok(())
    }

    async fn push_branch(&self, _repo_root: &Path, _branch_name: &str) -> Result<(), PrToolError> {
        self.record("push_branch");
        Ok(())
    }

    async fn create_pr(
        &self,
        _repo_root: &Path,
        _branch_name: &str,
        _title: &str,
        _body: &str,
    ) -> Result<String, PrToolError> {
        self.record("create_pr");
        let mut next_pr_number = self.next_pr_number.lock().expect("next PR mutex");
        let pr_number = *next_pr_number;
        *next_pr_number += 1;
        Ok(format!(
            "https://example.test/drain-harness/pull/{pr_number}"
        ))
    }
}

impl MockDrainPrWatchPort {
    fn configure_pr(&self, pr_number: u64, bead_id: &str, scenario: DrainHarnessScenario) {
        self.scenarios
            .lock()
            .expect("watch scenario mutex")
            .insert(pr_number, (bead_id.to_owned(), scenario));
    }

    fn recorded_reruns(&self) -> Vec<(String, u64)> {
        self.reruns.lock().expect("rerun mutex").clone()
    }

    fn scenario_for_pr(&self, pr_number: u64) -> (String, DrainHarnessScenario) {
        self.scenarios
            .lock()
            .expect("watch scenario mutex")
            .get(&pr_number)
            .cloned()
            .unwrap_or_else(|| ("unknown".to_owned(), DrainHarnessScenario::Happy))
    }
}

impl PrWatchToolPort for MockDrainPrWatchPort {
    async fn repo_slug(&self, _repo_root: &Path) -> Result<String, PrToolError> {
        Ok("example/ralph-burning".to_owned())
    }

    async fn pr_status(
        &self,
        _repo_root: &Path,
        _repo_slug: &str,
        pr_number: u64,
    ) -> Result<PrStatusSnapshot, PrToolError> {
        let (_, scenario) = self.scenario_for_pr(pr_number);
        let poll_count = {
            let mut poll_counts = self.poll_counts.lock().expect("poll mutex");
            let count = poll_counts.entry(pr_number).or_insert(0);
            let current = *count;
            *count += 1;
            current
        };
        let head_sha = format!("sha-pr-{pr_number}");
        Ok(match scenario {
            DrainHarnessScenario::KnownFlakeThenMerge if poll_count == 0 => status_with_ci(
                pr_number,
                head_sha,
                CiState::Failed {
                    failing: vec![KNOWN_CI_FLAKES[0].to_owned()],
                    failed_run_ids: vec![pr_number * 10],
                },
            ),
            DrainHarnessScenario::PermanentCiFailure => status_with_ci(
                pr_number,
                head_sha,
                CiState::Failed {
                    failing: vec!["tests::permanent_failure".to_owned()],
                    failed_run_ids: vec![pr_number * 10],
                },
            ),
            _ => status_with_ci(pr_number, head_sha, CiState::Success),
        })
    }

    async fn codex_bot_reaction(
        &self,
        _repo_root: &Path,
        _repo_slug: &str,
        _pr_number: u64,
        head_sha: Option<&str>,
        _head_review_watermark_at: Option<chrono::DateTime<Utc>>,
    ) -> Result<BotBodyReview, PrToolError> {
        Ok(BotBodyReview {
            reaction: BotBodyReaction::Approved,
            created_at: None,
            approved_head_sha: head_sha.map(ToOwned::to_owned),
        })
    }

    async fn codex_bot_line_comments_since_latest_push(
        &self,
        _repo_root: &Path,
        _repo_slug: &str,
        _pr_number: u64,
        _head_sha: Option<&str>,
        _latest_push_at: Option<chrono::DateTime<Utc>>,
    ) -> Result<Vec<BotLineComment>, PrToolError> {
        Ok(Vec::new())
    }

    async fn failed_run_logs(
        &self,
        _repo_root: &Path,
        _run_ids: &[u64],
    ) -> Result<Vec<String>, PrToolError> {
        Ok(Vec::new())
    }

    async fn rerun_failed_runs(
        &self,
        _repo_root: &Path,
        pr_number: u64,
        _run_ids: &[u64],
    ) -> Result<(), PrToolError> {
        let (bead_id, _) = self.scenario_for_pr(pr_number);
        self.reruns
            .lock()
            .expect("rerun mutex")
            .push((bead_id, pr_number));
        Ok(())
    }

    async fn merge_pr(
        &self,
        _repo_root: &Path,
        pr_number: u64,
        head_sha: &str,
    ) -> Result<PrMergeOutput, PrToolError> {
        Ok(PrMergeOutput {
            sha: head_sha.to_owned(),
            pr_url: format!("https://example.test/drain-harness/pull/{pr_number}"),
        })
    }
}

impl PrWatchClock for MockDrainPrWatchClock {
    fn elapsed(&self) -> Duration {
        *self.elapsed.lock().expect("clock mutex")
    }

    async fn sleep(&self, duration: Duration) {
        *self.elapsed.lock().expect("clock mutex") += duration;
    }
}

impl BeadPromptBrPort for MockDrainBrPort {
    async fn bead_show(&self, bead_id: &str) -> Result<BeadDetail, BeadPromptReadError> {
        self.issue_values()
            .into_iter()
            .find(|issue| issue["id"] == bead_id)
            .ok_or_else(|| BeadPromptReadError::NotFound {
                bead_id: bead_id.to_owned(),
            })
            .and_then(|issue| {
                serde_json::from_value(issue).map_err(|_| BeadPromptReadError::NotFound {
                    bead_id: bead_id.to_owned(),
                })
            })
    }

    async fn bead_dep_tree(&self, _bead_id: &str) -> Result<Vec<DepTreeNode>, BeadPromptReadError> {
        Ok(Vec::new())
    }
}

impl BeadProjectBrPort for MockDrainBrPort {
    async fn mark_bead_in_progress(&self, bead_id: &str) -> Result<(), String> {
        self.update_issue_status(bead_id, BeadStatus::InProgress)
    }
}

impl MockDrainBrPort {
    fn issue_values(&self) -> Vec<Value> {
        fs::read_to_string(&self.issues_path)
            .expect("read harness issues")
            .lines()
            .map(|line| serde_json::from_str(line).expect("parse harness issue"))
            .collect()
    }

    fn update_issue_status(&self, bead_id: &str, status: BeadStatus) -> Result<(), String> {
        let mut issues = self.issue_values();
        let updated_at = Utc
            .with_ymd_and_hms(2026, 4, 30, 12, 0, 0)
            .single()
            .expect("valid harness timestamp")
            .to_rfc3339();
        for issue in &mut issues {
            if issue["id"] == bead_id {
                issue["status"] = Value::String(status.to_string());
                issue["updated_at"] = Value::String(updated_at.clone());
            }
        }
        let mut content = String::new();
        for issue in issues {
            content.push_str(&serde_json::to_string(&issue).map_err(|error| error.to_string())?);
            content.push('\n');
        }
        fs::write(&self.issues_path, content).map_err(|error| error.to_string())
    }
}

impl FeatureBranchPort for MockDrainFeatureBranchPort {
    fn create_branch(&self, _base_dir: &Path, branch_name: &str) -> Result<(), String> {
        self.created
            .lock()
            .expect("branch create mutex")
            .push(branch_name.to_owned());
        Ok(())
    }
}

fn harness_diff_stat() -> DiffStat {
    DiffStat {
        path: "src/test_support/drain_harness.rs".to_owned(),
        additions: Some(12),
        deletions: Some(0),
    }
}

fn status_with_ci(pr_number: u64, head_sha: String, ci: CiState) -> PrStatusSnapshot {
    PrStatusSnapshot {
        ci,
        mergeable: MergeableState::Mergeable,
        head_sha: Some(head_sha),
        head_review_watermark_at: None,
        latest_push_at: None,
        pr_url: format!("https://example.test/drain-harness/pull/{pr_number}"),
    }
}

fn parse_pr_number(pr_url: &str) -> Option<u64> {
    pr_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .and_then(|value| value.parse::<u64>().ok())
}

impl DrainPort for ScratchDrainHarness {
    async fn sync_master_and_sanity_check(&mut self) -> Result<(), DrainError> {
        self.git.syncs += 1;
        self.events.push(DrainHarnessEvent::GitSyncMaster);
        Ok(())
    }

    async fn sync_master_and_sanity_check_preserving_resume(
        &mut self,
        bead_id: &str,
        _created_project: &DrainCreatedProject,
    ) -> Result<(), DrainError> {
        self.git.preserving_resume_syncs.push(bead_id.to_owned());
        self.events
            .push(DrainHarnessEvent::GitSyncMasterPreservingResume {
                bead_id: bead_id.to_owned(),
            });
        Ok(())
    }

    async fn ready_beads(&mut self) -> Result<Vec<ReadyBead>, DrainError> {
        Ok(self
            .bead_order
            .iter()
            .filter(|id| self.statuses.get(*id) == Some(&BeadStatus::Open))
            .map(|id| ReadyBead {
                id: id.clone(),
                title: format!("Drain harness {id}"),
                priority: BeadPriority::new(2),
                bead_type: BeadType::Task,
                labels: vec!["drain-harness".to_owned()],
            })
            .collect())
    }

    async fn bead_status(&mut self, bead_id: &str) -> Result<BeadStatus, DrainError> {
        Ok(self
            .statuses
            .get(bead_id)
            .cloned()
            .unwrap_or(BeadStatus::Open))
    }

    async fn create_project_from_bead(
        &mut self,
        bead_id: &str,
    ) -> Result<DrainCreatedProject, DrainError> {
        let branch_port = MockDrainFeatureBranchPort::default();
        let output = create_project_from_bead(
            &FsProjectStore,
            &FsJournalStore,
            &self.br_port(),
            &branch_port,
            self.workspace.path(),
            CreateProjectFromBeadInput {
                bead_id: bead_id.to_owned(),
                flow: FlowPreset::IterativeMinimal,
                branch: Some(String::new()),
                created_at: Utc
                    .with_ymd_and_hms(2026, 4, 30, 12, 0, 0)
                    .single()
                    .expect("valid harness timestamp"),
            },
        )
        .await
        .map_err(|error| DrainError::operation("create project from bead", error.to_string()))?;
        self.active_project = Some(output.project.id);
        self.statuses
            .insert(bead_id.to_owned(), BeadStatus::InProgress);
        if let Some(branch_name) = &output.branch_name {
            self.pr_tool.set_branch(branch_name);
        }
        let branch_name = output.branch_name.clone();
        self.events.push(DrainHarnessEvent::ProjectCreated {
            bead_id: bead_id.to_owned(),
            branch_name: branch_name.clone().unwrap_or_default(),
        });
        Ok(DrainCreatedProject { branch_name })
    }

    async fn restore_resume_context(
        &mut self,
        _bead_id: &str,
        _created_project: &DrainCreatedProject,
    ) -> Result<(), DrainError> {
        Ok(())
    }

    async fn start_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError> {
        self.runs.starts.push(bead_id.to_owned());
        self.events.push(DrainHarnessEvent::RunStarted {
            bead_id: bead_id.to_owned(),
        });
        Ok(match self.scenario(bead_id) {
            DrainHarnessScenario::InterruptedThenResume => DrainRunOutcome::Failed(
                FailureObservation::new(FailureObservationKind::RunInterrupted {
                    run_id: format!("run-{bead_id}"),
                }),
            ),
            DrainHarnessScenario::BackendExhaustedSkip => DrainRunOutcome::Failed(
                FailureObservation::new(FailureObservationKind::BackendFailure {
                    bead_id: bead_id.to_owned(),
                    failure_class: FailureClass::BackendExhausted,
                }),
            ),
            DrainHarnessScenario::BotRejected => DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::BotRejectedReaction,
            )),
            DrainHarnessScenario::ForceComplete => DrainRunOutcome::Failed({
                self.record_completed_run()?;
                FailureObservation::new(FailureObservationKind::AmendmentOscillationLimitReached {
                    completion_rounds: 3,
                    max_completion_rounds: 3,
                })
            }),
            DrainHarnessScenario::Happy
            | DrainHarnessScenario::KnownFlakeThenMerge
            | DrainHarnessScenario::PermanentCiFailure => {
                DrainRunOutcome::Completed(self.record_completed_run()?)
            }
        })
    }

    async fn resume_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError> {
        self.runs.resumes.push(bead_id.to_owned());
        self.events.push(DrainHarnessEvent::RunResumed {
            bead_id: bead_id.to_owned(),
        });
        let mut completion = self.record_completed_run()?;
        completion.convergence_pattern = "harness-resumed".to_owned();
        Ok(DrainRunOutcome::Completed(completion))
    }

    async fn stop_run(&mut self, bead_id: &str) -> Result<(), DrainError> {
        self.runs.stops.push(bead_id.to_owned());
        self.events.push(DrainHarnessEvent::RunStopped {
            bead_id: bead_id.to_owned(),
        });
        Ok(())
    }

    async fn open_pr(&mut self, bead_id: &str) -> Result<DrainPrOpenOutcome, DrainError> {
        let project_id = self
            .active_project
            .as_ref()
            .ok_or_else(|| DrainError::operation("open PR", "no active drain project"))?;
        let output = open_pr_for_completed_run(
            PrOpenRequest {
                base_dir: self.workspace.path(),
                project_id,
                bead_id_override: Some(bead_id),
                skip_gates: false,
            },
            PrOpenStores {
                project_store: &FsProjectStore,
                run_store: &FsRunSnapshotStore,
                journal_store: &FsJournalStore,
            },
            &self.br_port(),
            &self.pr_tool,
        )
        .await
        .map_err(|error| DrainError::operation("open PR", error.to_string()))?;
        let pr_number = parse_pr_number(&output.pr_url)
            .ok_or_else(|| DrainError::operation("parse PR number", output.pr_url.clone()))?;
        self.pr_numbers.insert(bead_id.to_owned(), pr_number);
        self.pr_watch
            .configure_pr(pr_number, bead_id, self.scenario(bead_id));
        self.pr_tool.opened.push((bead_id.to_owned(), pr_number));
        self.events.push(DrainHarnessEvent::PrOpened {
            bead_id: bead_id.to_owned(),
            pr_number,
        });
        Ok(DrainPrOpenOutcome::Opened(DrainPrOpenOutput {
            pr_number,
            pr_url: output.pr_url,
        }))
    }

    async fn watch_pr(
        &mut self,
        bead_id: &str,
        pr_number: u64,
    ) -> Result<WatchOutcome, DrainError> {
        self.pr_watch.watched.push((bead_id.to_owned(), pr_number));
        self.events.push(DrainHarnessEvent::PrWatched {
            bead_id: bead_id.to_owned(),
            pr_number,
        });
        let outcome = watch_pr(
            PrWatchRequest {
                base_dir: self.workspace.path(),
                pr_number,
                max_wait: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
            },
            &self.pr_watch,
            &MockDrainPrWatchClock::default(),
        )
        .await
        .map_err(|error| DrainError::operation("watch PR", error.to_string()))?;
        self.pr_watch.known_flake_reruns = self.pr_watch.recorded_reruns();
        if self
            .pr_watch
            .known_flake_reruns
            .iter()
            .any(|(rerun_bead_id, rerun_pr)| rerun_bead_id == bead_id && *rerun_pr == pr_number)
        {
            self.events.push(DrainHarnessEvent::KnownFlakeRerun {
                bead_id: bead_id.to_owned(),
                pr_number,
            });
        }
        Ok(outcome)
    }

    async fn close_bead(&mut self, bead_id: &str, reason: &str) -> Result<(), DrainError> {
        self.statuses.insert(bead_id.to_owned(), BeadStatus::Closed);
        self.close_issue_on_disk(bead_id, reason)?;
        self.events.push(DrainHarnessEvent::BeadClosed {
            bead_id: bead_id.to_owned(),
            reason: reason.to_owned(),
        });
        Ok(())
    }

    async fn file_follow_up_bead(
        &mut self,
        bead_id: &str,
        _action: &RecoveryAction,
        _observation: &FailureObservation,
    ) -> Result<(), DrainError> {
        self.append_follow_up_on_disk(bead_id)?;
        self.events.push(DrainHarnessEvent::FollowUpFiled {
            bead_id: bead_id.to_owned(),
        });
        Ok(())
    }

    async fn prepare_bead_mutation_base(&mut self) -> Result<(), DrainError> {
        self.git.prepared_mutation_bases += 1;
        self.events
            .push(DrainHarnessEvent::GitPrepareBeadMutationBase);
        Ok(())
    }

    async fn persist_bead_mutations(&mut self) -> Result<(), DrainError> {
        self.git.persisted_mutations += 1;
        self.events.push(DrainHarnessEvent::GitPersistBeadMutations);
        Ok(())
    }
}

pub fn known_flake_observation() -> FailureObservation {
    FailureObservation::new(FailureObservationKind::VerificationFailure {
        source: crate::contexts::bead_workflow::drain_failure::VerificationFailureSource::Ci,
        failing: vec![KNOWN_CI_FLAKES[0].to_owned()],
        reruns_attempted_for_pr: 0,
    })
}

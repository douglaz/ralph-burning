---
artifact: final-review-proposals
loop: 7
project: cursor-history-integrity
backend: codex
role: final_reviewer
created_at: 2026-03-14T01:40:40Z
---

Reviewing the diff against `85ccbdb59e406c10e305f26ebfac72d3de3ad731` from `ralph-burning-rewrite`, then I’ll read the touched implementation files and tests end-to-end before checking behavior against the master prompt.
The diff is larger than the two integrity fixes, so I’m separating the required scope from adjacent changes and reading the implementation and test files in full before I judge any regressions.
I’ve isolated the main implementation surfaces: cursor transitions and history IDs, plus the daemon/defaults work that landed in the same range. I’m reading those files in full and then I’ll run targeted checks where the control flow looks risky.
The workflow engine read is complete. I’m moving through requirements drafting and daemon paths now, since the diff also changes persisted run state and task failure handling there.
to=multi_tool_use.parallel  老时时彩  大发快三是国家ափոխ 大奖彩票站 to=multi_tool_use.parallel  天天中彩票如何json
{"tool_uses":[{"recipient_name":"functions.exec_command","parameters":{"cmd":"nl -ba src/contexts/automation_runtime/daemon_loop.rs | sed -n '881,1100p'","workdir":"/root/new-ralph-burning/ralph-burning-rewrite","yield_time_ms":1000,"max_output_tokens":12000}},{"recipient_name":"functions.exec_command","parameters":{"cmd":"nl -ba src/contexts/automation_runtime/daemon_loop.rs | sed -n '1101,1320p'","workdir":"/root/new-ralph-burning/ralph-burning-rewrite","yield_time_ms":1000,"max_output_tokens":12000}}]}_RGCTX to=functions.exec_command.commentary  鸿丰
Chunk ID: e035c4
Wall time: 0.0000 seconds
Process exited with code 0
Original token count: 2552
Output:
   881	                        "requirements_linking_failed",
   882	                        &e.to_string(),
   883	                    );
   884	                    return Err(e);
   885	                }
   886	            }
   887	            Ok(())
   888	        }
   889	    }
   890	
   891	    fn ensure_project(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()> {
   892	        if self
   893	            .project_store
   894	            .project_exists(base_dir, &task.project_id)?
   895	        {
   896	            return Ok(());
   897	        }
   898	
   899	        let prompt = task.prompt.as_ref().ok_or_else(|| AppError::RunStartFailed {
   900	            reason: format!("task '{}' is missing a prompt", task.task_id),
   901	        })?;
   902	        let flow = task.resolved_flow.unwrap_or(FlowPreset::Standard);
   903	        let project_name = task
   904	            .project_name
   905	            .clone()
   906	            .unwrap_or_else(|| task.project_id.to_string());
   907	
   908	        create_project(
   909	            base_dir,
   910	            CreateProjectInput {
   911	                project_id: task.project_id.clone(),
   912	                project_name: project_name.clone(),
   913	                flow,
   914	                prompt: prompt.to_owned(),
   915	            },
   916	        )?;
   917	
   918	        let metadata = json!({
   919	            "task_id": task.task_id,
   920	            "project_id": task.project_id.to_string(),
   921	            "project_name": project_name,
   922	            "flow": flow.as_str(),
   923	        });
   924	        DaemonTaskService::append_journal_event(
   925	            self.store,
   926	            base_dir,
   927	            super::model::DaemonJournalEventType::ProjectCreated,
   928	            metadata,
   929	        )?;
   930	        Ok(())
   931	    }
   932	
   933	    #[allow(clippy::too_many_arguments)]
   934	    async fn drive_dispatch(
   935	        &self,
   936	        base_dir: &Path,
   937	        task: &DaemonTask,
   938	        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
   939	        effective_config: &EffectiveConfig,
   940	        config: &DaemonLoopConfig,
   941	        shutdown: CancellationToken,
   942	        task_cancel: CancellationToken,
   943	    ) -> AppResult<()> {
   944	        let project_id = task.project_id.clone();
   945	        let dispatch = if self.run_snapshot_read.read_run_snapshot(base_dir, &project_id)?.status
   946	            == RunStatus::NotStarted
   947	        {
   948	            engine::execute_run_with_retry(
   949	                self.agent_service,
   950	                self.run_snapshot_read,
   951	                self.run_snapshot_write,
   952	                self.journal_store,
   953	                self.artifact_write,
   954	                self.log_write,
   955	                self.amendment_queue,
   956	                &lease.worktree_path,
   957	                &project_id,
   958	                task.resolved_flow.unwrap_or(FlowPreset::Standard),
   959	                effective_config,
   960	                &RetryPolicy::default_policy(),
   961	                task_cancel.clone(),
   962	            )
   963	        } else {
   964	            engine::resume_run_with_retry(
   965	                self.agent_service,
   966	                self.run_snapshot_read,
   967	                self.run_snapshot_write,
   968	                self.journal_store,
   969	                self.artifact_store,
   970	                self.artifact_write,
   971	                self.log_write,
   972	                self.amendment_queue,
   973	                &lease.worktree_path,
   974	                &project_id,
   975	                task.resolved_flow.unwrap_or(FlowPreset::Standard),
   976	                effective_config,
   977	                &RetryPolicy::default_policy(),
   978	                task_cancel.clone(),
   979	            )
   980	        };
   981	
   982	        tokio::pin!(dispatch);
   983	
   984	        loop {
   985	            tokio::select! {
   986	                outcome = &mut dispatch => return outcome,
   987	                _ = shutdown.cancelled() => {
   988	                    task_cancel.cancel();
   989	                    let _ = DaemonTaskService::append_journal_event(
   990	                        self.store,
   991	                        base_dir,
   992	                        super::model::DaemonJournalEventType::ShutdownRequested,
   993	                        json!({
   994	                            "task_id": task.task_id,
   995	                        }),
   996	                    );
   997	                }
   998	                _ = tokio::time::sleep(config.heartbeat_interval) => {
   999	                    LeaseService::heartbeat(self.store, base_dir, &lease.lease_id, config.lease_ttl.as_secs())?;
  1000	                }
  1001	            }
  1002	        }
  1003	    }
  1004	
  1005	    fn fail_claimed_task(
  1006	        &self,
  1007	        base_dir: &Path,
  1008	        repo_root: &Path,
  1009	        task_id: &str,
  1010	        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
  1011	        failure_class: &str,
  1012	        message: &str,
  1013	    ) -> AppResult<()> {
  1014	        DaemonTaskService::mark_failed(self.store, base_dir, task_id, failure_class, message)?;
  1015	        self.release_task_lease(base_dir, repo_root, task_id, lease)?;
  1016	        Ok(())
  1017	    }
  1018	
  1019	    fn handle_post_claim_failure(
  1020	        &self,
  1021	        base_dir: &Path,
  1022	        repo_root: &Path,
  1023	        task: &DaemonTask,
  1024	        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
  1025	        error: &AppError,
  1026	    ) -> AppResult<()> {
  1027	        let failure_class = error
  1028	            .failure_class()
  1029	            .map(|class| class.as_str().to_owned())
  1030	            .unwrap_or_else(|| "daemon_dispatch_failed".to_owned());
  1031	        DaemonTaskService::mark_failed(
  1032	            self.store,
  1033	            base_dir,
  1034	            &task.task_id,
  1035	            &failure_class,
  1036	            &error.to_string(),
  1037	        )?;
  1038	        self.release_task_lease(base_dir, repo_root, &task.task_id, lease)?;
  1039	        Ok(())
  1040	    }
  1041	
  1042	    fn cleanup_active_leases(&self, base_dir: &Path) -> AppResult<()> {
  1043	        let tasks = self.store.list_tasks(base_dir)?;
  1044	        let mut cleanup_failures = 0usize;
  1045	
  1046	        for task in tasks {
  1047	            let Some(lease_id) = task.lease_id.clone() else {
  1048	                continue;
  1049	            };
  1050	            let Some(lease) = self.store.read_lease(base_dir, &lease_id)? else {
  1051	                continue;
  1052	            };
  1053	            let repo_root = lease.repo_root.clone();
  1054	
  1055	            match self.release_task_lease(base_dir, &repo_root, &task.task_id, &lease) {
  1056	                Ok(()) => {}
  1057	                Err(AppError::LeaseCleanupPartialFailure { .. }) => cleanup_failures += 1,
  1058	                Err(error) => return Err(error),
  1059	            }
  1060	        }
  1061	
  1062	        if cleanup_failures > 0 {
  1063	            return Err(AppError::ReconcileCleanupFailed {
  1064	                failed_count: cleanup_failures,
  1065	            });
  1066	        }
  1067	
  1068	        Ok(())
  1069	    }
  1070	
  1071	    fn check_for_shutdown_file(base_dir: &Path) -> bool {
  1072	        FileSystem::path_exists(&base_dir.join(".ralph-burning/daemon/shutdown"))
  1073	    }
  1074	
  1075	    async fn wait_for_shutdown(&self, base_dir: &Path) -> AppResult<()> {
  1076	        while !Self::check_for_shutdown_file(base_dir) {
  1077	            tokio::time::sleep(Duration::from_millis(200)).await;
  1078	        }
  1079	        Ok(())
  1080	    }
  1081	
  1082	    pub async fn run_with_shutdown_file(
  1083	        &self,
  1084	        base_dir: &Path,
  1085	        config: &DaemonLoopConfig,
  1086	    ) -> AppResult<()> {
  1087	        let shutdown = CancellationToken::new();
  1088	        tokio::select! {
  1089	            result = self.run(base_dir, config) => result,
  1090	            result = self.wait_for_shutdown(base_dir) => {
  1091	                shutdown.cancel();
  1092	                result
  1093	            }
  1094	        }
  1095	    }
  1096	
  1097	    fn release_task_lease(
  1098	        &self,
  1099	        base_dir: &Path,
  1100	        repo_root: &Path,
There’s a larger daemon refactor in this range. I’m checking the exact control flow and recovery paths there now because that’s where subtle regressions are most likely to hide.
I have enough context to start validating the riskier paths directly. I’m running targeted checks now against the workflow/requirements/daemon changes, then I’ll decide whether the issues are real or only theoretical.
The toolchain isn’t on the base `PATH` here, so I’m switching to the project’s `nix develop` shell for the verification commands.
I haven’t found a concrete failing path yet, so I’m doing a higher-confidence pass with the project’s real build/test commands before I decide whether this is a no-amendments review.
The full suite is still running cleanly so far. I’m waiting on the tail of the longer integration tests before I close the review.
# Final Review: NO AMENDMENTS

## Summary
I found no correctness, safety, or robustness regressions in the reviewed changes.

The core integrity fixes are implemented coherently: [`src/shared/domain.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/domain.rs) now makes `StageCursor` transitions fallible with checked arithmetic, [`src/shared/error.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/shared/error.rs) introduces a dedicated overflow error, and [`src/contexts/workflow_composition/engine.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/workflow_composition/engine.rs) propagates those failures through both normal execution and resume/remediation paths. The workflow history ID format now includes `completion_round`, preserving distinct payload/artifact files across completion-round restarts and rollback branches. The requirements flow in [`src/contexts/requirements_drafting/service.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/requirements_drafting/service.rs) updates question/draft/review IDs to be round-aware while keeping seed IDs unchanged. The daemon/defaults work in [`src/contexts/automation_runtime/daemon_loop.rs`](/root/new-ralph-burning/ralph-burning-rewrite/src/contexts/automation_runtime/daemon_loop.rs) is also internally consistent and matches the CLI wiring.

Verification was strong: `nix develop -c cargo build` passed, `nix develop -c cargo test` passed in full, and `nix develop -c ralph-burning conformance run --filter workspace-init-fresh` passed with the dev-shell binary on `PATH`.

#![allow(
    clippy::await_holding_lock,
    clippy::io_other_error,
    clippy::unnecessary_map_or,
    clippy::needless_borrows_for_generic_args
)]

#[path = "unit/domain_test.rs"]
mod domain_test;

#[path = "unit/flow_preset_test.rs"]
mod flow_preset_test;

#[path = "unit/flow_semantics_test.rs"]
mod flow_semantics_test;

#[path = "unit/workspace_test.rs"]
mod workspace_test;

#[path = "unit/config_test.rs"]
mod config_test;

#[path = "unit/backend_resolver_test.rs"]
mod backend_resolver_test;

#[path = "unit/active_project_test.rs"]
mod active_project_test;

#[path = "unit/stage_contract_test.rs"]
mod stage_contract_test;

#[path = "unit/renderer_test.rs"]
mod renderer_test;

#[path = "unit/project_run_record_test.rs"]
mod project_run_record_test;

#[path = "unit/journal_test.rs"]
mod journal_test;

#[path = "unit/query_test.rs"]
mod query_test;

#[path = "unit/run_queries_test.rs"]
mod run_queries_test;

#[path = "unit/rollback_test.rs"]
mod rollback_test;

#[path = "unit/adapter_contract_test.rs"]
mod adapter_contract_test;

#[path = "unit/br_adapter_test.rs"]
mod br_adapter_test;

#[path = "unit/bv_adapter_test.rs"]
mod bv_adapter_test;

#[cfg(feature = "test-stub")]
#[path = "unit/agent_execution_test.rs"]
mod agent_execution_test;

#[cfg(feature = "test-stub")]
#[path = "unit/stub_backend_test.rs"]
mod stub_backend_test;

#[path = "unit/process_backend_test.rs"]
mod process_backend_test;

#[path = "unit/env_test_support.rs"]
mod env_test_support;

#[path = "unit/tmux_adapter_test.rs"]
mod tmux_adapter_test;

#[path = "unit/prompt_builder_test.rs"]
mod prompt_builder_test;

#[cfg(feature = "test-stub")]
#[path = "unit/workflow_engine_test.rs"]
mod workflow_engine_test;

#[path = "unit/retry_policy_test.rs"]
mod retry_policy_test;

#[path = "unit/requirements_drafting_test.rs"]
mod requirements_drafting_test;

#[path = "unit/automation_runtime_test.rs"]
mod automation_runtime_test;

#[cfg(feature = "test-stub")]
#[path = "unit/conformance_spec_test.rs"]
mod conformance_spec_test;

#[path = "unit/backend_policy_test.rs"]
mod backend_policy_test;

#[path = "unit/workflow_panels_test.rs"]
mod workflow_panels_test;

#[path = "unit/validation_runner_test.rs"]
mod validation_runner_test;

#[path = "unit/checkpoint_test.rs"]
mod checkpoint_test;

#[path = "unit/backend_diagnostics_test.rs"]
mod backend_diagnostics_test;

#[path = "unit/execution_config_test.rs"]
mod execution_config_test;

#[path = "unit/template_catalog_test.rs"]
mod template_catalog_test;

#[path = "unit/controller_runtime_test.rs"]
mod controller_runtime_test;

#[path = "unit/milestone_store_integration_test.rs"]
mod milestone_store_integration_test;

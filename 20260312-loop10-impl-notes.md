# Implementation Notes

## Decisions Made

- **Generalized `agent_execution` via `InvocationContract` enum** — introduced `InvocationContract { Stage(StageContract), Requirements { label: String } }` rather than trait objects (which would break `Clone`) or separate invoke paths (which would duplicate timeout/cancellation/session logic). The `InvocationRequest.stage_contract` field was renamed to `contract: InvocationContract`, and `CapabilityCheck` now stores a `contract_id: String` instead of tying to `StageId`. This allows requirements drafting to reuse the full agent execution pipeline — capability checks, timeout, cancellation, session management, raw output persistence — with zero workflow-specific coupling.

- **Error variants use `contract_id: String` instead of `stage_id: StageId`** — changed `InvocationFailed`, `InvocationTimeout`, `InvocationCancelled`, and `CapabilityMismatch` to use a generic string contract identifier. All existing tests use `matches!(error, AppError::Variant { .. })` pattern which is field-name-insensitive, so no test assertions broke.

- **Requirements contracts are separate from workflow contracts** — `RequirementsContract` follows the same schema → domain → render pipeline as `StageContract` but is a distinct type with its own `RequirementsStageId` enum (QuestionSet, RequirementsDraft, RequirementsReview, ProjectSeed). The `ContractError` type reuses `StageId::Planning` as a placeholder in the `stage_id` field since `ContractError` is currently tied to `StageId`. This is a known compromise; a future loop can generalize `ContractError` to use a string identifier.

- **Stub backend returns deterministic canned payloads for requirements contracts** — `canned_requirements_payload()` dispatches on the contract label substring (e.g., `"question_set"`, `"requirements_draft"`). The default question set returns empty questions (`{"questions": []}`), causing draft mode to fall through directly to completion. `with_label_payload()` / `with_label_payload_sequence()` methods enable test-specific overrides.

- **`FsRequirementsStore` creates `sessions.json` in run directory** — the agent execution service expects `sessions.json` in the `project_root`, which for requirements invocations is the run root. The store's `create_run_dir` seeds an empty sessions file to satisfy this invariant.

- **Seed rollback semantics** — `write_seed_pair` writes `project.json` first, then `prompt.md`. If the prompt write fails, the project file is removed. If the higher-level service detects seed write failure, it calls `remove_seed_pair` to clean up both files, then transitions the run to failed.

## Files Modified

| File | Change |
|------|--------|
| `src/contexts/agent_execution/model.rs` | Added `InvocationContract` enum; changed `InvocationRequest.stage_contract` → `.contract`; updated `CapabilityCheck` to use `contract_id: String` |
| `src/contexts/agent_execution/service.rs` | Changed `check_capability` signature from `&StageContract` to `&InvocationContract`; stage contract validation conditional on `contract.stage_contract()` |
| `src/contexts/agent_execution/mod.rs` | Added `InvocationContract` to public exports |
| `src/shared/error.rs` | Changed 4 error variants from `stage_id: StageId` to `contract_id: String`; added `InvalidRequirementsState`, `AnswerValidationFailed`, `SeedPersistenceFailed` |
| `src/contexts/workflow_composition/engine.rs` | Wrapped `StageContract` in `InvocationContract::Stage(...)` at invocation and preflight sites; updated error field names |
| `src/adapters/stub_backend.rs` | Added label-based payload overrides; `check_capability` accepts `&InvocationContract`; added `canned_requirements_payload()` |
| `src/adapters/fs.rs` | Added `FsRequirementsStore` implementing `RequirementsStorePort`; creates run directories, sessions.json, handles seed pair write/rollback |
| `src/cli/requirements.rs` | Wired CLI subcommands (draft, quick, show, answer) to `RequirementsService` |

## Files Created

| File | Purpose |
|------|---------|
| `src/contexts/requirements_drafting/model.rs` | `RequirementsRun`, `RequirementsMode`, `RequirementsStatus`, `RequirementsStageId`, `RequirementsJournalEvent`, `RequirementsReviewOutcome`, structured payloads (QuestionSet, Draft, Review, Seed), `PersistedAnswers` |
| `src/contexts/requirements_drafting/contracts.rs` | `RequirementsContract` with `evaluate()` pipeline: schema → domain → render. Domain validation: duplicate question IDs, empty required fields, flow preset check, findings required for non-approval |
| `src/contexts/requirements_drafting/renderers.rs` | Deterministic Markdown renderers for all four requirements payloads |
| `src/contexts/requirements_drafting/service.rs` | `RequirementsService` orchestrating draft/quick/show/answer commands; `RequirementsStorePort` trait; answer TOML parsing/validation; failure invariants; seed rollback |
| `src/contexts/requirements_drafting/mod.rs` | Module declarations and public exports |
| `tests/unit/requirements_drafting_test.rs` | 24 tests: model (5), contracts (10), renderers (5), service integration (4) |
| `tests/conformance/features/requirements_drafting.feature` | 10 Gherkin scenarios covering draft, quick, answer, show, review rejection, seed rollback, contract validation |

## Spec Deviations

- **`ContractError` placeholder `stage_id`** — requirements contracts use `StageId::Planning` as a placeholder in `ContractError` variants because `ContractError` is typed to `StageId`. This does not affect behavior since requirements contract errors are caught and re-mapped by the requirements service before reaching the workflow engine.

- **Answer validation is permissive** — the `parse_and_validate_answers` function validates TOML syntax and builds `PersistedAnswers`, but does not enforce required-question coverage (would need to read and deserialize the question set payload). This is a simplification for this loop; the spec's required-question enforcement is deferred.

- **Default stub returns empty question set** — the default canned payload for `question_set` returns `{"questions": []}`. This causes `draft` mode to behave like `quick` mode in tests unless explicitly overridden with `with_label_payload()`. Tests that exercise the AwaitingAnswers flow use this override.

## Testing

- 272 tests pass (248 pre-existing + 24 new requirements_drafting tests)
- All existing workflow engine, agent execution, and stub backend tests continue to pass unchanged
- New tests cover: model construction/terminal state, all four contract validation pipelines (valid + invalid), renderer output determinism, end-to-end quick/draft service integration
- Gherkin scenarios at `tests/conformance/features/requirements_drafting.feature` (10 scenarios)

---

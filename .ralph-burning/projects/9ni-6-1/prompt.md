Implement bead `ralph-burning-9ni.6.1` in `ralph-burning`.

Title: Define the task prompt contract for bead execution

Goal:
Define the canonical input shape for a milestone-aware bead execution prompt.

The contract should include clear sections for:
- milestone summary
- current bead details
- must-do scope
- explicit non-goals
- acceptance criteria
- already planned elsewhere
- review policy
- AGENTS/repo guidance

Acceptance criteria:
- prompt generator and workflow stages share one clear contract
- contract is stable enough to hash/version for drift detection

Implementation notes:
- preserve existing behavior outside this prompt-contract scope
- update the canonical prompt generator and any workflow-stage consumers that rely on this structure
- keep the contract explicit and inspectable
- add right-sized deterministic tests for contract generation/consumption changes

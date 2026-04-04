Implement bead `ralph-burning-9ni.6.2` in `ralph-burning`.

Title: Generate prompt.md from milestone summary, bead scope, dependencies, and planned-elsewhere context

Goal:
Generate the bead-linked `prompt.md` file from milestone and bead context so task execution starts from a deterministic, self-contained prompt.

The prompt should include:
- milestone context: name, goal, status, bead count, progress
- bead scope: title, description, acceptance criteria, non-goals
- dependency context: upstream dependencies with status/outcome and downstream dependents
- planned-elsewhere context: nearby related beads summarized for scope clarity
- milestone-plan constraints: architecture guardrails, naming conventions, testing expectations, explicit non-goals

Acceptance criteria:
- prompt.md includes milestone context, bead scope, dependency status, and planned-elsewhere context
- rendering is deterministic: same inputs produce identical output
- prompt is self-contained enough for an agent reading only prompt.md
- prompt stays concise enough for a reasonable context budget
- rendering composes with the existing Ralph prompt template system

Implementation notes:
- preserve the contract defined in `9ni.6.1`
- keep behavior outside prompt generation stable
- add deterministic tests for rendering and hashing-sensitive behavior
- do not expand scope into AGENTS guidance or broader context injection work already assigned to sibling beads

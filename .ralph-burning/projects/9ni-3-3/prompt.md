Implement bead `ralph-burning-9ni.3.3`: Render `plan.md`, `plan.json`, and an explicit acceptance map from the milestone planner.

Context:
- This is part of milestone planning output.
- The planner must produce durable plan artifacts that humans and downstream code can both consume.
- The artifacts should be deterministic for the same inputs.
- The artifacts must carry enough structure to drive bead creation and review scope boundaries.

Deliverables:
- `plan.md` for human review
- `plan.json` for machine use
- explicit acceptance map linked to workstreams and beads

Constraints:
- preserve compatibility with the existing milestone bundle contract and downstream milestone/bead generation work
- keep behavior explicit and inspectable
- add deterministic tests for artifact rendering and structure

Review bar:
- no ambiguous or lossy mapping between acceptance criteria and planned beads
- deterministic output for identical milestone planner inputs
- artifacts are suitable as authoritative inputs for downstream bead creation and review

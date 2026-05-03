## Bead ID: ralph-burning-9ni.7.2.1

## Goal

Define the data model for the three-way review classification that milestone-aware review and final_review stages will produce.

## Context

The workflow engine already has review/final_review stages that produce findings. This bead adds a structured classification schema so each finding can be categorized as one of three types, enabling downstream reconciliation (8.5.x) and prompt rendering (7.2.2).

## Classification Schema

### 1. fix-now
The finding is within the active bead's scope and should be remediated immediately.
- Fields: finding_summary (String), severity (enum: critical/high/medium/low), affected_files (Vec<String>), remediation_hint (Option<String>)

### 2. planned-elsewhere
The finding is valid but already owned by another bead in the graph.
- Fields: finding_summary (String), mapped_to_bead_id (String), confidence (f64), rationale (String)

### 3. propose-new-bead
The finding represents genuinely missing work not covered anywhere in the current plan.
- Fields: finding_summary (String), proposed_title (String), proposed_scope (String), severity (enum), rationale (String)

## Domain Validation Rules

- Every finding must have exactly one classification
- `mapped_to_bead_id` must be non-empty and syntactically valid (actual br existence check happens at reconciliation time, not at schema validation time)
- `proposed_title` must be non-empty and descriptive
- `confidence` for planned-elsewhere must be in range 0.0..=1.0
- Severity must be one of the defined enum variants
- The schema must derive Serialize/Deserialize for JSON persistence

## Acceptance Criteria

- Rust types defined for FindingClassification enum with FixNow, PlannedElsewhere, ProposeNewBead variants
- Each variant carries the required fields specified above
- Severity enum defined (Critical, High, Medium, Low)
- Domain validation function that rejects incomplete or malformed classifications
- Serde JSON round-trip tests
- Validation tests for edge cases (empty strings, out-of-range confidence, etc.)
- Existing tests pass

## Non-Goals

- Implementing the review logic that produces classifications (covered by 7.3, 7.5)
- Rendering the schema into prompts (covered by 7.2.2)
- br existence validation of mapped_to_bead_id (happens at reconciliation time)

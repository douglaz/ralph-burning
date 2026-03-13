use ralph_burning::shared::domain::{ProjectId, StageCursor, StageId};
use ralph_burning::shared::error::AppError;

#[test]
fn stage_cursor_rejects_zero_values() {
    assert!(StageCursor::new(StageId::Planning, 0, 1, 1).is_err());
    assert!(StageCursor::new(StageId::Planning, 1, 0, 1).is_err());
    assert!(StageCursor::new(StageId::Planning, 1, 1, 0).is_err());
}

#[test]
fn stage_cursor_cycle_and_completion_round_are_monotonic_and_independent() {
    let initial = StageCursor::initial(StageId::Planning);

    let next_cycle = initial.advance_cycle(StageId::Implementation).unwrap();
    assert_eq!(2, next_cycle.cycle);
    assert_eq!(1, next_cycle.completion_round);
    assert_eq!(1, next_cycle.attempt);

    let retry = next_cycle.retry().unwrap();
    assert_eq!(2, retry.cycle);
    assert_eq!(1, retry.completion_round);
    assert_eq!(2, retry.attempt);

    let next_completion_round = retry.advance_completion_round(StageId::Review).unwrap();
    assert_eq!(2, next_completion_round.cycle);
    assert_eq!(2, next_completion_round.completion_round);
    assert_eq!(1, next_completion_round.attempt);
}

#[test]
fn stage_cursor_retry_reports_attempt_overflow() {
    let cursor = StageCursor::new(StageId::Planning, 1, u32::MAX, 1).unwrap();

    let error = cursor.retry().expect_err("retry should overflow");

    assert!(matches!(
        error,
        AppError::StageCursorOverflow {
            field: "attempt",
            value: u32::MAX
        }
    ));
}

#[test]
fn stage_cursor_advance_cycle_reports_cycle_overflow() {
    let cursor = StageCursor::new(StageId::Planning, u32::MAX, 1, 1).unwrap();

    let error = cursor
        .advance_cycle(StageId::Implementation)
        .expect_err("cycle advance should overflow");

    assert!(matches!(
        error,
        AppError::StageCursorOverflow {
            field: "cycle",
            value: u32::MAX
        }
    ));
}

#[test]
fn stage_cursor_advance_completion_round_reports_round_overflow() {
    let cursor = StageCursor::new(StageId::Planning, 1, 1, u32::MAX).unwrap();

    let error = cursor
        .advance_completion_round(StageId::Review)
        .expect_err("completion round advance should overflow");

    assert!(matches!(
        error,
        AppError::StageCursorOverflow {
            field: "completion_round",
            value: u32::MAX
        }
    ));
}

#[test]
fn project_id_rejects_path_like_values() {
    for value in [
        "",
        ".",
        "..",
        "../escape",
        "nested/project",
        r"nested\project",
    ] {
        let error = ProjectId::new(value).expect_err("path-like project id should fail");
        assert!(matches!(error, AppError::InvalidIdentifier { .. }));
    }
}

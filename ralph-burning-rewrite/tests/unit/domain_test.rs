use ralph_burning::shared::domain::{StageCursor, StageId};

#[test]
fn stage_cursor_rejects_zero_values() {
    assert!(StageCursor::new(StageId::Planning, 0, 1, 1).is_err());
    assert!(StageCursor::new(StageId::Planning, 1, 0, 1).is_err());
    assert!(StageCursor::new(StageId::Planning, 1, 1, 0).is_err());
}

#[test]
fn stage_cursor_cycle_and_completion_round_are_monotonic_and_independent() {
    let initial = StageCursor::initial(StageId::Planning);

    let next_cycle = initial.advance_cycle(StageId::Implementation);
    assert_eq!(2, next_cycle.cycle);
    assert_eq!(1, next_cycle.completion_round);
    assert_eq!(1, next_cycle.attempt);

    let retry = next_cycle.retry();
    assert_eq!(2, retry.cycle);
    assert_eq!(1, retry.completion_round);
    assert_eq!(2, retry.attempt);

    let next_completion_round = retry.advance_completion_round(StageId::Review);
    assert_eq!(2, next_completion_round.cycle);
    assert_eq!(2, next_completion_round.completion_round);
    assert_eq!(1, next_completion_round.attempt);
}

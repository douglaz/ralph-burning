Feature: Workflow Checkpoints
  Primary stage completions create explicit git checkpoint commits so rollback
  targets named history entries with stable metadata instead of ambient HEAD.

  @workflow.rollback.hard_uses_checkpoint
  Scenario: Hard rollback resets to the checkpoint commit instead of the current HEAD
    Given an initialized workspace with a git-backed workflow project
    When the run creates an implementation checkpoint and HEAD later moves forward
    Then hard rollback to implementation resets HEAD to the checkpoint SHA

  @workflow.checkpoint.commit_metadata_stable
  Scenario: Checkpoint commits include the required subject and trailers
    Given an initialized workspace with a git-backed workflow project
    When the run creates an implementation checkpoint commit
    Then the checkpoint commit message matches the required metadata format

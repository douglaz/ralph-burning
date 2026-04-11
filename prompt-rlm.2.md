## Bead ID: ralph-burning-rlm.2

## Goal

Make PID liveness safe across platforms and after PID reuse

## Description

Non-Linux builds only populate proc_start_marker but is_pid_alive only checks proc_start_ticks, making macOS/BSD runs unrecoverable from run.pid. Legacy pid files without proc_start_ticks are treated as stale even when the process is alive, or conversely treated as live after PID reuse via raw kill(pid,0). Need: (1) honor proc_start_marker on non-Linux, (2) treat missing start-time fields as unsafe/legacy, (3) compare live process start time against recorded started_at for legacy records to detect PID reuse.

## Acceptance Criteria

- Existing tests pass
- cargo test && cargo clippy && cargo fmt --check

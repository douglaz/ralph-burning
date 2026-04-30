//! Shared test infrastructure for adapters, fixtures, and structured logs.
//!
//! These helpers are available to both in-crate unit tests and integration
//! tests via `crate::test_support` or `ralph_burning::test_support`.

pub mod br;
pub mod bv;
pub mod drain_harness;
pub mod e2e_fixtures;
pub mod env;
pub mod fixtures;
pub mod logging;

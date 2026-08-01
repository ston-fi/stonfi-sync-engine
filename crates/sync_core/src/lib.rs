#![doc = include_str!("../README.md")]

/// Error and result types returned by the crate.
pub mod errors;
/// In-memory status storage for tests and ephemeral processes.
pub mod mem_status_manager;
/// Sync engine types, extension traits, and lifecycle handles.
pub mod sync_engine;

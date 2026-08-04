#![doc = include_str!("../README.md")]

/// Error and result types returned by the crate.
pub mod errors;
/// In-memory progress storage for tests and ephemeral processes.
pub mod mem_progress_store;
/// ScyllaDB-backed progress storage and its builder.
#[cfg(feature = "scylla")]
pub mod scylla_progress_store;
/// Sync engine types, extension traits, and lifecycle handles.
pub mod sync_engine;

#![doc = include_str!("../README.md")]

/// In-memory coordinator state and task priority definitions.
pub mod coordinator;
/// Consumer-defined distributed handlers and payload serialization.
pub mod handler;
/// Adapter from a distributed handler to `stonfi_sync_core`.
pub mod synchronizer;
/// Common bincode-backed task and result payloads.
pub mod task;
/// gRPC task server and its lifecycle handle.
pub mod task_server;
/// Remote worker configuration, registration, and lifecycle.
pub mod worker;

#[allow(missing_docs)]
mod proto {
    tonic::include_proto!("stonfi.distributed_sync.v1");
}

use std::time::Duration;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};

pub(crate) fn timeout_deadline(duration: Duration, name: &str) -> SyncCoreResult<tokio::time::Instant> {
    if duration.is_zero() {
        return Err(SyncCoreError::invalid_args(format!("{name} must be positive")));
    }
    tokio::time::Instant::now()
        .checked_add(duration)
        .ok_or_else(|| SyncCoreError::invalid_args(format!("{name} is too large")))
}

pub(crate) fn timeout_millis(duration: Duration, name: &str) -> SyncCoreResult<u64> {
    let _ = timeout_deadline(duration, name)?;
    let milliseconds = u64::try_from(duration.as_millis())
        .map_err(|_| SyncCoreError::invalid_args(format!("{name} exceeds u64 milliseconds")))?;
    if milliseconds == 0 {
        return Err(SyncCoreError::invalid_args(format!("{name} must be at least one millisecond")));
    }
    Ok(milliseconds)
}

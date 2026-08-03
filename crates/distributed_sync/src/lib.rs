#![doc = include_str!("../README.md")]

/// In-memory coordinator state and task priority definitions.
pub mod coordinator;
mod distributed_adapter;
/// Common bincode-backed task and result payloads.
pub mod task;
/// gRPC task server and its lifecycle handle.
pub mod task_server;
/// Consumer-defined distributed traits and task batches.
pub mod traits;
mod utils;
/// Remote worker configuration, registration, and lifecycle.
pub mod worker;

#[cfg(test)]
mod _tests;

#[allow(missing_docs)]
mod proto {
    tonic::include_proto!("stonfi.distributed_sync.v1");
}

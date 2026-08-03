# Changelog

All notable changes to `stonfi_distributed_sync` are documented here.

## Unreleased

- Rename `handler::DistributedSyncHandler` to `traits::DistributedHandler` and
  `synchronizer::DistributedSynchronizer` to
  `distributed_adapter::DistributedAdapter`. Remove the unused `TaskBatch`
  getters.
- Update examples and integration coverage for the core `HeightLoader`,
  `HeightProvider`, and `ProgressProvider` API.
- Remove initial-height configuration from `DistributedHandler`; the core
  `SyncStatusStore` now owns and persists the engine-wide fallback.
- Emit diagnostics through `tracing` while preserving message text and levels.
- Use the core `u64` `SyncHeight` domain and the `retry_delay`, batch-size, and
  rewind handler controls. `RangeTask` serializes the full height range.
- Add the initial public distributed synchronization coordinator, gRPC task
  server, worker, payload contract, lifecycle handles, and metrics.
- Use absolute Unix assignment deadlines in the unreleased
  `stonfi.distributed_sync.v1` contract. Workers compare the coordinator
  deadline with their synchronized local system clock.
- Expose shared process-local coordination through `coordinator::Coordinator`.
- Delegate metric-cell initialization exclusively to application startup
  through `stonfi_metrics::init_metrics!`.
- Keep assignment routing worker-agnostic except for service-task support;
  workers report missing handlers as retryable task failures.
- Keep long-poll requests outside worker processing capacity so idle polls
  cannot starve exclusive service tasks.
- Back off failed task attempts within the synchronization deadline.
- Use one batch deadline across task creation, coordinator queueing, worker
  capacity waits, and task processing.
- Limit each distributed synchronization range to 10,000 ordered, buffered
  coordinator calls under the original batch deadline.
- Configure servers and workers through private-module builders and default
  worker parallelism to the system's available parallelism.
- Document every direct dependency required by the canonical consumer example.
- Clarify service-queue preference and priority/FIFO ordering, and relax
  `TaskPayload` values from `Send + Sync` to `Send`.

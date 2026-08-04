# Changelog

All notable consumer-facing changes to `stonfi_distributed_sync` are documented
here.

## [0.2.1](https://github.com/ston-fi/stonfi-sync-engine/compare/stonfi_distributed_sync-v0.2.0...stonfi_distributed_sync-v0.2.1) - 2026-08-04

### Other

- support untracked Cargo lockfile

## [0.1.0](https://github.com/ston-fi/stonfi-sync-engine/releases/tag/stonfi_distributed_sync-v0.1.0) - 2026-08-04

- Restore optional per-handler worker processing-stat summaries in application
  logs. The default zero period disables them; configure a non-zero period with
  `Worker::builder(...).with_stats_logging_period(...)`.
- Label the processing-stat log table's handler column as `handler_id`.
- Change `DistributedHandler::into_sync` and
  `Worker::builder(...).add_handler` to consume owned handlers instead of
  `Arc<Handler>`. Applications should construct independent handler instances
  for coordinator and worker processes.
- Return the private sync adapter from `DistributedHandler::into_sync` so it can
  be registered directly; convert it explicitly to a core `Synchronizer` only
  when its progress is an upstream dependency.
- Make `DistributedHandler::into_sync` infallible, share one absolute deadline
  across distributed task attempts, and pass polling, reconnect, and lifecycle
  durations through standard Tokio semantics. Millisecond wire values saturate
  to `u64::MAX` only on numeric overflow.
- Publish the package on crates.io after `stonfi_sync_core` and use registry
  releases for workspace dependencies.

## [0.0.1](https://github.com/ston-fi/stonfi-sync-engine/releases/tag/v0.0.1) - 2026-08-03

- Add the initial public distributed synchronization package with an in-memory
  coordinator, gRPC task server, workers, and one typed handler extension point.
- Provide at-least-once task delivery, ordered batch results, priority/FIFO
  queues, exclusive service tasks, bounded coordinator concurrency, and shared
  absolute deadlines across retries and processing.
- Add owned server and worker lifecycle handles with bounded shutdown.
- Document the trusted-network boundary, versioned `v1` wire protocol,
  idempotency requirements, metrics, and complete integration workflow.

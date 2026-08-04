# `stonfi_distributed_sync` Agent Guide

This package is a public Rust library published on crates.io and released
through Git tags as part of the `stonfi-sync-engine` workspace. Use the
`rust-library-review` skill for non-trivial reviews, implementations, and
refactors.

## Responsibility and non-goals

The package connects `stonfi_sync_core` to remote gRPC workers:

- `DistributedHandler` defines typed task creation, task execution, and
  ordered result handling.
- The private `DistributedAdapter` adapts a handler to `SyncHandler`.
- `Coordinator` owns the in-memory priority queues and in-flight completions.
- `TaskServer` exposes those queues through the private versioned protobuf API.
- `Worker` polls, routes, processes, and completes tasks with bounded lifecycle
  ownership.

The crate does not provide persistent queues, distributed locking, multi-writer
status coordination, authentication, TLS, forwarding, reflection, deployment
configuration, or a Tokio runtime. Keep those responsibilities outside this
package. Do not move transport concerns into `stonfi_sync_core`.

## Public API and construction

Keep public paths module-qualified; do not add root re-exports. The external
extension point is `traits::DistributedHandler`, with associated
`TaskPayload` task and result types. Do not add a parallel coordinator trait,
processor trait, codec abstraction, or convenience conversion without a
demonstrated consumer requirement.

Create independent handler instances for coordinator and worker processes.
Consume the coordinator-side instance with `handler.into_sync(coordinator)` and
pass the returned private adapter directly to `SyncEngine`'s builder. Convert
the adapter into a core `Synchronizer` first only when another handler depends
on its progress. Register each worker-side instance with
`Worker::builder(endpoint).add_handler(handler)`. Handler state is process-local;
only the handler type and stable ID are shared across binaries. A handler ID
must be stable and identical in every coordinator and worker binary.
`SyncEngine`'s builder owns ID validation when the synchronizer is registered;
distributed constructors do not duplicate it. Initial-height configuration
belongs to the core `SyncStatusStore`, not to distributed handlers.

Public fallible APIs return `stonfi_sync_core::errors::SyncCoreResult`. Keep
transport-generated protobuf types private. `TaskBatch` owns ordered tasks and
the height committed after successful result handling. Empty batches are valid.
`RangeTask` has public fields as an intentional passive serialization contract;
types with invariants should keep fields private.
`DistributedHandler::into_sync` is infallible. Handler durations pass through
the core Tokio timeout and also initialize the absolute deadline shared by
non-empty distributed task attempts. Pass polling, reconnect, and lifecycle
durations through standard Tokio semantics without zero-specific normalization
or validation. Millisecond wire values saturate to `u64::MAX` only on numeric
overflow.
Library diagnostics use `tracing`; applications own subscriber configuration.
Worker processing-stat logging is disabled by the default zero period. A
non-zero `with_stats_logging_period` value runs summaries at that interval from
the existing task counters, with per-handler period deltas for received,
processed, failed, timed-out, and completion-RPC-failed tasks.
Height-bearing APIs use the core `u64` `SyncHeight` domain and preserve `0` as
the initial no-progress sentinel. Handler retry and range controls use
`retry_delay`, `min_batch_size`, `max_batch_size`, and `allow_rewind`.

Create the shared `Coordinator` explicitly, then pass it to
`TaskServer::builder` and `DistributedHandler::into_sync`. Builders live in
private child modules and expose `with_*` configuration setters plus `build`;
the worker builder also exposes `add_handler` for required registrations. Do
not add configuration structs, builder re-exports, or parallel construction paths.
Worker parallelism defaults to `std::thread::available_parallelism()` and can be
overridden explicitly. Validate invariants before spawning background tasks.

Downstream applications depend on both crates.io packages at compatible
versions, as documented in `README.md`.

Initialize `stonfi_metrics`, create the shared coordinator plus independent
coordinator-side and worker-side handlers, start the server and workers, and
register the coordinator-side handler's adapter with the core engine.
Retain every run handle and shut down the core engine before workers and the
server. The README doctest and `examples/distributed.rs` are the canonical
integration references.

## Delivery, ordering, and lifecycle invariants

- Delivery is at-least-once. `process_task` must be idempotent because a task
  can repeat after failure, timeout, cancellation, or lost completion.
- Tasks in one batch run concurrently, while `handle_results` receives results
  in task-creation order.
- Task IDs identify one attempt. A retry receives a new ID; late completion of
  an expired attempt is rejected. One synchronization range submits at most
  10,000 coordinator tasks concurrently; later tasks retain result order and
  the original batch deadline.
- Except for service-task eligibility, every worker may receive any task. A
  worker without the assigned handler reports a retryable failure; do not add
  handler capability routing or handler-indexed queues.
- Service-capable workers select the service queue before the regular queue.
  Within either queue, higher priority dispatches first and equal-priority
  tasks are FIFO.
- A service task owns all task permits while it executes. Do not weaken this
  exclusivity when changing worker concurrency. Polling must not consume task
  permits.
- Cancelled coordinator futures must remove or invalidate their queued and
  in-flight assignments. Do not leave abandoned work or unbounded stale heap
  entries.
- Worker failures retry after the handler's backoff until the enclosing handler
  timeout. Task creation, queueing, worker capacity waits, and processing share
  that deadline. The core engine still owns range-level retries and status
  persistence.
- `run()` returns the only owner of spawned server or worker tasks. Dropping the
  handle requests best-effort cancellation; `shutdown()` cancels, waits
  boundedly, and aborts remaining tasks on timeout; `wait()` only observes
  natural completion.
- Consumer futures are cooperative. They may run until their task deadline or
  be aborted after the worker shutdown timeout.

## Protocol and compatibility

The protobuf package is `stonfi.distributed_sync.v1`. It contains only poll and
complete RPCs. Poll requests carry worker identity, timeout, and service-task
support, but no handler capability list. Assignments carry the coordinator's
absolute Unix deadline in milliseconds; workers compare it directly with their
local system clock and must not replace it with a fresh timeout. Coordinator and
worker hosts therefore require synchronized clocks.

Changing an RPC path, field number, outcome shape, task codec, default timeout,
delivery guarantee, result ordering, or service-task rule is a behavioral and
wire compatibility change. Update README, rustdoc, examples, tests, this guide,
and the consumer-facing changelog together.

The build script uses vendored `protoc` through `prost-build`; it must propagate
errors and must not mutate process environment or panic.

## Metrics

Metrics are global collectors registered through
`stonfi_metrics::register_metrics!` and stored in
`stonfi_metrics::MetricsCell`. Applications call
`stonfi_metrics::init_metrics!` once during startup, before dispatching
coordinator tasks, serving requests, or running a worker. Metric helpers access
registered cells directly and therefore panic if startup skipped
initialization. Constructors must not initialize individual metric cells or add
redundant availability checks. Keep worker IDs out of labels and preserve these
names and label sets because dashboards and alerts consume them:

- `stonfi_distributed_sync_coordinator_tasks_total{handler_id,status}`
- `stonfi_distributed_sync_coordinator_task_duration_ms{handler_id,status}`
- `stonfi_distributed_sync_coordinator_queue_size{kind}`
- `stonfi_distributed_sync_worker_polls_total{outcome}`
- `stonfi_distributed_sync_worker_tasks_total{handler_id,status}`
- `stonfi_distributed_sync_worker_task_duration_ms{handler_id,status}`
- `stonfi_distributed_sync_server_requests_total{method,status}`
- `stonfi_distributed_sync_server_request_duration_ms{method,status}`

Do not add provider-style collector APIs or tests that only assert metric
registration, names, labels, or increments.

## Errors, tests, and common mistakes

Production paths must not use `unwrap`, `expect`, or panic-driven control flow.
Validate malformed payloads, missing completion outcomes, invalid endpoints,
duplicate handlers, stale completions, and identifier/parallelism conversions
at the owning boundary. Millisecond wire values saturate only when they exceed
`u64`.

Tests should cover owned queue ordering, retries, stale attempts, timeout and
cancellation cleanup, ordered results, concurrency, service exclusivity, and
bounded shutdown. Use ephemeral localhost ports and deterministic
synchronization. Do not add tests for tonic, bincode derives, or Prometheus
registration by themselves.

Avoid these mistakes:

- re-exporting `stonfi_sync_core` or generated protobuf modules;
- accepting a detached atomic stop flag or spawning unowned progress tasks;
- using task or worker IDs as unbounded metric labels;
- adding special zero-duration behavior instead of using Tokio semantics;
- treating successful RPC receipt as exactly-once task execution;
- adding durable-queue claims to this in-memory coordinator.

## Package changes and validation

For any public API, behavior, protocol, dependency, metric, workspace, or
package-surface change, review and update README, rustdoc, example, tests, this
guide, changelog, CI, and package include rules in the same change, or record
why an artifact is unaffected.

Fast gate:

```text
cargo test -p stonfi_distributed_sync --all-features --locked
cargo clippy -p stonfi_distributed_sync --all-targets --all-features --locked -- -D warnings
```

Full gate from the workspace root:

```text
cargo test --workspace --all-features --locked
cargo test --workspace --doc --locked
cargo test --workspace --examples --locked
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps --all-features --locked
cargo +1.95.0 check --workspace --all-features --locked
cargo package --list --locked -p stonfi_sync_core
cargo package --list --locked -p stonfi_distributed_sync
cargo publish --dry-run --locked -p stonfi_sync_core
```

Also compile a fresh external consumer and inspect Cargo metadata/package
contents for private dependency origins. Release-plz owns dependency-ordered
publishing and releases `stonfi_sync_core` before this package when required. Do
not replace that native release flow unless explicitly requested.

# `stonfi_distributed_sync`

`stonfi_distributed_sync` distributes `stonfi_sync_core` handler work across
gRPC workers. A coordinator creates an in-memory task batch, workers process the
tasks concurrently, and the coordinator advances only after it receives and
accepts every ordered result.

The crate is distributed from the
[`stonfi-sync-engine`](https://github.com/ston-fi/stonfi-sync-engine) Git
repository. It requires Rust 1.95 or newer and a Tokio runtime.
Diagnostics are emitted through `tracing`; applications install and configure
their own subscriber.

Height-bearing APIs use the core `u64` `SyncHeight` domain. Height `0` remains
the core engine's initial no-progress sentinel.

Depend on both workspace packages from the same release tag:

```toml
[dependencies]
anyhow = "1"
async-trait = "0.1"
stonfi_distributed_sync = { git = "https://github.com/ston-fi/stonfi-sync-engine", tag = "v0.0.1" }
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", rev = "v0.0.1" }
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", tag = "v0.0.1" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

## Runtime model

Implement [`DistributedHandler`](crate::traits::DistributedHandler),
construct independent instances for the coordinator and each worker process,
and retain every lifecycle handle. This co-located example uses separate
instances just as separate deployments do:

```no_run
use std::net::SocketAddr;
use stonfi_distributed_sync::traits::DistributedHandler;
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::task_server::{TaskServer, TaskServerRunHandle};
use stonfi_distributed_sync::worker::{Worker, WorkerRunHandle};
use stonfi_sync_core::sync_engine::{SyncHandler, Synchronizer};

async fn build_runtime<H>(
    coordinator_handler: H,
    worker_handler: H,
    listen_address: SocketAddr,
) -> anyhow::Result<(impl SyncHandler + Into<Synchronizer>, WorkerRunHandle, TaskServerRunHandle)>
where
    H: DistributedHandler,
{
    stonfi_metrics::init_metrics!()?;
    let coordinator = Coordinator::new();
    let synchronizer = coordinator_handler.into_sync(coordinator.clone())?;

    let server = TaskServer::builder(coordinator)
        .with_listen_address(listen_address)
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let worker = Worker::builder(endpoint)
        .add_handler(worker_handler)?
        .build()?;

    Ok((synchronizer, worker.run(), server.run()))
}
```

Workers use [`std::thread::available_parallelism`] by default. Call
`with_parallelism` only when the application needs an explicit limit.

Call `into_sync` on the coordinator-side handler and pass the returned adapter
directly to `stonfi_sync_core::SyncEngine`'s builder. Convert it into a
`Synchronizer` first only when another handler depends on its progress. Register
separately constructed handlers with workers; handler state is local to each
process. On shutdown, stop the core engine before the worker and server. See
[`examples/distributed.rs`](examples/distributed.rs) for the complete workflow.
Initial-height configuration belongs to the core `SyncStatusStore`; distributed
handlers define task behavior only.

## Delivery and ordering

- Delivery is **at least once**. Processing may repeat after worker failure,
  timeout, lost completion, or coordinator retry. Handler effects must be
  idempotent.
- Tasks in one batch are dispatched concurrently. Results passed to
  `handle_results` retain task-creation order.
- An empty task batch is valid. Its height advances after `handle_results`
  accepts the empty result list.
- A service-capable worker selects the service queue first and falls back to
  regular work. Within the selected queue, higher-priority tasks are dispatched
  first, with FIFO ordering within one priority.
- Except for service-task eligibility, the coordinator does not filter tasks
  by worker capability. A worker without the assigned handler reports a
  retryable failure.
- A service task waits for exclusive access to that worker's configured task
  capacity. Idle long-polls do not consume processing capacity.
- Failed worker attempts wait for the handler's `retry_delay()` backoff
  before retrying. Task creation, queueing, worker capacity waits, and
  processing share the enclosing synchronization deadline.
- One synchronization range keeps at most 10,000 coordinator task futures in
  flight. New tasks are admitted as earlier tasks finish, while the original
  deadline and task-creation result order are preserved.

## Lifecycle

`TaskServer::run` and `Worker::run` return owned handles. `shutdown()` requests
cancellation and waits up to the configured shutdown timeout; a timed-out task
is aborted and reported as an error. Dropping a handle requests best-effort
shutdown without waiting. `wait()` observes natural termination.

The worker stops new polling promptly after cancellation. Already-running
consumer futures are cooperative and may continue until they finish, reach the
task deadline, or the worker shutdown timeout aborts the polling task.

## Metrics

Call `stonfi_metrics::init_metrics!` once during application startup, before
dispatching coordinator tasks, serving requests, or running a worker. The crate
registers coordinator, worker, and server counters, duration histograms, and
queue gauges under the `stonfi_distributed_sync_` prefix. Metric access before
startup initialization panics by design. Worker IDs are not metric labels.

## Boundaries and limitations

- The coordinator queue and in-flight completion state are in memory and are
  lost on restart.
- The protocol has no authentication, TLS, forwarding, or persistent transport.
  Deploy it only on a trusted network or behind infrastructure that supplies
  those controls.
- The wire protocol is `stonfi.distributed_sync.v1` and carries
  absolute Unix task deadlines. Coordinator and worker processes must use
  compatible crate revisions.
- Coordinator and worker hosts must keep their system clocks synchronized. Task
  assignments carry the coordinator's absolute Unix deadline, which workers
  compare directly with their local clocks.
- The crate does not provide distributed locking or multi-writer status
  coordination. The `stonfi_sync_core` single-writer rule still applies.
- Task payload compatibility is owned by each handler's
  [`TaskPayload`](crate::traits::TaskPayload) implementation.

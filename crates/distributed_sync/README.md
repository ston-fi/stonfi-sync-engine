# `stonfi_distributed_sync`

`stonfi_distributed_sync` distributes `stonfi_sync_core` handler work across
gRPC workers. A coordinator creates an in-memory task batch, workers process the
tasks concurrently, and the coordinator advances only after it receives and
accepts every ordered result.

The crate is currently unreleased and distributed from the
[`stonfi-sync-engine`](https://github.com/ston-fi/stonfi-sync-engine) Git
repository. It requires Rust 1.93 or newer and a Tokio runtime. During local
development, depend on both workspace packages from the same revision. A
downstream application using the example below needs these dependencies:

```toml
[dependencies]
anyhow = "1"
async-trait = "0.1"
stonfi_distributed_sync = { git = "https://github.com/ston-fi/stonfi-sync-engine", rev = "<revision>" }
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", rev = "v0.0.1" }
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", rev = "<revision>" }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

## Runtime model

Implement
[`DistributedSyncHandler`](crate::handler::DistributedSyncHandler), wrap the
same `Arc` in a
[`DistributedSynchronizer`](crate::synchronizer::DistributedSynchronizer) for
the coordinator, and register it with a [`Worker`](crate::worker::Worker) in
each worker process that can execute it.

```no_run
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::handler::{
    DistributedSyncHandler, TaskBatch,
};
use stonfi_distributed_sync::synchronizer::DistributedSynchronizer;
use stonfi_distributed_sync::task::{EmptyTaskResult, RangeTask};
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::worker::Worker;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::{SyncHeight, Synchronizer};

struct RangeHandler;

#[async_trait::async_trait]
impl DistributedSyncHandler for RangeHandler {
    type Task = RangeTask;
    type TaskResult = EmptyTaskResult;

    fn id(&self) -> &str {
        "range"
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn create_tasks(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        Ok(Some(TaskBatch::new(to, vec![RangeTask { from, to }])))
    }

    async fn process_task(
        &self,
        _task: Self::Task,
    ) -> SyncCoreResult<Self::TaskResult> {
        Ok(EmptyTaskResult)
    }

    fn sync_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

# #[tokio::main]
# async fn main() -> anyhow::Result<()> {
stonfi_metrics::init_metrics!()?;

let coordinator = Coordinator::new();
let handler = Arc::new(RangeHandler);
let distributed = DistributedSynchronizer::new(
    handler.clone(),
    coordinator.clone(),
)?;
let _core_synchronizer = Synchronizer::new(distributed);

let server = TaskServer::builder(coordinator)
    .with_listen_address("127.0.0.1:0".parse()?)
    .build()
    .await?;
let endpoint = format!("http://{}", server.local_address());
let server_handle = server.run();

let parallelism = NonZeroUsize::new(2)
    .ok_or_else(|| anyhow::anyhow!("parallelism must be positive"))?;
let worker = Worker::builder(endpoint)
    .with_parallelism(parallelism)
    .with_service_tasks_enabled(true)
    .add_handler(handler)?
    .build()?;
let worker_handle = worker.run();

worker_handle.shutdown().await?;
server_handle.shutdown().await?;
# Ok(())
# }
```

Workers use [`std::thread::available_parallelism`] by default. Call
`with_parallelism` only when the application needs an explicit limit.

The complete runnable example in `examples/distributed.rs` also wires the
adapter into a `stonfi_sync_core::SyncEngine`.

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
- Failed worker attempts wait for the handler's `sleep_on_error()` backoff
  before retrying. Task creation, queueing, worker capacity waits, and
  processing share the enclosing synchronization deadline.

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
- This protocol is a clean break from Tongrid's original `distributed_sync`;
  old and new coordinator/worker processes cannot interoperate.
- The crate does not provide distributed locking or multi-writer status
  coordination. The `stonfi_sync_core` single-writer rule still applies.
- Task payload compatibility is owned by each handler's
  [`TaskPayload`](crate::handler::TaskPayload) implementation.

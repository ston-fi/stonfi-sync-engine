use crate::coordinator::{Coordinator, TaskPriority};
use crate::distributed_adapter::DistributedAdapter;
use std::time::Duration;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight, Synchronizer};

/// Deterministic task and result serialization for gRPC transport.
///
/// Coordinators and workers must use compatible codecs.
pub trait TaskPayload: Send + Sized + 'static {
    /// Serializes this value for transport.
    ///
    /// # Errors
    ///
    /// Returns an error when encoding fails.
    fn encode(&self) -> SyncCoreResult<Vec<u8>>;

    /// Decodes one complete payload and rejects malformed or trailing data.
    ///
    /// # Errors
    ///
    /// Returns an error when decoding fails.
    fn decode(data: &[u8]) -> SyncCoreResult<Self>;
}

/// Tasks created for one engine range and the height they commit together.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use]
pub struct TaskBatch<T> {
    synced_height: SyncHeight,
    tasks: Vec<T>,
}

impl<T> TaskBatch<T> {
    /// Creates a batch whose successful completion advances to `synced_height`.
    ///
    /// Empty batches are valid. At most 10,000 tasks run concurrently and
    /// results preserve this vector's order.
    pub fn new(synced_height: SyncHeight, tasks: Vec<T>) -> Self {
        Self { synced_height, tasks }
    }

    pub(crate) fn into_parts(self) -> (SyncHeight, Vec<T>) {
        (self.synced_height, self.tasks)
    }
}

/// Defines coordinator-side task creation and worker-side task processing.
///
/// Construct independent handler instances for coordinator and worker
/// processes. Task processing is concurrent and at-least-once, so effects must
/// be idempotent.
#[async_trait::async_trait]
pub trait DistributedHandler: Send + Sync + 'static {
    /// Task payload sent to workers.
    type Task: TaskPayload;
    /// Result payload returned to the coordinator.
    type TaskResult: TaskPayload;

    /// Returns the stable handler ID used for routing, logging, metrics, and status.
    fn id(&self) -> &str;

    /// Consumes this coordinator-side handler into an adapter for registration
    /// with `stonfi_sync_core`.
    ///
    /// The adapter can be passed directly to `SyncEngine`'s builder. Convert it
    /// into a [`Synchronizer`] first when another handler depends on its
    /// progress.
    ///
    #[must_use = "register the returned adapter with SyncEngine"]
    fn into_sync(self, coordinator: Coordinator) -> impl SyncHandler + Into<Synchronizer>
    where
        Self: Sized,
    {
        DistributedAdapter::new(self, coordinator)
    }

    /// Creates tasks for the inclusive engine range `[from, to]`.
    ///
    /// `Ok(None)` ignores the range. Results preserve batch order.
    ///
    /// # Errors
    ///
    /// Returns an error when task creation fails.
    async fn create_tasks(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>>;

    /// Processes one task on a worker.
    ///
    /// Calls may run concurrently and repeat after ambiguous completion.
    ///
    /// # Errors
    ///
    /// Returns a retryable error bounded by the synchronization timeout.
    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult>;

    /// Validates or persists the ordered results before height advancement.
    ///
    /// # Errors
    ///
    /// Returns an error to reject and retry the range.
    async fn handle_results(&self, _synced_height: SyncHeight, _results: Vec<Self::TaskResult>) -> SyncCoreResult<()> {
        Ok(())
    }

    /// Returns the queue priority assigned to this handler's tasks.
    fn task_priority(&self) -> TaskPriority {
        TaskPriority::Normal
    }

    /// Returns whether tasks require exclusive execution on a service-capable
    /// worker.
    fn is_service_task(&self) -> bool {
        false
    }

    /// Returns whether the core engine may process new ranges.
    fn is_enabled(&self) -> bool {
        true
    }

    /// Returns the retry backoff used by distributed task attempts and the core
    /// engine.
    fn retry_delay(&self) -> Duration {
        Duration::from_millis(200)
    }

    /// Returns the minimum number of heights included in one engine batch.
    fn min_batch_size(&self) -> usize {
        1
    }

    /// Returns the maximum number of heights included in one engine batch.
    fn max_batch_size(&self) -> usize {
        1
    }

    /// Returns the end-to-end timeout for task creation, dispatch, processing,
    /// and result handling. The core passes it to Tokio, while distributed task
    /// attempts share an absolute deadline with millisecond wire precision.
    fn sync_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }

    /// Allows the batch to report a height below the offered range and restart
    /// this synchronizer from that lower height.
    fn allow_rewind(&self) -> bool {
        false
    }
}

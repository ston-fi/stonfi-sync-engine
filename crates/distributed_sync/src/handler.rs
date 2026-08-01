use crate::coordinator::TaskPriority;
use std::time::Duration;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::SyncHeight;

/// Serialization contract for task and result payloads sent over gRPC.
///
/// Implementations must be deterministic and must reject malformed input.
/// Coordinator and worker binaries must use compatible implementations.
pub trait TaskPayload: Send + Sized + 'static {
    /// Serializes this value for transport.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be represented by the codec.
    fn encode(&self) -> SyncCoreResult<Vec<u8>>;

    /// Deserializes one complete transport payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload is malformed, incomplete, or contains
    /// data that the implementation does not accept.
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
    /// An empty task list is valid and advances after
    /// [`DistributedSyncHandler::handle_results`] accepts an empty result list.
    pub fn new(synced_height: SyncHeight, tasks: Vec<T>) -> Self {
        Self { synced_height, tasks }
    }

    /// Returns the height committed after all tasks and result handling succeed.
    #[must_use]
    pub fn synced_height(&self) -> SyncHeight {
        self.synced_height
    }

    /// Returns the tasks in result-order.
    #[must_use]
    pub fn tasks(&self) -> &[T] {
        &self.tasks
    }

    pub(crate) fn into_parts(self) -> (SyncHeight, Vec<T>) {
        (self.synced_height, self.tasks)
    }
}

/// Defines coordinator-side task creation and worker-side task processing.
///
/// The same handler type is registered with the coordinator adapter and the
/// workers intended to process it. Workers may still receive tasks for
/// unregistered handlers and report them as retryable failures. The handler is
/// shared through [`std::sync::Arc`], and `process_task` can run concurrently.
/// Task processing is at-least-once: implementations must make externally
/// visible effects idempotent.
#[async_trait::async_trait]
pub trait DistributedSyncHandler: Send + Sync + 'static {
    /// Task payload sent to workers.
    type Task: TaskPayload;
    /// Result payload returned to the coordinator.
    type TaskResult: TaskPayload;

    /// Returns the stable ID used for routing, logging, metrics, and status.
    fn id(&self) -> &str;

    /// Creates tasks for the inclusive engine range `[from, to]`.
    ///
    /// Return `Ok(None)` to ignore the range without advancing. Task results are
    /// supplied to [`Self::handle_results`] in the same order as this batch.
    ///
    /// # Errors
    ///
    /// Returns a consumer error when task construction cannot complete.
    async fn create_tasks(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>>;

    /// Processes one task on a worker.
    ///
    /// This method can execute concurrently and can be called again after an
    /// ambiguous completion or failure.
    ///
    /// # Errors
    ///
    /// Returns an error that the coordinator treats as a retryable task failure
    /// until the enclosing synchronization attempt times out.
    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult>;

    /// Validates or persists the ordered results before height advancement.
    ///
    /// # Errors
    ///
    /// Returns an error to reject the batch and let `stonfi_sync_core` retry the
    /// synchronization range.
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
    /// and result handling.
    fn sync_timeout(&self) -> Duration {
        Duration::from_secs(10)
    }

    /// Allows the batch to report a height below the offered range and restart
    /// this synchronizer from that lower height.
    fn allow_rewind(&self) -> bool {
        false
    }
}

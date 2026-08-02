use crate::coordinator::{Coordinator, TaskDeadline, TaskPriority};
use crate::handler::{DistributedSyncHandler, TaskPayload};
use crate::utils::validate_timeout_millis;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use std::time::Duration;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};

const MAX_ONGOING_TASKS: usize = 10_000;

/// Adapts a [`DistributedSyncHandler`] to `stonfi_sync_core`.
#[derive(Clone)]
pub struct DistributedSynchronizer {
    handler: Arc<dyn ErasedHandler>,
    coordinator: Coordinator,
}

impl DistributedSynchronizer {
    /// Creates a coordinator-side adapter for `handler`.
    ///
    /// The same `Arc` can be registered through the builder returned by
    /// [`Worker::builder`](crate::worker::Worker::builder).
    ///
    /// # Errors
    ///
    /// Returns an error when the synchronization timeout cannot be represented
    /// by the distributed protocol.
    pub fn new<H>(handler: Arc<H>, coordinator: Coordinator) -> SyncCoreResult<Self>
    where
        H: DistributedSyncHandler,
    {
        validate_timeout_millis(handler.sync_timeout(), "distributed handler synchronization timeout")?;
        Ok(Self { handler, coordinator })
    }
}

#[async_trait::async_trait]
impl SyncHandler for DistributedSynchronizer {
    fn id(&self) -> &str {
        self.handler.id()
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        let batch_timeout = self.handler.sync_timeout();
        let task_deadline = TaskDeadline::new(batch_timeout)?;
        let Some((synced_height, payloads)) = self.handler.create_tasks_bytes(from, to).await? else {
            return Ok(None);
        };

        let result_payloads = stream::iter(
            payloads
                .into_iter()
                .map(|payload| self.coordinator.handle_task(self.handler.clone(), payload, task_deadline)),
        )
        .buffered(MAX_ONGOING_TASKS)
        .try_collect()
        .await?;

        self.handler.handle_results_bytes(synced_height, result_payloads).await?;
        Ok(Some(synced_height))
    }

    fn is_enabled(&self) -> bool {
        self.handler.is_enabled()
    }

    fn retry_delay(&self) -> Duration {
        self.handler.retry_delay()
    }

    fn min_batch_size(&self) -> usize {
        self.handler.min_batch_size()
    }

    fn max_batch_size(&self) -> usize {
        self.handler.max_batch_size()
    }

    fn sync_timeout(&self) -> Duration {
        self.handler.sync_timeout()
    }

    fn allow_rewind(&self) -> bool {
        self.handler.allow_rewind()
    }
}

#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync {
    fn id(&self) -> &str;
    async fn create_tasks_bytes(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<(SyncHeight, Vec<Vec<u8>>)>>;
    async fn process_task_bytes(&self, task_payload: &[u8]) -> SyncCoreResult<Vec<u8>>;
    async fn handle_results_bytes(
        &self,
        synced_height: SyncHeight,
        result_payloads: Vec<Vec<u8>>,
    ) -> SyncCoreResult<()>;
    fn task_priority(&self) -> TaskPriority;
    fn is_service_task(&self) -> bool;
    fn is_enabled(&self) -> bool;
    fn retry_delay(&self) -> Duration;
    fn min_batch_size(&self) -> usize;
    fn max_batch_size(&self) -> usize;
    fn sync_timeout(&self) -> Duration;
    fn allow_rewind(&self) -> bool;
}

#[async_trait::async_trait]
impl<T> ErasedHandler for T
where
    T: DistributedSyncHandler,
{
    fn id(&self) -> &str {
        DistributedSyncHandler::id(self)
    }

    async fn create_tasks_bytes(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<(SyncHeight, Vec<Vec<u8>>)>> {
        let Some(batch) = DistributedSyncHandler::create_tasks(self, from, to).await? else {
            return Ok(None);
        };
        let (synced_height, tasks) = batch.into_parts();
        let payloads = tasks
            .into_iter()
            .map(|task| task.encode())
            .collect::<SyncCoreResult<Vec<_>>>()?;
        Ok(Some((synced_height, payloads)))
    }

    async fn process_task_bytes(&self, task_payload: &[u8]) -> SyncCoreResult<Vec<u8>> {
        let task = T::Task::decode(task_payload)?;
        DistributedSyncHandler::process_task(self, task).await?.encode()
    }

    async fn handle_results_bytes(
        &self,
        synced_height: SyncHeight,
        result_payloads: Vec<Vec<u8>>,
    ) -> SyncCoreResult<()> {
        let results = result_payloads
            .iter()
            .map(|payload| T::TaskResult::decode(payload))
            .collect::<SyncCoreResult<Vec<_>>>()?;
        DistributedSyncHandler::handle_results(self, synced_height, results).await
    }

    fn task_priority(&self) -> TaskPriority {
        DistributedSyncHandler::task_priority(self)
    }

    fn is_service_task(&self) -> bool {
        DistributedSyncHandler::is_service_task(self)
    }

    fn is_enabled(&self) -> bool {
        DistributedSyncHandler::is_enabled(self)
    }

    fn retry_delay(&self) -> Duration {
        DistributedSyncHandler::retry_delay(self)
    }

    fn min_batch_size(&self) -> usize {
        DistributedSyncHandler::min_batch_size(self)
    }

    fn max_batch_size(&self) -> usize {
        DistributedSyncHandler::max_batch_size(self)
    }

    fn sync_timeout(&self) -> Duration {
        DistributedSyncHandler::sync_timeout(self)
    }

    fn allow_rewind(&self) -> bool {
        DistributedSyncHandler::allow_rewind(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{DistributedSynchronizer, MAX_ONGOING_TASKS};
    use crate::coordinator::Coordinator;
    use crate::handler::{DistributedSyncHandler, TaskBatch};
    use crate::proto::CompleteRequest;
    use crate::proto::complete_request::Outcome;
    use crate::task::{EmptyTaskResult, RangeTask};
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Duration;
    use stonfi_sync_core::errors::SyncCoreResult;
    use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};

    struct LargeBatchHandler;

    #[async_trait::async_trait]
    impl DistributedSyncHandler for LargeBatchHandler {
        type Task = RangeTask;
        type TaskResult = EmptyTaskResult;

        fn id(&self) -> &str {
            "large-batch"
        }

        async fn create_tasks(
            &self,
            from: SyncHeight,
            to: SyncHeight,
        ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
            Ok(Some(TaskBatch::new(to, vec![RangeTask { from, to }; MAX_ONGOING_TASKS + 1])))
        }

        async fn process_task(&self, _task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
            Ok(EmptyTaskResult)
        }

        fn sync_timeout(&self) -> Duration {
            Duration::from_secs(30)
        }
    }

    #[tokio::test]
    async fn test_large_batch_limits_coordinator_ongoing_tasks() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let coordinator = Coordinator::new();
        let mut synchronizer = DistributedSynchronizer::new(Arc::new(LargeBatchHandler), coordinator.clone())?;
        let sync_task = tokio::spawn(async move { synchronizer.sync_range(1, 1).await });

        let mut assignments = VecDeque::with_capacity(MAX_ONGOING_TASKS);
        for _ in 0..MAX_ONGOING_TASKS {
            assignments.push_back(
                coordinator
                    .poll(Duration::from_secs(1), false)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("expected a buffered assignment"))?,
            );
        }
        assert!(coordinator.poll(Duration::from_millis(10), false).await?.is_none());

        let first = assignments
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("expected the first buffered assignment"))?;
        complete_assignment(&coordinator, first.assignment_id)?;
        let final_assignment = coordinator
            .poll(Duration::from_secs(1), false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected the final buffered assignment"))?;

        for assignment in assignments {
            complete_assignment(&coordinator, assignment.assignment_id)?;
        }
        complete_assignment(&coordinator, final_assignment.assignment_id)?;

        assert_eq!(sync_task.await??, Some(1));
        Ok(())
    }

    fn complete_assignment(coordinator: &Coordinator, assignment_id: u64) -> SyncCoreResult<()> {
        coordinator.complete(CompleteRequest {
            worker_id: "test-worker".to_owned(),
            assignment_id,
            outcome: Some(Outcome::ResultPayload(Vec::new())),
        })
    }
}

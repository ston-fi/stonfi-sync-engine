use crate::coordinator::{Coordinator, TaskDeadline, TaskPriority};
use crate::traits::{DistributedHandler, TaskPayload};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;
use std::time::Duration;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};

const MAX_ONGOING_TASKS: usize = 10_000;

/// Adapts a [`DistributedHandler`] to `stonfi_sync_core`.
#[derive(Clone)]
pub(crate) struct DistributedAdapter {
    handler: Arc<dyn ErasedHandler>,
    coordinator: Coordinator,
}

impl DistributedAdapter {
    pub(crate) fn new<H>(handler: H, coordinator: Coordinator) -> Self
    where
        H: DistributedHandler,
    {
        Self {
            handler: Arc::new(handler),
            coordinator,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for DistributedAdapter {
    fn id(&self) -> &str {
        self.handler.id()
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        let batch_timeout = self.handler.sync_timeout();
        let task_deadline = TaskDeadline::new(batch_timeout);
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
    T: DistributedHandler,
{
    fn id(&self) -> &str {
        DistributedHandler::id(self)
    }

    async fn create_tasks_bytes(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<(SyncHeight, Vec<Vec<u8>>)>> {
        let Some(batch) = DistributedHandler::create_tasks(self, from, to).await? else {
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
        DistributedHandler::process_task(self, task).await?.encode()
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
        DistributedHandler::handle_results(self, synced_height, results).await
    }

    fn task_priority(&self) -> TaskPriority {
        DistributedHandler::task_priority(self)
    }

    fn is_service_task(&self) -> bool {
        DistributedHandler::is_service_task(self)
    }

    fn is_enabled(&self) -> bool {
        DistributedHandler::is_enabled(self)
    }

    fn retry_delay(&self) -> Duration {
        DistributedHandler::retry_delay(self)
    }

    fn min_batch_size(&self) -> usize {
        DistributedHandler::min_batch_size(self)
    }

    fn max_batch_size(&self) -> usize {
        DistributedHandler::max_batch_size(self)
    }

    fn sync_timeout(&self) -> Duration {
        DistributedHandler::sync_timeout(self)
    }

    fn allow_rewind(&self) -> bool {
        DistributedHandler::allow_rewind(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{DistributedAdapter, MAX_ONGOING_TASKS};
    use crate::coordinator::Coordinator;
    use crate::proto::complete_request::Outcome;
    use crate::proto::{CompleteRequest, TaskAssignment};
    use crate::task::{EmptyTaskResult, RangeTask};
    use crate::traits::{DistributedHandler, TaskBatch};
    use parking_lot::Mutex;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Duration;
    use stonfi_sync_core::errors::SyncCoreResult;
    use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};

    struct LargeBatchHandler;

    struct OrderingHandler {
        results: Arc<Mutex<Vec<SyncHeight>>>,
    }

    #[async_trait::async_trait]
    impl DistributedHandler for OrderingHandler {
        type Task = RangeTask;
        type TaskResult = RangeTask;

        fn id(&self) -> &str {
            "ordering"
        }

        async fn create_tasks(
            &self,
            _from: SyncHeight,
            to: SyncHeight,
        ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
            let tasks = (1..=3)
                .map(|height| RangeTask {
                    from: height,
                    to: height,
                })
                .collect();
            Ok(Some(TaskBatch::new(to, tasks)))
        }

        async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
            Ok(task)
        }

        async fn handle_results(
            &self,
            _synced_height: SyncHeight,
            results: Vec<Self::TaskResult>,
        ) -> SyncCoreResult<()> {
            *self.results.lock() = results.into_iter().map(|result| result.from).collect();
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DistributedHandler for LargeBatchHandler {
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
        let mut adapter = DistributedAdapter::new(LargeBatchHandler, coordinator.clone());
        let sync_task = tokio::spawn(async move { adapter.sync_range(1, 1).await });

        let mut assignments = VecDeque::with_capacity(MAX_ONGOING_TASKS);
        for _ in 0..MAX_ONGOING_TASKS {
            assignments.push_back(
                coordinator
                    .poll("worker", Duration::from_secs(1), false)
                    .await
                    .ok_or_else(|| anyhow::anyhow!("expected a buffered assignment"))?,
            );
        }
        assert!(coordinator.poll("worker", Duration::from_millis(10), false).await.is_none());

        let first = assignments
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("expected the first buffered assignment"))?;
        complete_assignment(&coordinator, first.assignment_id)?;
        let final_assignment = coordinator
            .poll("worker", Duration::from_secs(1), false)
            .await
            .ok_or_else(|| anyhow::anyhow!("expected the final buffered assignment"))?;

        for assignment in assignments {
            complete_assignment(&coordinator, assignment.assignment_id)?;
        }
        complete_assignment(&coordinator, final_assignment.assignment_id)?;

        assert_eq!(sync_task.await??, Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn test_results_preserve_creation_order() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let coordinator = Coordinator::new();
        let results = Arc::new(Mutex::new(Vec::new()));
        let handler = OrderingHandler {
            results: results.clone(),
        };
        let mut adapter = DistributedAdapter::new(handler, coordinator.clone());
        let sync_task = tokio::spawn(async move { adapter.sync_range(1, 3).await });

        let mut assignments = Vec::new();
        for _ in 0..3 {
            assignments.push(
                coordinator
                    .poll("worker", Duration::from_secs(1), false)
                    .await
                    .ok_or_else(|| anyhow::anyhow!("expected an assignment"))?,
            );
        }
        for assignment in assignments.into_iter().rev() {
            complete_with_payload(&coordinator, assignment)?;
        }

        assert_eq!(sync_task.await??, Some(3));
        assert_eq!(&*results.lock(), &[1, 2, 3]);
        Ok(())
    }

    fn complete_assignment(coordinator: &Coordinator, assignment_id: u64) -> SyncCoreResult<()> {
        coordinator.complete(CompleteRequest {
            worker_id: "test-worker".to_owned(),
            assignment_id,
            outcome: Some(Outcome::ResultPayload(Vec::new())),
        })
    }

    fn complete_with_payload(coordinator: &Coordinator, assignment: TaskAssignment) -> SyncCoreResult<()> {
        coordinator.complete(CompleteRequest {
            worker_id: "test-worker".to_owned(),
            assignment_id: assignment.assignment_id,
            outcome: Some(Outcome::ResultPayload(assignment.payload)),
        })
    }
}

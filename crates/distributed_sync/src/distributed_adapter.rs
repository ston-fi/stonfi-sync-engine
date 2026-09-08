use crate::coordinator::{Coordinator, TaskDeadline};
use crate::traits::{DistributedHandler, TaskPayload};
use futures::stream::{self, StreamExt, TryStreamExt};
use std::time::Duration;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};

const MAX_ONGOING_TASKS: usize = 10_000;

/// Adapts a [`DistributedHandler`] to `stonfi_sync_core`.
pub(crate) struct DistributedAdapter<H> {
    // Preserve the opaque adapter's Unpin bound even for a !Unpin handler.
    handler: Box<H>,
    coordinator: Coordinator,
}

impl<H: DistributedHandler> DistributedAdapter<H> {
    pub(crate) fn new(handler: H, coordinator: Coordinator) -> Self {
        Self {
            handler: Box::new(handler),
            coordinator,
        }
    }
}

#[async_trait::async_trait]
impl<H: DistributedHandler> SyncHandler for DistributedAdapter<H> {
    fn id(&self) -> &str {
        self.handler.id()
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        let batch_timeout = self.handler.sync_timeout();
        let task_deadline = TaskDeadline::new(batch_timeout);
        let Some(batch) = self.handler.create_tasks(from, to).await? else {
            return Ok(None);
        };

        let (synced_height, tasks) = batch.into_parts();
        let payloads = tasks
            .into_iter()
            .map(|task| task.encode())
            .collect::<SyncCoreResult<Vec<_>>>()?;
        let result_payloads: Vec<Vec<u8>> = stream::iter(
            payloads
                .into_iter()
                .map(|payload| self.coordinator.handle_task(self.handler.as_ref(), payload, task_deadline)),
        )
        .buffered(MAX_ONGOING_TASKS)
        .try_collect()
        .await?;

        let results = result_payloads
            .iter()
            .map(|payload| H::TaskResult::decode(payload))
            .collect::<SyncCoreResult<Vec<_>>>()?;
        self.handler.handle_results(synced_height, results).await?;
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

    #[tokio::test]
    async fn test_malformed_result_does_not_reach_result_handler() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let coordinator = Coordinator::new();
        let results = Arc::new(Mutex::new(vec![99]));
        let handler = OrderingHandler {
            results: results.clone(),
        };
        let mut adapter = DistributedAdapter::new(handler, coordinator.clone());
        let sync_task = tokio::spawn(async move { adapter.sync_range(1, 3).await });

        for index in 0..3 {
            let assignment = coordinator
                .poll("worker", Duration::from_secs(1), false)
                .await
                .ok_or_else(|| anyhow::anyhow!("expected an assignment"))?;
            if index == 1 {
                complete_assignment(&coordinator, assignment.assignment_id)?;
            } else {
                complete_with_payload(&coordinator, assignment)?;
            }
        }

        assert!(sync_task.await?.is_err());
        assert_eq!(&*results.lock(), &[99]);
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

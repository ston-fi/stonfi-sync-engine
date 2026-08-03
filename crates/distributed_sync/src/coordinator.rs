mod metrics;
mod queue;
mod types;

pub(crate) use types::TaskDeadline;
pub use types::TaskPriority;

use crate::distributed_adapter::ErasedHandler;
use crate::proto::complete_request::Outcome;
use crate::proto::{CompleteRequest, TaskAssignment};
use metrics::{CoordinatorMetrics, CoordinatorTaskStatus};
use parking_lot::Mutex;
use queue::TaskQueue;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use tokio::sync::{Notify, oneshot};

/// Process-local task queues and in-flight assignments shared by all clones.
///
/// The coordinator is not durable and does not own the server or workers.
#[derive(Clone, Default)]
pub struct Coordinator {
    inner: Arc<Inner>,
}

impl Coordinator {
    /// Creates empty coordination state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn handle_task(
        &self,
        handler: Arc<dyn ErasedHandler>,
        payload: Vec<u8>,
        deadline: TaskDeadline,
    ) -> SyncCoreResult<Vec<u8>> {
        self.inner.handle_task(handler, payload, deadline).await
    }

    pub(crate) async fn poll(&self, polling_timeout: Duration, service_tasks_enabled: bool) -> Option<TaskAssignment> {
        self.inner.poll(polling_timeout, service_tasks_enabled).await
    }

    pub(crate) fn complete(&self, request: CompleteRequest) -> SyncCoreResult<()> {
        self.inner.complete(request)
    }

    #[cfg(test)]
    fn queued_task_count(&self) -> usize {
        self.inner.state.lock().ongoing.values().filter(|task| task.queued).count()
    }
}

#[derive(Default)]
struct Inner {
    next_assignment_id: AtomicU64,
    state: Mutex<State>,
    task_available: Notify,
}

#[derive(Default)]
struct State {
    queue: TaskQueue,
    ongoing: HashMap<u64, OngoingTask>,
}

struct OngoingTask {
    completion: oneshot::Sender<SyncCoreResult<Vec<u8>>>,
    deadline: tokio::time::Instant,
    queued: bool,
}

impl Inner {
    async fn handle_task(
        self: &Arc<Self>,
        handler: Arc<dyn ErasedHandler>,
        payload: Vec<u8>,
        deadline: TaskDeadline,
    ) -> SyncCoreResult<Vec<u8>> {
        let started_at = Instant::now();
        let id = handler.id().to_owned();

        loop {
            let elapsed = started_at.elapsed();
            if deadline.instant <= tokio::time::Instant::now() {
                CoordinatorMetrics::complete(&id, CoordinatorTaskStatus::TimedOut, elapsed);
                return Err(SyncCoreError::net(format!(
                    "distributed task for handler '{id}' reached its deadline"
                )));
            }

            let assignment_id = self.next_assignment_id()?;
            let assignment = TaskAssignment {
                assignment_id,
                handler_id: id.clone(),
                payload: payload.clone(),
                deadline_unix_ms: deadline.unix_ms,
                service_task: handler.is_service_task(),
            };
            let receiver = self.push(assignment, handler.task_priority(), deadline.instant);
            let guard = AssignmentGuard::new(self.clone(), assignment_id);
            CoordinatorMetrics::queued(&id);

            let completion = tokio::time::timeout_at(deadline.instant, receiver).await;
            drop(guard);

            match completion {
                Ok(Ok(Ok(result))) => {
                    CoordinatorMetrics::complete(&id, CoordinatorTaskStatus::Processed, started_at.elapsed());
                    return Ok(result);
                },
                Ok(Ok(Err(error))) => {
                    CoordinatorMetrics::complete(&id, CoordinatorTaskStatus::Failed, started_at.elapsed());
                    tracing::warn!("[DISTRIBUTED_SYNC][{id}] assignment {assignment_id} failed: {error}; retrying");
                    sleep_before_retry(handler.retry_delay(), deadline.instant).await;
                },
                Ok(Err(error)) => {
                    CoordinatorMetrics::complete(&id, CoordinatorTaskStatus::Failed, started_at.elapsed());
                    tracing::warn!(
                        "[DISTRIBUTED_SYNC][{id}] assignment {assignment_id} completion channel closed: {error}; retrying"
                    );
                    sleep_before_retry(handler.retry_delay(), deadline.instant).await;
                },
                Err(_) => {
                    CoordinatorMetrics::complete(&id, CoordinatorTaskStatus::TimedOut, started_at.elapsed());
                    return Err(SyncCoreError::net(format!(
                        "distributed task for handler '{id}' reached its deadline"
                    )));
                },
            }
        }
    }

    fn next_assignment_id(&self) -> SyncCoreResult<u64> {
        self.next_assignment_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_add(1))
            .map(|previous| previous + 1)
            .map_err(|_| SyncCoreError::logic("distributed assignment ID space exhausted"))
    }

    fn push(
        &self,
        assignment: TaskAssignment,
        priority: TaskPriority,
        deadline: tokio::time::Instant,
    ) -> oneshot::Receiver<SyncCoreResult<Vec<u8>>> {
        let assignment_id = assignment.assignment_id;
        let (completion, receiver) = oneshot::channel();
        let (regular_size, service_size) = {
            let mut state = self.state.lock();
            state.queue.push(assignment, priority);
            state.ongoing.insert(
                assignment_id,
                OngoingTask {
                    completion,
                    deadline,
                    queued: true,
                },
            );
            state.queue.sizes()
        };
        CoordinatorMetrics::set_queue_sizes(regular_size, service_size);
        self.task_available.notify_waiters();
        receiver
    }

    async fn poll(&self, polling_timeout: Duration, service_tasks_enabled: bool) -> Option<TaskAssignment> {
        let poll = async {
            loop {
                let notified = self.task_available.notified();
                if let Some(assignment) = self.pop(service_tasks_enabled) {
                    return assignment;
                }
                notified.await;
            }
        };
        tokio::time::timeout(polling_timeout, poll).await.ok()
    }

    fn pop(&self, service_tasks_enabled: bool) -> Option<TaskAssignment> {
        let (assignment, regular_size, service_size) = {
            let mut state = self.state.lock();
            let assignment = loop {
                let Some(candidate) = state.queue.pop(service_tasks_enabled) else {
                    break None;
                };
                let Some(ongoing) = state.ongoing.get_mut(&candidate.assignment_id) else {
                    continue;
                };
                if !ongoing.queued {
                    continue;
                }
                if ongoing.deadline <= tokio::time::Instant::now() {
                    continue;
                }
                ongoing.queued = false;
                break Some(candidate);
            };
            let (regular_size, service_size) = state.queue.sizes();
            (assignment, regular_size, service_size)
        };
        CoordinatorMetrics::set_queue_sizes(regular_size, service_size);
        assignment
    }

    fn complete(&self, request: CompleteRequest) -> SyncCoreResult<()> {
        let outcome = request
            .outcome
            .ok_or_else(|| SyncCoreError::invalid_args("completion outcome is missing"))?;
        let completion = {
            let mut state = self.state.lock();
            let ongoing = state.ongoing.get(&request.assignment_id).ok_or_else(|| {
                SyncCoreError::invalid_args(format!("assignment {} is stale or unknown", request.assignment_id))
            })?;
            if ongoing.queued {
                return Err(SyncCoreError::logic(format!(
                    "assignment {} was completed before dispatch",
                    request.assignment_id
                )));
            }
            let ongoing = state.ongoing.remove(&request.assignment_id).ok_or_else(|| {
                SyncCoreError::logic(format!("assignment {} disappeared during completion", request.assignment_id))
            })?;
            ongoing.completion
        };

        let result = match outcome {
            Outcome::ResultPayload(payload) => Ok(payload),
            Outcome::ErrorMessage(message) => {
                let error: Arc<dyn std::error::Error + Send + Sync> = Arc::new(std::io::Error::other(format!(
                    "worker '{}' failed assignment {}: {message}",
                    request.worker_id, request.assignment_id
                )));
                Err(SyncCoreError::external(error))
            },
        };
        completion.send(result).map_err(|_| {
            SyncCoreError::logic(format!("assignment {} completion receiver was dropped", request.assignment_id))
        })
    }

    fn cancel(&self, assignment_id: u64) {
        let queue_sizes = {
            let mut state = self.state.lock();
            let removed = state.ongoing.remove(&assignment_id);
            removed.filter(|task| task.queued).map(|_| {
                state.queue.remove(assignment_id);
                state.queue.sizes()
            })
        };
        if let Some((regular_size, service_size)) = queue_sizes {
            CoordinatorMetrics::set_queue_sizes(regular_size, service_size);
        }
    }
}

async fn sleep_before_retry(delay: Duration, deadline: tokio::time::Instant) {
    let now = tokio::time::Instant::now();
    let retry_at = now.checked_add(delay).map_or(deadline, |candidate| candidate.min(deadline));
    tokio::time::sleep_until(retry_at).await;
}

struct AssignmentGuard {
    inner: Arc<Inner>,
    assignment_id: u64,
}

impl AssignmentGuard {
    fn new(inner: Arc<Inner>, assignment_id: u64) -> Self {
        Self { inner, assignment_id }
    }
}

impl Drop for AssignmentGuard {
    fn drop(&mut self) {
        self.inner.cancel(self.assignment_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{Coordinator, TaskDeadline};
    use crate::distributed_adapter::ErasedHandler;
    use crate::proto::CompleteRequest;
    use crate::proto::complete_request::Outcome;
    use crate::task::{EmptyTaskResult, RangeTask};
    use crate::task_server::TaskServiceImpl;
    use crate::traits::{DistributedHandler, TaskBatch};
    use std::sync::Arc;
    use std::time::Duration;
    use stonfi_sync_core::errors::SyncCoreResult;
    use stonfi_sync_core::sync_engine::SyncHeight;

    fn init_test_metrics() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        Ok(())
    }

    struct TestHandler(Duration);

    #[async_trait::async_trait]
    impl DistributedHandler for TestHandler {
        type Task = RangeTask;
        type TaskResult = EmptyTaskResult;

        fn id(&self) -> &str {
            "test"
        }

        async fn create_tasks(
            &self,
            from: SyncHeight,
            to: SyncHeight,
        ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
            Ok(Some(TaskBatch::new(to, vec![RangeTask { from, to }])))
        }

        async fn process_task(&self, _task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
            Ok(EmptyTaskResult)
        }

        fn retry_delay(&self) -> Duration {
            Duration::from_millis(20)
        }

        fn sync_timeout(&self) -> Duration {
            self.0
        }
    }

    #[tokio::test]
    async fn test_failed_assignment_is_retried_and_completed() -> anyhow::Result<()> {
        init_test_metrics()?;
        let coordinator = Coordinator::new();
        let timeout = Duration::from_millis(100);
        let handler: Arc<dyn ErasedHandler> = Arc::new(TestHandler(timeout));
        let coordinator_for_task = coordinator.clone();
        let deadline = TaskDeadline::new(timeout);
        let task = tokio::spawn(async move { coordinator_for_task.handle_task(handler, vec![1], deadline).await });

        let first = coordinator
            .poll(Duration::from_secs(1), false)
            .await
            .ok_or_else(|| anyhow::anyhow!("first assignment was not dispatched"))?;
        coordinator.complete(CompleteRequest {
            worker_id: "worker".to_owned(),
            assignment_id: first.assignment_id,
            outcome: Some(Outcome::ErrorMessage("retry".to_owned())),
        })?;

        assert!(coordinator.poll(Duration::from_millis(5), false).await.is_none());
        let second = coordinator
            .poll(Duration::from_secs(1), false)
            .await
            .ok_or_else(|| anyhow::anyhow!("retried assignment was not dispatched"))?;
        assert_ne!(first.assignment_id, second.assignment_id);
        assert_eq!(first.deadline_unix_ms, second.deadline_unix_ms);
        coordinator.complete(CompleteRequest {
            worker_id: "worker".to_owned(),
            assignment_id: second.assignment_id,
            outcome: Some(Outcome::ResultPayload(vec![9])),
        })?;

        assert_eq!(task.await??, vec![9]);
        Ok(())
    }

    #[tokio::test]
    async fn test_cancelled_waiter_invalidates_queued_assignment() -> anyhow::Result<()> {
        init_test_metrics()?;
        let coordinator = Coordinator::new();
        let timeout = Duration::from_millis(100);
        let handler: Arc<dyn ErasedHandler> = Arc::new(TestHandler(timeout));
        let coordinator_for_task = coordinator.clone();
        let deadline = TaskDeadline::new(timeout);
        let task = tokio::spawn(async move { coordinator_for_task.handle_task(handler, vec![1], deadline).await });

        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;
        tokio::task::yield_now().await;

        assert_eq!(coordinator.queued_task_count(), 0);
        assert!(coordinator.poll(Duration::from_millis(10), false).await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_server_rejects_missing_completion_outcome() -> anyhow::Result<()> {
        use crate::proto::task_service_server::TaskService;
        use tonic::Request;

        init_test_metrics()?;
        let service = TaskServiceImpl::new(Coordinator::new());
        let result = service
            .complete(Request::new(CompleteRequest {
                worker_id: "worker".to_owned(),
                assignment_id: 1,
                outcome: None,
            }))
            .await;

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_timed_out_assignment_becomes_stale() -> anyhow::Result<()> {
        init_test_metrics()?;
        let coordinator = Coordinator::new();
        let timeout = Duration::from_millis(100);
        let handler: Arc<dyn ErasedHandler> = Arc::new(TestHandler(timeout));
        let coordinator_for_task = coordinator.clone();
        let deadline = TaskDeadline::new(timeout);
        let task = tokio::spawn(async move { coordinator_for_task.handle_task(handler, vec![1], deadline).await });

        let assignment = coordinator
            .poll(Duration::from_secs(1), false)
            .await
            .ok_or_else(|| anyhow::anyhow!("assignment was not dispatched"))?;
        assert!(task.await?.is_err());

        assert!(
            coordinator
                .complete(CompleteRequest {
                    worker_id: "worker".to_owned(),
                    assignment_id: assignment.assignment_id,
                    outcome: Some(Outcome::ResultPayload(vec![1])),
                })
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_dispatch_preserves_absolute_deadline_after_queue_delay() -> anyhow::Result<()> {
        init_test_metrics()?;
        let coordinator = Coordinator::new();
        let timeout = Duration::from_secs(2);
        let handler: Arc<dyn ErasedHandler> = Arc::new(TestHandler(timeout));
        let coordinator_for_task = coordinator.clone();
        let deadline = TaskDeadline::new(timeout);
        let deadline_unix_ms = deadline.unix_ms;
        let task = tokio::spawn(async move { coordinator_for_task.handle_task(handler, vec![1], deadline).await });

        while coordinator.queued_task_count() == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let assignment = coordinator
            .poll(Duration::from_secs(1), false)
            .await
            .ok_or_else(|| anyhow::anyhow!("assignment was not dispatched"))?;

        assert_eq!(assignment.deadline_unix_ms, deadline_unix_ms);
        task.abort();
        let _ = task.await;
        Ok(())
    }
}

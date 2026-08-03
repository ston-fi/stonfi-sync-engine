mod builder;
mod grpc_client;
mod metrics;

use crate::proto::complete_request::Outcome;
use crate::proto::{CompleteRequest, TaskAssignment};
use crate::synchronizer::ErasedHandler;
use crate::utils::deadline_from_unix_millis;
use builder::Builder;
use futures::stream::{FuturesUnordered, StreamExt};
use grpc_client::GrpcClient;
use metrics::{PollOutcome, WorkerMetrics, WorkerTaskStatus};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Endpoint;

/// Configured worker before execution starts.
pub struct Worker {
    inner: Arc<Inner>,
}

impl Worker {
    /// Starts configuring a worker for `endpoint`.
    #[must_use]
    pub fn builder(endpoint: impl Into<String>) -> Builder {
        Builder::new(endpoint.into())
    }

    /// Starts worker polling loops on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, following
    /// [`tokio::spawn`] semantics.
    #[must_use = "the run handle owns worker shutdown and task completion"]
    pub fn run(self) -> WorkerRunHandle {
        let cancellation = CancellationToken::new();
        let tasks = FuturesUnordered::new();
        for _ in 0..self.inner.parallelism {
            tasks.push(tokio::spawn(run_loop(self.inner.clone(), cancellation.clone())));
        }
        WorkerRunHandle {
            cancellation,
            tasks,
            shutdown_timeout: self.inner.shutdown_timeout,
        }
    }
}

/// Owns polling loops spawned by [`Worker::run`].
#[must_use = "dropping the run handle requests worker shutdown without waiting"]
pub struct WorkerRunHandle {
    cancellation: CancellationToken,
    tasks: FuturesUnordered<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl WorkerRunHandle {
    /// Requests shutdown and waits for polling and active tasks to finish.
    ///
    /// # Errors
    ///
    /// Returns an error on task failure or timeout. Timed-out tasks are aborted.
    pub async fn shutdown(mut self) -> SyncCoreResult<()> {
        self.cancellation.cancel();
        let shutdown_timeout = self.shutdown_timeout;
        match tokio::time::timeout(shutdown_timeout, self.join_tasks()).await {
            Ok(result) => result,
            Err(_) => {
                for task in self.tasks.iter() {
                    task.abort();
                }
                let _ = self.join_tasks().await;
                Err(SyncCoreError::system(format!(
                    "worker shutdown exceeded {shutdown_timeout:.3?}"
                )))
            },
        }
    }

    /// Waits for natural worker termination without requesting shutdown.
    ///
    /// The first terminated loop cancels and joins the rest.
    ///
    /// # Errors
    ///
    /// Returns an error when a polling task panics or is cancelled.
    pub async fn wait(mut self) -> SyncCoreResult<()> {
        self.join_tasks().await
    }

    async fn join_tasks(&mut self) -> SyncCoreResult<()> {
        let mut first_error = None;
        let cancellation = self.cancellation.clone();
        while let Some(result) = self.tasks.next().await {
            cancellation.cancel();
            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(SyncCoreError::system(format!("worker task failed to join: {error}")));
                    for task in self.tasks.iter() {
                        task.abort();
                    }
                } else {
                    tracing::warn!("[DISTRIBUTED_SYNC][WORKER] additional join failure: {error}");
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for WorkerRunHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct Inner {
    worker_id: String,
    endpoint: Endpoint,
    service_tasks_enabled: bool,
    polling_timeout: Duration,
    reconnect_delay: Duration,
    shutdown_timeout: Duration,
    parallelism: u32,
    active_tasks: Arc<Semaphore>,
    handlers: HashMap<String, Arc<dyn ErasedHandler>>,
}

async fn run_loop(inner: Arc<Inner>, cancellation: CancellationToken) {
    loop {
        let Some(mut client) = connect(&inner, &cancellation).await else {
            return;
        };
        loop {
            let poll_result = tokio::select! {
                _ = cancellation.cancelled() => return,
                result = client.poll() => result,
            };
            let assignment = match poll_result {
                Ok(Some(assignment)) => {
                    WorkerMetrics::poll(PollOutcome::Task);
                    assignment
                },
                Ok(None) => {
                    WorkerMetrics::poll(PollOutcome::Empty);
                    continue;
                },
                Err(error) => {
                    WorkerMetrics::poll(PollOutcome::Error);
                    tracing::warn!("[DISTRIBUTED_SYNC][WORKER][{}] poll failed: {error}", inner.worker_id);
                    break;
                },
            };

            let id = assignment.handler_id.clone();
            let started_at = Instant::now();
            WorkerMetrics::task(&id, WorkerTaskStatus::Received, Duration::ZERO);
            let Some(completion) = task_completion(&inner, assignment, &cancellation, started_at).await else {
                return;
            };

            if let Err(error) = client.complete(completion).await {
                WorkerMetrics::task(&id, WorkerTaskStatus::CompletionFailed, started_at.elapsed());
                tracing::warn!("[DISTRIBUTED_SYNC][WORKER][{}] completion RPC failed: {error}", inner.worker_id);
                break;
            }
        }
    }
}

async fn task_completion(
    inner: &Inner,
    assignment: TaskAssignment,
    cancellation: &CancellationToken,
    started_at: Instant,
) -> Option<CompleteRequest> {
    let id = &assignment.handler_id;
    let task_deadline = match deadline_from_unix_millis(assignment.deadline_unix_ms, "task deadline") {
        Ok(task_deadline) => task_deadline,
        Err(error) => {
            WorkerMetrics::task(id, WorkerTaskStatus::TimedOut, started_at.elapsed());
            return Some(completion_request(
                &inner.worker_id,
                assignment.assignment_id,
                Outcome::ErrorMessage(error.to_string()),
            ));
        },
    };

    let _permit = match processing_permit(inner, &assignment, cancellation, task_deadline).await {
        ProcessingPermit::Acquired(permit) => permit,
        ProcessingPermit::TimedOut => {
            WorkerMetrics::task(id, WorkerTaskStatus::TimedOut, started_at.elapsed());
            return Some(completion_request(
                &inner.worker_id,
                assignment.assignment_id,
                Outcome::ErrorMessage(format!(
                    "assignment {} timed out waiting for processing capacity",
                    assignment.assignment_id
                )),
            ));
        },
        ProcessingPermit::Stopped => return None,
    };

    let completion = process_assignment(inner, assignment, started_at, task_deadline).await;
    Some(completion)
}

async fn connect(inner: &Inner, cancellation: &CancellationToken) -> Option<GrpcClient> {
    loop {
        let result = tokio::select! {
            _ = cancellation.cancelled() => return None,
            result = GrpcClient::connect(
                inner.endpoint.clone(),
                inner.worker_id.clone(),
                inner.polling_timeout,
                inner.service_tasks_enabled,
            ) => result,
        };
        match result {
            Ok(client) => return Some(client),
            Err(error) => {
                tracing::warn!("[DISTRIBUTED_SYNC][WORKER][{}] connection failed: {error}", inner.worker_id);
                tokio::select! {
                    _ = cancellation.cancelled() => return None,
                    _ = tokio::time::sleep(inner.reconnect_delay) => {},
                }
            },
        }
    }
}

async fn processing_permit(
    inner: &Inner,
    assignment: &TaskAssignment,
    cancellation: &CancellationToken,
    task_deadline: tokio::time::Instant,
) -> ProcessingPermit {
    let permits = if assignment.service_task { inner.parallelism } else { 1 };
    tokio::select! {
        _ = cancellation.cancelled() => ProcessingPermit::Stopped,
        permit = tokio::time::timeout_at(task_deadline, inner.active_tasks.clone().acquire_many_owned(permits)) => {
            match permit {
                Ok(Ok(permit)) => ProcessingPermit::Acquired(permit),
                Ok(Err(error)) => {
                    tracing::error!("[DISTRIBUTED_SYNC][WORKER] task semaphore closed: {error}");
                    ProcessingPermit::Stopped
                },
                Err(_) => ProcessingPermit::TimedOut,
            }
        },
    }
}

enum ProcessingPermit {
    Acquired(OwnedSemaphorePermit),
    TimedOut,
    Stopped,
}

async fn process_assignment(
    inner: &Inner,
    assignment: TaskAssignment,
    started_at: Instant,
    task_deadline: tokio::time::Instant,
) -> CompleteRequest {
    let id = assignment.handler_id.clone();
    let outcome = process_outcome(inner, &assignment, &id, started_at, task_deadline).await;

    completion_request(&inner.worker_id, assignment.assignment_id, outcome)
}

fn completion_request(worker_id: &str, assignment_id: u64, outcome: Outcome) -> CompleteRequest {
    CompleteRequest {
        worker_id: worker_id.to_owned(),
        assignment_id,
        outcome: Some(outcome),
    }
}

async fn process_outcome(
    inner: &Inner,
    assignment: &TaskAssignment,
    id: &str,
    started_at: Instant,
    task_deadline: tokio::time::Instant,
) -> Outcome {
    let Some(handler) = inner.handlers.get(id) else {
        WorkerMetrics::task(id, WorkerTaskStatus::Failed, started_at.elapsed());
        return Outcome::ErrorMessage(format!("worker has no handler '{id}'"));
    };

    match tokio::time::timeout_at(task_deadline, handler.process_task_bytes(&assignment.payload)).await {
        Ok(Ok(payload)) => {
            WorkerMetrics::task(id, WorkerTaskStatus::Processed, started_at.elapsed());
            Outcome::ResultPayload(payload)
        },
        Ok(Err(error)) => {
            WorkerMetrics::task(id, WorkerTaskStatus::Failed, started_at.elapsed());
            Outcome::ErrorMessage(error.to_string())
        },
        Err(_) => {
            WorkerMetrics::task(id, WorkerTaskStatus::TimedOut, started_at.elapsed());
            Outcome::ErrorMessage(format!("assignment {} reached its deadline", assignment.assignment_id))
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{Inner, ProcessingPermit, Worker, process_outcome, processing_permit, task_completion};
    use crate::coordinator::Coordinator;
    use crate::handler::{DistributedSyncHandler, TaskBatch};
    use crate::proto::TaskAssignment;
    use crate::proto::complete_request::Outcome;
    use crate::synchronizer::{DistributedSynchronizer, ErasedHandler};
    use crate::task::{EmptyTaskResult, RangeTask};
    use crate::task_server::{PollObserver, TaskServer};
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use stonfi_sync_core::errors::SyncCoreResult;
    use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;
    use tonic::transport::Endpoint;

    struct TestHandler {
        id: &'static str,
        service_task: bool,
    }

    struct DeadlineHandler {
        process_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl DistributedSyncHandler for DeadlineHandler {
        type Task = RangeTask;
        type TaskResult = EmptyTaskResult;

        fn id(&self) -> &str {
            "absolute-deadline"
        }

        async fn create_tasks(
            &self,
            from: SyncHeight,
            to: SyncHeight,
        ) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
            Ok(Some(TaskBatch::new(to, vec![RangeTask { from, to }])))
        }

        async fn process_task(&self, _task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
            self.process_calls.fetch_add(1, Ordering::SeqCst);
            Ok(EmptyTaskResult)
        }
    }

    #[async_trait::async_trait]
    impl DistributedSyncHandler for TestHandler {
        type Task = RangeTask;
        type TaskResult = EmptyTaskResult;

        fn id(&self) -> &str {
            self.id
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

        fn is_service_task(&self) -> bool {
            self.service_task
        }

        fn sync_timeout(&self) -> Duration {
            Duration::from_millis(300)
        }
    }

    #[tokio::test]
    async fn test_idle_polls_do_not_hold_service_task_capacity() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let coordinator = Coordinator::new();
        let observer = Arc::new(PollObserver::new());
        let server = TaskServer::builder(coordinator.clone())
            .with_listen_address("127.0.0.1:0".parse()?)
            .with_shutdown_timeout(Duration::from_secs(1))
            .build_with_poll_observer(observer.clone())
            .await?;
        let endpoint = format!("http://{}", server.local_address());
        let server_handle = server.run();

        let handler = Arc::new(TestHandler {
            id: "deterministic-service-capacity",
            service_task: true,
        });
        let mut synchronizer = DistributedSynchronizer::new(handler.clone(), coordinator)?;
        let parallelism = NonZeroUsize::new(2).ok_or_else(|| anyhow::anyhow!("parallelism must be positive"))?;
        let worker_handle = Worker::builder(endpoint)
            .with_parallelism(parallelism)
            .with_service_tasks_enabled(true)
            .with_polling_timeout(Duration::from_secs(2))
            .with_reconnect_delay(Duration::from_millis(10))
            .with_shutdown_timeout(Duration::from_secs(1))
            .add_handler(handler)?
            .build()?
            .run();

        tokio::time::timeout(Duration::from_secs(1), observer.wait_for(2)).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), synchronizer.sync_range(1, 1)).await??,
            Some(1)
        );

        worker_handle.shutdown().await?;
        server_handle.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_missing_handler_is_reported_as_worker_failure() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let inner = Inner {
            worker_id: "test-worker".to_owned(),
            endpoint: Endpoint::from_static("http://127.0.0.1:1"),
            service_tasks_enabled: false,
            polling_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            parallelism: 1,
            active_tasks: Arc::new(Semaphore::new(1)),
            handlers: HashMap::new(),
        };
        let assignment = TaskAssignment {
            assignment_id: 1,
            handler_id: "unregistered".to_owned(),
            payload: Vec::new(),
            deadline_unix_ms: crate::utils::deadline_unix_millis(Duration::from_millis(100), "test deadline")?,
            service_task: false,
        };

        let timeout = Duration::from_millis(100);
        let outcome = process_outcome(
            &inner,
            &assignment,
            "unregistered",
            std::time::Instant::now(),
            tokio::time::Instant::now() + timeout,
        )
        .await;
        match outcome {
            Outcome::ErrorMessage(message) => assert!(message.contains("has no handler 'unregistered'")),
            Outcome::ResultPayload(_) => return Err(anyhow::anyhow!("missing handler must fail the assignment")),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_processing_capacity_wait_uses_assignment_deadline() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let inner = Inner {
            worker_id: "test-worker".to_owned(),
            endpoint: Endpoint::from_static("http://127.0.0.1:1"),
            service_tasks_enabled: false,
            polling_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            parallelism: 1,
            active_tasks: Arc::new(Semaphore::new(1)),
            handlers: HashMap::new(),
        };
        let active_permit = inner.active_tasks.clone().acquire_owned().await?;
        let assignment = TaskAssignment {
            assignment_id: 1,
            handler_id: "test".to_owned(),
            payload: Vec::new(),
            deadline_unix_ms: crate::utils::deadline_unix_millis(Duration::from_millis(10), "test deadline")?,
            service_task: false,
        };

        let result = processing_permit(
            &inner,
            &assignment,
            &CancellationToken::new(),
            tokio::time::Instant::now() + Duration::from_millis(10),
        )
        .await;
        assert!(matches!(result, ProcessingPermit::TimedOut));
        drop(active_permit);
        Ok(())
    }

    #[tokio::test]
    async fn test_expired_absolute_deadline_reports_failure_without_processing() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        let handler = Arc::new(DeadlineHandler {
            process_calls: AtomicUsize::new(0),
        });
        let mut handlers = HashMap::new();
        let erased_handler: Arc<dyn ErasedHandler> = handler.clone();
        let id = DistributedSyncHandler::id(handler.as_ref()).to_owned();
        handlers.insert(id.clone(), erased_handler);
        let inner = Inner {
            worker_id: "test-worker".to_owned(),
            endpoint: Endpoint::from_static("http://127.0.0.1:1"),
            service_tasks_enabled: false,
            polling_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(1),
            parallelism: 1,
            active_tasks: Arc::new(Semaphore::new(1)),
            handlers,
        };
        let completion = task_completion(
            &inner,
            TaskAssignment {
                assignment_id: 1,
                handler_id: id,
                payload: Vec::new(),
                deadline_unix_ms: 1,
                service_task: false,
            },
            &CancellationToken::new(),
            std::time::Instant::now(),
        )
        .await
        .ok_or_else(|| anyhow::anyhow!("expired task should produce a completion"))?;

        assert!(matches!(completion.outcome, Some(Outcome::ErrorMessage(_))));
        assert_eq!(handler.process_calls.load(Ordering::SeqCst), 0);
        Ok(())
    }
}

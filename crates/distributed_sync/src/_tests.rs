use crate::coordinator::Coordinator;
use crate::distributed_adapter::DistributedAdapter;
use crate::task::{EmptyTaskResult, RangeTask};
use crate::task_server::{TaskServer, TaskServerRunHandle};
use crate::traits::{DistributedHandler, TaskBatch};
use crate::worker::{Worker, WorkerRunHandle};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_sync_core::mem_progress_store::MemProgressStore;
use stonfi_sync_core::sync_engine::{
    HeightLoader, HeightProvider, SyncEngine, SyncHandler, SyncHeight, SyncProgressStore,
};

fn init_test_metrics() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;
    Ok(())
}

struct TestHandler {
    id: &'static str,
    tasks: Vec<RangeTask>,
    service_task: bool,
    process_delay: Duration,
    create_delay: Duration,
    sync_timeout: Duration,
    state: Arc<TestHandlerState>,
}

#[derive(Default)]
struct TestHandlerState {
    active: AtomicUsize,
    max_active: AtomicUsize,
    results: Mutex<Option<Vec<SyncHeight>>>,
}

impl TestHandler {
    fn new(id: &'static str) -> Self {
        Self {
            id,
            tasks: (1..=3)
                .map(|height| RangeTask {
                    from: height,
                    to: height,
                })
                .collect(),
            service_task: false,
            process_delay: Duration::from_millis(1),
            create_delay: Duration::ZERO,
            sync_timeout: Duration::from_secs(2),
            state: Arc::new(TestHandlerState::default()),
        }
    }

    fn with_tasks(mut self, tasks: Vec<RangeTask>) -> Self {
        self.tasks = tasks;
        self
    }

    fn with_service_task(mut self) -> Self {
        self.service_task = true;
        self
    }

    fn with_process_delay(mut self, process_delay: Duration) -> Self {
        self.process_delay = process_delay;
        self
    }

    fn with_sync_timeout(mut self, sync_timeout: Duration) -> Self {
        self.sync_timeout = sync_timeout;
        self
    }

    fn with_create_delay(mut self, create_delay: Duration) -> Self {
        self.create_delay = create_delay;
        self
    }

    fn state(&self) -> Arc<TestHandlerState> {
        self.state.clone()
    }
}

#[async_trait::async_trait]
impl DistributedHandler for TestHandler {
    type Task = RangeTask;
    type TaskResult = RangeTask;

    fn id(&self) -> &str {
        self.id
    }

    async fn create_tasks(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        tokio::time::sleep(self.create_delay).await;
        Ok(Some(TaskBatch::new(to, self.tasks.clone())))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        let active = self.state.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.state.max_active.fetch_max(active, Ordering::SeqCst);
        let multiplier = match task.from {
            1 => 3,
            2 => 2,
            _ => 1,
        };
        tokio::time::sleep(self.process_delay.saturating_mul(multiplier)).await;
        self.state.active.fetch_sub(1, Ordering::SeqCst);
        Ok(task)
    }

    async fn handle_results(&self, _synced_height: SyncHeight, results: Vec<Self::TaskResult>) -> SyncCoreResult<()> {
        *self.state.results.lock() = Some(results.into_iter().map(|result| result.from).collect());
        Ok(())
    }

    fn is_service_task(&self) -> bool {
        self.service_task
    }

    fn sync_timeout(&self) -> Duration {
        self.sync_timeout
    }
}

struct OneHeightLoader;

#[async_trait::async_trait]
impl HeightLoader for OneHeightLoader {
    fn id(&self) -> &str {
        "one-height"
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        if after == 0 {
            return Ok(1);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(after)
    }
}

struct PanickingHandler;

#[async_trait::async_trait]
impl DistributedHandler for PanickingHandler {
    type Task = RangeTask;
    type TaskResult = EmptyTaskResult;

    fn id(&self) -> &str {
        "panicking-handler"
    }

    async fn create_tasks(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        Ok(Some(TaskBatch::new(to, vec![RangeTask { from: to, to }])))
    }

    async fn process_task(&self, _task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        panic!("intentional worker join-failure test")
    }
}

#[tokio::test]
async fn test_sync_engine_runs_through_server_and_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler_id = "engine-end-to-end";
    let (distributed, worker, server) = setup(TestHandler::new(handler_id), TestHandler::new(handler_id)).await?;
    let height_provider = HeightProvider::new(OneHeightLoader);
    let progress_store = Arc::new(MemProgressStore::new(0));
    let engine = SyncEngine::builder(progress_store.clone())
        .add_synchronizer(distributed, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build()
        .run();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if progress_store.load_synced_height(handler_id).await? == Some(1) {
                return Ok::<(), SyncCoreError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    engine.shutdown().await?;
    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_task_creation_uses_the_batch_deadline() -> anyhow::Result<()> {
    init_test_metrics()?;
    let coordinator_handler = TestHandler::new("slow-task-creation")
        .with_create_delay(Duration::from_millis(60))
        .with_sync_timeout(Duration::from_millis(40));
    let worker_handler = TestHandler::new("slow-task-creation");
    let worker_state = worker_handler.state();
    let (mut synchronizer, worker, server) = setup(coordinator_handler, worker_handler).await?;

    assert!(synchronizer.sync_range(1, 1).await.is_err());
    assert_eq!(worker_state.max_active.load(Ordering::SeqCst), 0);

    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_service_tasks_are_exclusive_on_the_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let coordinator_handler = TestHandler::new("service-exclusion").with_service_task();
    let worker_handler = TestHandler::new("service-exclusion").with_process_delay(Duration::from_millis(10));
    let worker_state = worker_handler.state();
    let (mut synchronizer, worker, server) = setup(coordinator_handler, worker_handler).await?;

    assert_eq!(synchronizer.sync_range(1, 3).await?, Some(3),);
    assert_eq!(worker_state.max_active.load(Ordering::SeqCst), 1);

    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_empty_batch_advances_without_a_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = TestHandler::new("empty-batch").with_tasks(Vec::new());
    let handler_state = handler.state();
    let mut synchronizer = DistributedAdapter::new(handler, Coordinator::new());

    assert_eq!(synchronizer.sync_range(4, 7).await?, Some(7));
    assert_eq!(*handler_state.results.lock(), Some(Vec::new()));
    Ok(())
}

#[tokio::test]
async fn test_worker_shutdown_interrupts_connection_backoff() -> anyhow::Result<()> {
    init_test_metrics()?;
    let worker = Worker::builder("http://127.0.0.1:9")
        .with_reconnect_delay(Duration::from_secs(5))
        .with_shutdown_timeout(Duration::from_millis(200))
        .add_handler(TestHandler::new("unavailable-server"))?
        .build()?;
    let handle = worker.run();

    tokio::time::sleep(Duration::from_millis(20)).await;
    let started_at = Instant::now();
    handle.shutdown().await?;
    assert!(started_at.elapsed() < Duration::from_millis(200));
    Ok(())
}

#[tokio::test]
async fn test_worker_shutdown_interrupts_long_poll() -> anyhow::Result<()> {
    init_test_metrics()?;
    let coordinator = Coordinator::new();
    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .with_shutdown_timeout(Duration::from_secs(1))
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let worker = Worker::builder(endpoint)
        .with_polling_timeout(Duration::from_secs(5))
        .with_shutdown_timeout(Duration::from_millis(200))
        .add_handler(TestHandler::new("long-poll"))?
        .build()?
        .run();

    tokio::time::sleep(Duration::from_millis(50)).await;
    tokio::time::timeout(Duration::from_millis(200), worker.shutdown()).await??;
    server_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_worker_shutdown_aborts_overlong_active_task() -> anyhow::Result<()> {
    init_test_metrics()?;
    let coordinator = Coordinator::new();
    let mut synchronizer = DistributedAdapter::new(TestHandler::new("bounded-shutdown"), coordinator.clone());
    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .with_shutdown_timeout(Duration::from_secs(1))
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let worker_handler = TestHandler::new("bounded-shutdown").with_process_delay(Duration::from_millis(500));
    let worker_state = worker_handler.state();
    let worker = Worker::builder(endpoint)
        .with_polling_timeout(Duration::from_millis(20))
        .with_reconnect_delay(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(30))
        .add_handler(worker_handler)?
        .build()?;
    let worker_handle = worker.run();
    let sync_task = tokio::spawn(async move { synchronizer.sync_range(1, 3).await });

    tokio::time::timeout(Duration::from_secs(1), async {
        while worker_state.active.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let started_at = Instant::now();
    assert!(worker_handle.shutdown().await.is_err());
    assert!(started_at.elapsed() < Duration::from_millis(200));

    sync_task.abort();
    let _ = sync_task.await;
    server_handle.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_worker_wait_reports_join_failure() -> anyhow::Result<()> {
    init_test_metrics()?;
    let (mut synchronizer, worker, server) = setup(PanickingHandler, PanickingHandler).await?;
    let sync_task = tokio::spawn(async move { synchronizer.sync_range(1, 1).await });

    let result = tokio::time::timeout(Duration::from_secs(1), worker.wait()).await?;
    assert!(result.is_err());

    sync_task.abort();
    let _ = sync_task.await;
    server.shutdown().await?;
    Ok(())
}

#[test]
fn test_duplicate_handler_registration_is_rejected() -> anyhow::Result<()> {
    let result = Worker::builder("http://127.0.0.1:1")
        .add_handler(TestHandler::new("duplicate"))?
        .add_handler(TestHandler::new("duplicate"));

    assert!(result.is_err());
    Ok(())
}

async fn setup<H>(
    coordinator_handler: H,
    worker_handler: H,
) -> anyhow::Result<(DistributedAdapter<H>, WorkerRunHandle, TaskServerRunHandle)>
where
    H: DistributedHandler,
{
    let coordinator = Coordinator::new();
    let synchronizer = DistributedAdapter::new(coordinator_handler, coordinator.clone());
    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .with_shutdown_timeout(Duration::from_secs(1))
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let worker = Worker::builder(endpoint)
        .with_service_tasks_enabled(true)
        .with_polling_timeout(Duration::from_millis(20))
        .with_reconnect_delay(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_secs(1))
        .add_handler(worker_handler)?
        .build()?;
    Ok((synchronizer, worker.run(), server_handle))
}

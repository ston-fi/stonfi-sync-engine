use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::handler::{DistributedSyncHandler, TaskBatch, TaskPayload};
use stonfi_distributed_sync::synchronizer::DistributedSynchronizer;
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::worker::Worker;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_sync_core::mem_status_store::MemStatusStore;
use stonfi_sync_core::sync_engine::{
    Initiator, SyncEngine, SyncHandler, SyncHeight, SyncInitiator, SyncStatusStore, Synchronizer,
};

fn init_test_metrics() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;
    Ok(())
}

#[derive(Debug)]
struct TestTask(u8);

impl TaskPayload for TestTask {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        Ok(vec![self.0])
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        match data {
            [value] => Ok(Self(*value)),
            _ => Err(SyncCoreError::invalid_args("test task payload must contain exactly one byte")),
        }
    }
}

#[derive(Debug)]
struct TestResult(u8);

impl TaskPayload for TestResult {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        Ok(vec![self.0])
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        match data {
            [value] => Ok(Self(*value)),
            _ => Err(SyncCoreError::invalid_args("test result payload must contain exactly one byte")),
        }
    }
}

struct TestHandler {
    id: &'static str,
    service_task: bool,
    base_delay: Duration,
    create_delay: Duration,
    sync_timeout: Duration,
    active: AtomicUsize,
    max_active: AtomicUsize,
    retry_calls: AtomicUsize,
    results: Mutex<Vec<u8>>,
}

impl TestHandler {
    fn new(id: &'static str, service_task: bool, base_delay: Duration) -> Self {
        Self {
            id,
            service_task,
            base_delay,
            create_delay: Duration::ZERO,
            sync_timeout: Duration::from_secs(2),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            retry_calls: AtomicUsize::new(0),
            results: Mutex::new(Vec::new()),
        }
    }

    fn with_sync_timeout(mut self, sync_timeout: Duration) -> Self {
        self.sync_timeout = sync_timeout;
        self
    }

    fn with_create_delay(mut self, create_delay: Duration) -> Self {
        self.create_delay = create_delay;
        self
    }
}

#[async_trait::async_trait]
impl DistributedSyncHandler for TestHandler {
    type Task = TestTask;
    type TaskResult = TestResult;

    fn id(&self) -> &str {
        self.id
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn create_tasks(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        tokio::time::sleep(self.create_delay).await;
        Ok(Some(TaskBatch::new(to, vec![TestTask(1), TestTask(2), TestTask(3)])))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let multiplier = u32::from(4_u8.saturating_sub(task.0));
        tokio::time::sleep(self.base_delay.saturating_mul(multiplier)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(TestResult(task.0))
    }

    async fn handle_results(&self, _synced_height: SyncHeight, results: Vec<Self::TaskResult>) -> SyncCoreResult<()> {
        *self.results.lock() = results.into_iter().map(|result| result.0).collect();
        Ok(())
    }

    fn is_service_task(&self) -> bool {
        self.service_task
    }

    fn sync_timeout(&self) -> Duration {
        self.sync_timeout
    }

    fn retry_delay(&self) -> Duration {
        self.retry_calls.fetch_add(1, Ordering::SeqCst);
        Duration::from_millis(10)
    }
}

struct OneHeightInitiator;

#[async_trait::async_trait]
impl SyncInitiator for OneHeightInitiator {
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

struct EmptyBatchHandler {
    handled: AtomicBool,
}

#[async_trait::async_trait]
impl DistributedSyncHandler for EmptyBatchHandler {
    type Task = TestTask;
    type TaskResult = TestResult;

    fn id(&self) -> &str {
        "empty-batch"
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn create_tasks(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        Ok(Some(TaskBatch::new(to, Vec::new())))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        Ok(TestResult(task.0))
    }

    async fn handle_results(&self, _synced_height: SyncHeight, results: Vec<Self::TaskResult>) -> SyncCoreResult<()> {
        if !results.is_empty() {
            return Err(SyncCoreError::logic("empty batch produced results"));
        }
        self.handled.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct PanickingHandler;

#[async_trait::async_trait]
impl DistributedSyncHandler for PanickingHandler {
    type Task = TestTask;
    type TaskResult = TestResult;

    fn id(&self) -> &str {
        "panicking-handler"
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn create_tasks(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        Ok(Some(TaskBatch::new(to, vec![TestTask(1)])))
    }

    async fn process_task(&self, _task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        panic!("intentional worker join-failure test")
    }
}

#[tokio::test]
async fn test_regular_tasks_run_concurrently_and_results_stay_ordered() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = Arc::new(TestHandler::new("regular-ordering", false, Duration::from_millis(20)));
    let (mut synchronizer, worker, server) = setup(handler.clone(), 2).await?;

    assert_eq!(synchronizer.sync_range(1, 3).await?, Some(3),);
    assert_eq!(&*handler.results.lock(), &[1, 2, 3]);
    assert_eq!(handler.max_active.load(Ordering::SeqCst), 2);

    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_sync_engine_runs_through_server_and_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = Arc::new(TestHandler::new("engine-end-to-end", false, Duration::from_millis(1)));
    let (distributed, worker, server) = setup(handler.clone(), 2).await?;
    let initiator = Initiator::new(OneHeightInitiator);
    let status_store = Arc::new(MemStatusStore::new());
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(Synchronizer::new(distributed), &[&initiator])?
        .add_initiator(initiator)?
        .build()
        .run();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if status_store.load_synced_height(handler.id()).await? == Some(1) {
                return Ok::<(), SyncCoreError>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(&*handler.results.lock(), &[1, 2, 3]);

    engine.shutdown().await?;
    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_task_creation_uses_the_batch_deadline() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = Arc::new(
        TestHandler::new("slow-task-creation", false, Duration::from_millis(1))
            .with_create_delay(Duration::from_millis(60))
            .with_sync_timeout(Duration::from_millis(40)),
    );
    let (mut synchronizer, worker, server) = setup(handler.clone(), 1).await?;

    assert!(synchronizer.sync_range(1, 1).await.is_err());
    assert_eq!(handler.max_active.load(Ordering::SeqCst), 0);

    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_missing_handler_failure_is_retried_on_compatible_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let coordinator = Coordinator::new();
    let handler = Arc::new(TestHandler::new("retry-compatible", false, Duration::from_millis(1)));
    let mut synchronizer = DistributedSynchronizer::new(handler.clone(), coordinator.clone())?;
    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .with_shutdown_timeout(Duration::from_secs(1))
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server = server.run();
    let parallelism = NonZeroUsize::new(1).ok_or_else(|| anyhow::anyhow!("test parallelism must be positive"))?;
    let incompatible_handler = Arc::new(TestHandler::new("different-handler", false, Duration::from_millis(1)));
    let incompatible_worker = Worker::builder(endpoint.clone())
        .with_parallelism(parallelism)
        .with_polling_timeout(Duration::from_millis(20))
        .with_reconnect_delay(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_secs(1))
        .add_handler(incompatible_handler)?
        .build()?
        .run();
    let sync_task = tokio::spawn(async move { synchronizer.sync_range(1, 1).await });

    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.retry_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    incompatible_worker.shutdown().await?;

    let compatible_worker = Worker::builder(endpoint)
        .with_parallelism(parallelism)
        .with_polling_timeout(Duration::from_millis(20))
        .with_reconnect_delay(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_secs(1))
        .add_handler(handler.clone())?
        .build()?
        .run();

    assert_eq!(tokio::time::timeout(Duration::from_secs(1), sync_task).await???, Some(1));
    assert_eq!(&*handler.results.lock(), &[1, 2, 3]);

    compatible_worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_service_tasks_are_exclusive_on_the_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = Arc::new(TestHandler::new("service-exclusion", true, Duration::from_millis(10)));
    let (mut synchronizer, worker, server) = setup(handler.clone(), 2).await?;

    assert_eq!(synchronizer.sync_range(1, 3).await?, Some(3),);
    assert_eq!(&*handler.results.lock(), &[1, 2, 3]);
    assert_eq!(handler.max_active.load(Ordering::SeqCst), 1);

    worker.shutdown().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_empty_batch_advances_without_a_worker() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = Arc::new(EmptyBatchHandler {
        handled: AtomicBool::new(false),
    });
    let mut synchronizer = DistributedSynchronizer::new(handler.clone(), Coordinator::new())?;

    assert_eq!(synchronizer.sync_range(4, 7).await?, Some(7));
    assert!(handler.handled.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn test_worker_shutdown_interrupts_connection_backoff() -> anyhow::Result<()> {
    init_test_metrics()?;
    let handler = Arc::new(TestHandler::new("unavailable-server", false, Duration::from_millis(1)));
    let parallelism = NonZeroUsize::new(1).ok_or_else(|| anyhow::anyhow!("test parallelism must be positive"))?;
    let worker = Worker::builder("http://127.0.0.1:9")
        .with_parallelism(parallelism)
        .with_reconnect_delay(Duration::from_secs(5))
        .with_shutdown_timeout(Duration::from_millis(200))
        .add_handler(handler)?
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

    let handler = Arc::new(TestHandler::new("long-poll", false, Duration::from_millis(1)));
    let parallelism = NonZeroUsize::new(1).ok_or_else(|| anyhow::anyhow!("test parallelism must be positive"))?;
    let worker = Worker::builder(endpoint)
        .with_parallelism(parallelism)
        .with_polling_timeout(Duration::from_secs(5))
        .with_shutdown_timeout(Duration::from_millis(200))
        .add_handler(handler)?
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
    let handler = Arc::new(TestHandler::new("bounded-shutdown", false, Duration::from_millis(500)));
    let coordinator = Coordinator::new();
    let mut synchronizer = DistributedSynchronizer::new(handler.clone(), coordinator.clone())?;
    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .with_shutdown_timeout(Duration::from_secs(1))
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let parallelism = NonZeroUsize::new(1).ok_or_else(|| anyhow::anyhow!("test parallelism must be positive"))?;
    let worker = Worker::builder(endpoint)
        .with_parallelism(parallelism)
        .with_polling_timeout(Duration::from_millis(20))
        .with_reconnect_delay(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_millis(30))
        .add_handler(handler.clone())?
        .build()?;
    let worker_handle = worker.run();
    let sync_task = tokio::spawn(async move { synchronizer.sync_range(1, 3).await });

    tokio::time::timeout(Duration::from_secs(1), async {
        while handler.active.load(Ordering::SeqCst) == 0 {
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
    let handler = Arc::new(PanickingHandler);
    let (mut synchronizer, worker, server) = setup(handler, 2).await?;
    let sync_task = tokio::spawn(async move { synchronizer.sync_range(1, 1).await });

    let result = tokio::time::timeout(Duration::from_secs(1), worker.wait()).await?;
    assert!(result.is_err());

    sync_task.abort();
    let _ = sync_task.await;
    server.shutdown().await?;
    Ok(())
}

#[test]
fn test_registration_and_configuration_validation() -> anyhow::Result<()> {
    init_test_metrics()?;
    let parallelism = NonZeroUsize::new(1).ok_or_else(|| anyhow::anyhow!("test parallelism must be positive"))?;
    let handler = Arc::new(TestHandler::new("duplicate", false, Duration::from_millis(1)));

    assert!(
        Worker::builder("not a URI")
            .with_parallelism(parallelism)
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("https://127.0.0.1:1")
            .with_parallelism(parallelism)
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .with_polling_timeout(Duration::ZERO)
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .with_polling_timeout(Duration::from_nanos(1))
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .with_reconnect_delay(Duration::ZERO)
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .with_reconnect_delay(Duration::MAX)
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .with_shutdown_timeout(Duration::MAX)
            .add_handler(handler.clone())?
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .build()
            .is_err()
    );
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(parallelism)
            .add_handler(handler.clone())?
            .add_handler(handler)
            .is_err()
    );

    let blank = Arc::new(TestHandler::new(" ", false, Duration::from_millis(1)));
    assert!(DistributedSynchronizer::new(blank, Coordinator::new()).is_err());
    let zero_timeout =
        Arc::new(TestHandler::new("zero-timeout", false, Duration::from_millis(1)).with_sync_timeout(Duration::ZERO));
    assert!(DistributedSynchronizer::new(zero_timeout, Coordinator::new()).is_err());

    #[cfg(target_pointer_width = "64")]
    {
        let excessive_parallelism = NonZeroUsize::new(u32::MAX as usize + 1)
            .ok_or_else(|| anyhow::anyhow!("excessive parallelism must be positive"))?;
        let handler = Arc::new(TestHandler::new("parallelism", false, Duration::from_millis(1)));
        assert!(
            Worker::builder("http://127.0.0.1:1")
                .with_parallelism(excessive_parallelism)
                .add_handler(handler)?
                .build()
                .is_err()
        );
    }

    let excessive_parallelism = NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1)
        .ok_or_else(|| anyhow::anyhow!("excessive semaphore parallelism must be positive"))?;
    let handler = Arc::new(TestHandler::new("semaphore-parallelism", false, Duration::from_millis(1)));
    assert!(
        Worker::builder("http://127.0.0.1:1")
            .with_parallelism(excessive_parallelism)
            .add_handler(handler)?
            .build()
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn test_server_rejects_invalid_shutdown_timeout() -> anyhow::Result<()> {
    init_test_metrics()?;
    let address = "127.0.0.1:0".parse()?;
    assert!(TaskServer::builder(Coordinator::new()).build().await.is_err());
    assert!(
        TaskServer::builder(Coordinator::new())
            .with_listen_address(address)
            .with_shutdown_timeout(Duration::ZERO)
            .build()
            .await
            .is_err()
    );
    assert!(
        TaskServer::builder(Coordinator::new())
            .with_listen_address(address)
            .with_shutdown_timeout(Duration::MAX)
            .build()
            .await
            .is_err()
    );
    Ok(())
}

async fn setup<H>(
    handler: Arc<H>,
    parallelism: usize,
) -> anyhow::Result<(
    DistributedSynchronizer,
    stonfi_distributed_sync::worker::WorkerRunHandle,
    stonfi_distributed_sync::task_server::TaskServerRunHandle,
)>
where
    H: DistributedSyncHandler,
{
    let coordinator = Coordinator::new();
    let synchronizer = DistributedSynchronizer::new(handler.clone(), coordinator.clone())?;
    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .with_shutdown_timeout(Duration::from_secs(1))
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let parallelism =
        NonZeroUsize::new(parallelism).ok_or_else(|| anyhow::anyhow!("test parallelism must be positive"))?;
    let worker = Worker::builder(endpoint)
        .with_parallelism(parallelism)
        .with_service_tasks_enabled(true)
        .with_polling_timeout(Duration::from_millis(20))
        .with_reconnect_delay(Duration::from_millis(10))
        .with_shutdown_timeout(Duration::from_secs(1))
        .add_handler(handler)?
        .build()?;
    Ok((synchronizer, worker.run(), server_handle))
}

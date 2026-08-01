use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::handler::{DistributedSyncHandler, TaskBatch};
use stonfi_distributed_sync::synchronizer::DistributedSynchronizer;
use stonfi_distributed_sync::task::{EmptyTaskResult, RangeTask};
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::worker::Worker;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::mem_status_store::MemStatusStore;
use stonfi_sync_core::sync_engine::{Initiator, SyncEngine, SyncHeight, SyncInitiator, SyncStatusStore, Synchronizer};

struct OneHeightInitiator;

#[async_trait::async_trait]
impl SyncInitiator for OneHeightInitiator {
    fn id(&self) -> &str {
        "source"
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        if after == 0 {
            return Ok(1);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(after)
    }
}

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

    async fn create_tasks(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        Ok(Some(TaskBatch::new(to, vec![RangeTask { from, to }])))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        println!("processed inclusive range [{}, {}]", task.from, task.to);
        Ok(EmptyTaskResult)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;

    let coordinator = Coordinator::new();
    let handler = Arc::new(RangeHandler);
    let distributed = DistributedSynchronizer::new(handler.clone(), coordinator.clone())?;

    let server = TaskServer::builder(coordinator)
        .with_listen_address("127.0.0.1:0".parse()?)
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let parallelism = NonZeroUsize::new(2).ok_or_else(|| anyhow::anyhow!("parallelism must be positive"))?;
    let worker = Worker::builder(endpoint)
        .with_parallelism(parallelism)
        .with_polling_timeout(Duration::from_millis(100))
        .add_handler(handler)?
        .build()?;
    let worker_handle = worker.run();

    let initiator = Initiator::new(OneHeightInitiator);
    let status_store = Arc::new(MemStatusStore::new());
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(Synchronizer::new(distributed), &[&initiator])?
        .add_initiator(initiator)?
        .build();
    let engine_handle = engine.run();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if status_store.load_synced_height("range").await? == Some(1) {
                return Ok::<(), stonfi_sync_core::errors::SyncCoreError>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;

    engine_handle.shutdown().await?;
    worker_handle.shutdown().await?;
    server_handle.shutdown().await?;
    Ok(())
}

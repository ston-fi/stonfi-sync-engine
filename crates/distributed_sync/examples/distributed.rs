use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::task::{EmptyTaskResult, RangeTask};
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::traits::{DistributedHandler, TaskBatch};
use stonfi_distributed_sync::worker::Worker;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::mem_status_store::MemStatusStore;
use stonfi_sync_core::sync_engine::{HeightLoader, HeightProvider, SyncEngine, SyncHeight, SyncStatusStore};

struct OneHeightLoader;

#[async_trait::async_trait]
impl HeightLoader for OneHeightLoader {
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
impl DistributedHandler for RangeHandler {
    type Task = RangeTask;
    type TaskResult = EmptyTaskResult;

    fn id(&self) -> &str {
        "range"
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
    let server = TaskServer::builder(coordinator.clone())
        .with_listen_address("127.0.0.1:0".parse()?)
        .build()
        .await?;
    let endpoint = format!("http://{}", server.local_address());
    let server_handle = server.run();

    let worker = Worker::builder(endpoint)
        .with_polling_timeout(Duration::from_millis(100))
        .add_handler(RangeHandler)?
        .build()?;
    let worker_handle = worker.run();

    let height_provider = HeightProvider::new(OneHeightLoader);
    let status_store = Arc::new(MemStatusStore::new(0));
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(RangeHandler.into_sync(coordinator)?, &[&height_provider])?
        .add_height_provider(height_provider)?
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

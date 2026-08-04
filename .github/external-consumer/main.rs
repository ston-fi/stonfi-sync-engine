use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::worker::Worker;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::mem_progress_store::MemProgressStore;
use stonfi_sync_core::scylla_progress_store::ScyllaProgressStore;
use stonfi_sync_core::sync_engine::{HeightLoader, HeightProvider, SyncEngine, SyncHandler, SyncHeight};

struct Source;

#[async_trait::async_trait]
impl HeightLoader for Source {
    fn id(&self) -> &str {
        "external-source"
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        Ok(after)
    }
}

struct Handler;

#[async_trait::async_trait]
impl SyncHandler for Handler {
    fn id(&self) -> &str {
        "external-handler"
    }

    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(Some(to))
    }
}

fn main() -> SyncCoreResult<()> {
    let source = HeightProvider::new(Source);
    let _engine = SyncEngine::builder(Arc::new(MemProgressStore::new(0)))
        .with_shutdown_timeout(Duration::from_secs(1))
        .add_synchronizer(Handler, &[&source])?
        .add_height_provider(source)?
        .build();

    let coordinator = Coordinator::new();
    let _server = TaskServer::builder(coordinator);
    let _worker = Worker::builder("http://127.0.0.1:50051");
    let _scylla_store = ScyllaProgressStore::builder(0)
        .with_endpoints("127.0.0.1:9042")
        .with_keyspace("sync")
        .with_table_name("sync_progress");
    Ok(())
}

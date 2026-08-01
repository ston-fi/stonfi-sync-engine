use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::worker::Worker;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::mem_status_manager::MemStatusManager;
use stonfi_sync_core::sync_engine::{Initiator, SyncEngine, SyncHandler, SyncHeight, SyncInitiator, Synchronizer};

struct Source;

#[async_trait::async_trait]
impl SyncInitiator for Source {
    fn id(&self) -> &str {
        "external-source"
    }

    async fn last_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        Ok(after)
    }
}

struct Processor;

#[async_trait::async_trait]
impl SyncHandler for Processor {
    fn id(&self) -> &str {
        "external-processor"
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(Some(to))
    }
}

fn main() -> SyncCoreResult<()> {
    let source = Initiator::new(Source);
    let processor = Synchronizer::new(Processor);
    let _engine = SyncEngine::builder(Arc::new(MemStatusManager::new()))
        .with_shutdown_timeout(Duration::from_secs(1))?
        .add_sync(processor, &[&source])?
        .add_initiator(source)?
        .build();

    let coordinator = Coordinator::new();
    let _server = TaskServer::builder(coordinator);
    let _worker = Worker::builder("http://127.0.0.1:50051");
    Ok(())
}

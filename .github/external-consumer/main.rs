use std::cell::Cell;
use std::marker::PhantomPinned;
use std::sync::Arc;
use std::time::Duration;
use stonfi_distributed_sync::coordinator::Coordinator;
use stonfi_distributed_sync::task::RangeTask;
use stonfi_distributed_sync::task_server::TaskServer;
use stonfi_distributed_sync::traits::{DistributedHandler, TaskBatch, TaskPayload};
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

// Public handler and payload bounds must not require Clone, Unpin, or Sync payloads.
struct Distributed(PhantomPinned);
struct Payload(Cell<u64>);

impl TaskPayload for Payload {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        RangeTask {
            from: self.0.get(),
            to: self.0.get(),
        }
        .encode()
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        Ok(Self(Cell::new(RangeTask::decode(data)?.to)))
    }
}

#[async_trait::async_trait]
impl DistributedHandler for Distributed {
    type Task = Payload;
    type TaskResult = Payload;

    fn id(&self) -> &str {
        "external-distributed"
    }

    async fn create_tasks(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<TaskBatch<Self::Task>>> {
        Ok(Some(TaskBatch::new(to, vec![Payload(Cell::new(to))])))
    }

    async fn process_task(&self, task: Self::Task) -> SyncCoreResult<Self::TaskResult> {
        Ok(task)
    }
}

fn assert_adapter_bounds(_: &(impl SyncHandler + Sync + Unpin)) {}

fn main() -> SyncCoreResult<()> {
    let source = HeightProvider::new(Source);
    let coordinator = Coordinator::new();
    let adapter = Distributed(PhantomPinned).into_sync(coordinator.clone());
    assert_adapter_bounds(&adapter);
    let _engine = SyncEngine::builder(Arc::new(MemProgressStore::new(0)))
        .with_shutdown_timeout(Duration::from_secs(1))
        .add_synchronizer(Handler, &[&source])?
        .add_synchronizer(adapter, &[&source])?
        .add_height_provider(source)?
        .build();

    let _server = TaskServer::builder(coordinator);
    let _worker = Worker::builder("http://127.0.0.1:50051").add_handler(Distributed(PhantomPinned))?;
    let _scylla_store = ScyllaProgressStore::builder()
        .with_endpoints("127.0.0.1:9042")
        .with_keyspace("sync")
        .with_table_name("sync_progress");
    Ok(())
}

use std::sync::Arc;
use std::time::Duration;

use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::mem_status_store::MemStatusStore;
use stonfi_sync_core::sync_engine::{
    Initiator, SyncCallback, SyncEngine, SyncHandler, SyncHeight, SyncInitiator, SyncStatusStore, Synchronizer,
};
use tracing_subscriber::EnvFilter;

// A toy initiator that reveals a fixed target height one step at a time.
// In production this would usually poll a chain node, queue, or external API.
struct ExampleInitiator {
    id: &'static str,
    max_height: SyncHeight,
}

#[async_trait::async_trait]
impl SyncInitiator for ExampleInitiator {
    fn id(&self) -> &str {
        self.id
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(50)
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(std::cmp::min(after.saturating_add(1), self.max_height))
    }
}

// A toy handler that prints the ranges it processes and reports full progress.
// Replace this with your real range-processing logic. Handlers may also return
// `Ok(None)` to explicitly ignore an offered range.
struct ExampleHandler {
    id: &'static str,
}

#[async_trait::async_trait]
impl SyncHandler for ExampleHandler {
    fn id(&self) -> &str {
        self.id
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        tracing::info!("handler {} processed range [{from}, {to}]", self.id);
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(Some(to))
    }

    fn max_batch_size(&self) -> usize {
        2
    }
}

// Callbacks are the place for side effects such as logging, metrics forwarding,
// tracing, or notifications. They observe engine activity without owning sync logic.
struct ExampleCallback;

#[async_trait::async_trait]
impl SyncCallback for ExampleCallback {
    async fn on_initiator_next_height(
        &self,
        sync_id: &str,
        prev_height: SyncHeight,
        next_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        if next_height <= prev_height {
            return Ok(());
        }
        tracing::info!("callback: initiator {sync_id} advanced from {prev_height} to {next_height}");
        Ok(())
    }

    async fn on_sync_start(&self, sync_id: &str, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<()> {
        tracing::info!("callback: sync {sync_id} started range [{from}, {to}]");
        Ok(())
    }

    async fn on_sync_complete(
        &self,
        sync_id: &str,
        from: SyncHeight,
        to: SyncHeight,
        processed_to: SyncHeight,
    ) -> SyncCoreResult<()> {
        tracing::info!("callback: sync {sync_id} finished requested [{from}, {to}] and committed {processed_to}");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    stonfi_metrics::init_metrics!()?;

    // `MemStatusStore` keeps synced heights in memory. It is useful for tests,
    // examples, and ephemeral tools. It returns `None` until something is saved.
    let status_store = Arc::new(MemStatusStore::new());

    let initiator = Initiator::new(ExampleInitiator {
        id: "example_initiator",
        max_height: 5,
    });
    let synchronizer = Synchronizer::new(ExampleHandler { id: "example_sync" });

    // The builder wires together:
    // 1. the initiator that discovers new heights
    // 2. the synchronizer that processes them
    // 3. the callback that observes the lifecycle
    //
    // Passing `&initiator` makes the synchronizer depend on its progress. The
    // builder clones the progress receiver before taking ownership below.
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(synchronizer, &[&initiator])?
        .add_initiator(initiator)?
        .add_callback(Arc::new(ExampleCallback))
        .build();

    let run_handle = engine.run();

    // The engine spawns background tasks onto the Tokio runtime. This example
    // sleeps for a short bounded time so the demo can finish deterministically.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Explicit shutdown signals every engine-owned wait and confirms that all
    // tasks have exited. Dropping the handle also signals shutdown, but does
    // not wait for completion.
    run_handle.shutdown().await?;

    let final_height = status_store.load_synced_height("example_sync").await?;
    tracing::info!("final synced height: {}", final_height.unwrap_or_default());
    tracing::info!("swap the toy initiator/handler with real implementations to build your service.");

    Ok(())
}

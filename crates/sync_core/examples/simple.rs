use std::sync::Arc;
use std::time::Duration;

use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::mem_progress_store::MemProgressStore;
use stonfi_sync_core::sync_engine::{
    HeightLoader, HeightProvider, SyncCallback, SyncEngine, SyncHandler, SyncHeight, SyncProgressStore,
};
use tracing_subscriber::EnvFilter;

// A toy height loader that reveals a fixed target height one step at a time.
// In production this would usually poll a chain node, queue, or external API.
struct ExampleHeightLoader {
    id: &'static str,
    max_height: SyncHeight,
}

#[async_trait::async_trait]
impl HeightLoader for ExampleHeightLoader {
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
    async fn on_height_loaded(
        &self,
        handler_id: &str,
        prev_height: SyncHeight,
        next_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        if next_height <= prev_height {
            return Ok(());
        }
        tracing::info!("callback: height provider {handler_id} advanced from {prev_height} to {next_height}");
        Ok(())
    }

    async fn on_sync_start(&self, handler_id: &str, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<()> {
        tracing::info!("callback: sync {handler_id} started range [{from}, {to}]");
        Ok(())
    }

    async fn on_sync_complete(
        &self,
        handler_id: &str,
        from: SyncHeight,
        to: SyncHeight,
        processed_to: SyncHeight,
    ) -> SyncCoreResult<()> {
        tracing::info!("callback: sync {handler_id} finished requested [{from}, {to}] and committed {processed_to}");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    stonfi_metrics::init_metrics!()?;

    // `MemProgressStore` keeps synced heights in memory. The constructor's `0`
    // configures the engine-wide fallback that is stored under `INITIAL` when
    // the first synchronizer starts without persisted progress.
    let progress_store = Arc::new(MemProgressStore::new(0));

    let height_provider = HeightProvider::new(ExampleHeightLoader {
        id: "example_height_source",
        max_height: 5,
    });
    // The builder wires together:
    // 1. the height provider that discovers new heights
    // 2. the synchronizer that processes them
    // 3. the callback that observes the lifecycle
    //
    // Passing `&height_provider` makes the synchronizer depend on its progress. The
    // builder clones the progress receiver before taking ownership below.
    let engine = SyncEngine::builder(progress_store.clone())
        .add_synchronizer(ExampleHandler { id: "example_sync" }, &[&height_provider])?
        .add_height_provider(height_provider)?
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

    let final_height = progress_store.load_synced_height("example_sync").await?;
    tracing::info!("final synced height: {}", final_height.unwrap_or_default());
    tracing::info!("swap the toy height loader/handler with real implementations to build your service.");

    Ok(())
}

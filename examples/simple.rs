use std::sync::Arc;
use std::time::Duration;

use env_logger::Env;
use stonfi_sync_core::{
    MemStatusManager, SyncCallback, SyncCoreResult, SyncEngine, SyncHandler, SyncHeight, SyncID, SyncInitiator,
    SyncStatusManager,
};

// A toy initiator that reveals a fixed target height one step at a time.
// In production this would usually poll a chain node, queue, or external API.
struct ExampleInitiator {
    id: SyncID,
    max_height: SyncHeight,
}

#[async_trait::async_trait]
impl SyncInitiator for ExampleInitiator {
    fn id(&self) -> &SyncID {
        &self.id
    }

    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(50)
    }

    async fn last_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(std::cmp::min(after.saturating_add(1), self.max_height))
    }
}

// A toy handler that prints the ranges it processes and reports full progress.
// Replace this with your real range-processing logic. Handlers may also return
// `Ok(None)` to explicitly ignore an offered range.
struct ExampleHandler {
    id: SyncID,
}

#[async_trait::async_trait]
impl SyncHandler for ExampleHandler {
    fn id(&self) -> &SyncID {
        &self.id
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        log::info!("handler {} processed range [{from}, {to}]", self.id);
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok(Some(to))
    }

    fn max_sync_range(&self) -> usize {
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
        sync_id: &SyncID,
        prev_height: SyncHeight,
        next_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        if next_height <= prev_height {
            return Ok(());
        }
        log::info!("callback: initiator {sync_id} advanced from {prev_height} to {next_height}");
        Ok(())
    }

    async fn on_sync_start(&self, sync_id: &SyncID, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<()> {
        log::info!("callback: sync {sync_id} started range [{from}, {to}]");
        Ok(())
    }

    async fn on_sync_complete(
        &self,
        sync_id: &SyncID,
        from: SyncHeight,
        to: SyncHeight,
        real_to: SyncHeight,
    ) -> SyncCoreResult<()> {
        log::info!("callback: sync {sync_id} finished requested [{from}, {to}] and committed {real_to}");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // `MemStatusManager` keeps synced heights in memory. It is useful for tests,
    // examples, and ephemeral tools. It returns `None` until something is saved.
    let status_manager = Arc::new(MemStatusManager::new());

    let initiator = stonfi_sync_core::Initiator::new(ExampleInitiator {
        id: "example_initiator".to_string(),
        max_height: 5,
    });
    let synchronizer = stonfi_sync_core::Synchronizer::new(ExampleHandler {
        id: "example_sync".to_string(),
    });

    // The builder wires together:
    // 1. the initiator that discovers new heights
    // 2. the synchronizer that processes them
    // 3. the callback that observes the lifecycle
    //
    // `add_sync(..., &[&initiator])` means the synchronizer should only advance
    // when this initiator publishes a higher completed height.
    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(synchronizer, &[&initiator])?
        .add_initiator(initiator)?
        .add_callback(Arc::new(ExampleCallback))?
        .build();

    let run_handle = engine.run();

    // The engine spawns background tasks onto the Tokio runtime. This example
    // sleeps for a short bounded time so the demo can finish deterministically.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Dropping the engine drops the shared parent handle used by all tasks as a
    // shutdown marker. This is why the example exits cleanly without custom
    // shutdown plumbing.
    drop(engine);
    run_handle.wait().await;

    let final_height = status_manager.load_synced_height(&"example_sync".to_string()).await?;
    log::info!("final synced height: {}", final_height.unwrap_or_default());
    log::info!("swap the toy initiator/handler with real implementations to build your service.");

    Ok(())
}

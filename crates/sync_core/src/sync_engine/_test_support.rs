use super::*;
use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::traits::{HeightLoader, ProgressProvider, SyncHandler};
use parking_lot::RwLock;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static TRACING_INIT: std::sync::Once = std::sync::Once::new();
pub(crate) fn init_test_runtime() -> anyhow::Result<()> {
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .try_init();
    });
    stonfi_metrics::init_metrics!()?;
    Ok(())
}

// `Cell` keeps the common fixtures `Send` but not `Sync`, so regular workflow
// tests also guard the engine's task-local ownership contract.
pub(super) struct TestHeightLoader {
    id: String,
    delay: Duration,
    polls: Cell<usize>,
}
impl TestHeightLoader {
    pub(super) fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
            polls: Cell::new(0),
        }
    }
}

#[async_trait::async_trait]
impl HeightLoader for TestHeightLoader {
    fn id(&self) -> &str {
        &self.id
    }
    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        self.polls.set(self.polls.get() + 1);
        tokio::time::sleep(self.delay).await;
        Ok(after + 1)
    }
}

pub(super) struct TestSync {
    id: String,
    delay: Duration,
    ranges: Cell<usize>,
}
impl TestSync {
    pub(super) fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
            ranges: Cell::new(0),
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSync {
    fn id(&self) -> &str {
        &self.id
    }
    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.ranges.set(self.ranges.get() + 1);
        tokio::time::sleep(self.delay).await;
        Ok(Some(to))
    }
}

pub(super) struct TestStatusStore {
    initial_synced_height: SyncHeight,
    pub(super) initial_save_failures: AtomicUsize,
    pub(super) storage: RwLock<HashMap<String, Vec<SyncHeight>>>,
}

impl TestStatusStore {
    pub fn new(initial_synced_height: SyncHeight) -> Self {
        Self {
            initial_synced_height,
            initial_save_failures: AtomicUsize::new(0),
            storage: RwLock::new(HashMap::new()),
        }
    }

    pub(super) fn with_initial_save_failures(mut self, failures: usize) -> Self {
        self.initial_save_failures = AtomicUsize::new(failures);
        self
    }
}

#[async_trait::async_trait]
impl SyncStatusStore for TestStatusStore {
    fn initial_synced_height(&self) -> SyncHeight {
        self.initial_synced_height
    }

    async fn save_synced_height(&self, handler_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()> {
        if handler_id == INITIAL_HEIGHT
            && self
                .initial_save_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                .is_ok()
        {
            return Err(SyncCoreError::custom("configured initial save failure"));
        }
        self.storage.write().entry(handler_id.to_owned()).or_default().push(sync_height);
        Ok(())
    }

    async fn load_synced_height(&self, handler_id: &str) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(self.storage.read().get(handler_id).and_then(|heights| heights.last().copied()))
    }
}

pub(super) struct TestProgressProvider(pub(super) ProgressReceiver);

impl ProgressProvider for TestProgressProvider {
    fn subscribe(&self) -> ProgressReceiver {
        self.0.clone()
    }
}

pub(super) async fn shutdown_engine_after(run_handle: RunHandle, run_for: Duration) -> anyhow::Result<()> {
    tokio::time::sleep(run_for).await;
    run_handle.shutdown().await?;
    Ok(())
}

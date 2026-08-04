use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::SyncHeight;
use crate::sync_engine::progress::ProgressReceiver;
use std::time::Duration;

/// Reserved initial-height key in the handler ID namespace.
pub const INITIAL_HEIGHT: &str = "INITIAL";

/// Loads the latest upstream height.
///
/// IDs must be stable and unique across registered handlers.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait HeightLoader: Send + 'static {
    /// Returns the stable handler ID.
    fn id(&self) -> &str;
    /// Returns the delay after no progress or a loader or callback failure.
    fn retry_delay(&self) -> Duration { Duration::from_millis(200) }
    /// Returns the latest height; values not above `after` mean no progress.
    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight>;
}

/// Processes inclusive height ranges.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncHandler: Send + 'static {
    /// Returns the stable handler ID.
    fn id(&self) -> &str;
    /// Processes the inclusive range `[from, to]`.
    ///
    /// The range and batch size are valid. `Ok(None)` ignores the range without
    /// persistence or publication. `Ok(Some(height))` commits a height in the
    /// range, or a lower height when [`Self::allow_rewind`] is enabled.
    ///
    /// Calls are at-least-once and passed to [`tokio::time::timeout`] with
    /// [`Self::sync_timeout`]; effects must therefore be idempotent,
    /// cancellation-safe, or transactional.
    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>>;

    /// Returns whether new ranges may be processed.
    fn is_enabled(&self) -> bool { true }
    /// Returns the retry delay for this handler and related operations.
    fn retry_delay(&self) -> Duration { Duration::from_millis(200) }
    /// Returns the minimum batch size.
    fn min_batch_size(&self) -> usize { 1 }
    /// Returns the maximum batch size.
    fn max_batch_size(&self) -> usize { 1 }
    /// Returns the duration passed to [`tokio::time::timeout`] for [`Self::sync_range`].
    fn sync_timeout(&self) -> Duration { Duration::from_secs(10) }
    /// Allows [`Self::sync_range`] to rewind below `from`.
    fn allow_rewind(&self) -> bool { false }
}

/// Persists handler progress and the application-provided initial height.
///
/// Only one writer may update each handler ID or [`INITIAL_HEIGHT`] key; this
/// trait does not coordinate concurrent writers.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncProgressStore: Send + Sync + 'static {
    /// Stores the latest synced height for `handler_id`.
    async fn save_synced_height(&self, handler_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()>;
    /// Loads the persisted height for `handler_id`.
    async fn load_synced_height(&self, handler_id: &str) -> SyncCoreResult<Option<SyncHeight>>;
    /// Stores the application-provided [`INITIAL_HEIGHT`] value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails.
    async fn save_initial_height(&self, initial_height: SyncHeight) -> SyncCoreResult<()> {
        self.save_synced_height(INITIAL_HEIGHT, initial_height).await
    }
    /// Loads the persisted [`INITIAL_HEIGHT`] value.
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails.
    async fn load_initial_height(&self) -> SyncCoreResult<Option<SyncHeight>> {
        self.load_synced_height(INITIAL_HEIGHT).await
    }
    /// Loads `handler_id`, falling back to the persisted [`INITIAL_HEIGHT`].
    ///
    /// # Errors
    ///
    /// Returns an error when storage access fails. Returns
    /// [`SyncCoreError::Logic`] when the application has not initialized
    /// [`INITIAL_HEIGHT`].
    async fn load_synced_or_initial(&self, handler_id: &str) -> SyncCoreResult<SyncHeight> {
        if let Some(sync_height) = self.load_synced_height(handler_id).await? {
            return Ok(sync_height);
        }
        if let Some(initial_height) = self.load_initial_height().await? {
            return Ok(initial_height);
        }

        Err(SyncCoreError::logic(format!(
            "initial height is not initialized; call SyncProgressStore::save_initial_height before loading {handler_id:?}"
        )))
    }
}

/// Provides progress subscriptions.
pub trait ProgressProvider {
    /// Subscribes to the provider's latest published height.
    ///
    /// Receivers start at the current value and may coalesce updates. Upstream
    /// rewinds affect subsequent waits but do not cancel already selected work.
    fn subscribe(&self) -> ProgressReceiver;
}

/// Receives notifications about engine progress and failures.
///
/// Failures retry while the engine is active. Earlier callbacks may replay, so
/// implementations must be idempotent. Delivery is process-local.
///
/// `handler_id` identifies the height loader or sync handler that emitted the
/// event.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncCallback: Send + Sync + 'static {
    /// Called when `HeightLoader::latest_height()` returns an error.
    /// `height` is the last height passed to it as `after`.
    async fn on_height_load_error(&self, _handler_id: &str, _height: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after each successful height load, including the initial load.
    async fn on_height_loaded(&self, _handler_id: &str, _prev_height: SyncHeight, _loaded_height: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after each nonzero height publication.
    async fn on_height_published(&self, _handler_id: &str, _prev_height: SyncHeight, _published_height: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called before processing `[from, to]`.
    async fn on_sync_start(&self, _handler_id: &str, _from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after a range fails, times out, or returns an invalid height.
    async fn on_sync_error(&self, _handler_id: &str, _from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after the synchronizer saves the new synced height.
    ///
    /// `to` is the offered bound and `processed_to` is the committed height.
    /// Callback failure does not roll back the saved height.
    async fn on_sync_complete(&self, _handler_id: &str, _from: SyncHeight, _to: SyncHeight, _processed_to: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
}

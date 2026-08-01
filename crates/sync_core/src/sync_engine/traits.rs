use crate::errors::SyncCoreResult;
use crate::sync_engine::SyncHeight;
use crate::sync_engine::multi_receiver::SyncReceiver;
use std::time::Duration;

/// Produces the latest height that dependent synchronizers may process.
///
/// `id()` must be stable for the lifetime of the engine and unique among all
/// registered initiators and synchronizers that share metrics labels.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncInitiator: Send + 'static {
    /// Returns the stable identifier used in logs and metrics.
    fn id(&self) -> &str;
    /// Returns the backoff used after `last_height()` or callback failures.
    fn sleep_on_error(&self) -> Duration { Duration::from_millis(200) }
    /// Returns the latest known height relative to `after`.
    ///
    /// Returning a height less than or equal to `after` is allowed, but the
    /// engine will treat it as "no progress", sleep for `sleep_on_error()`,
    /// and poll again.
    async fn last_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight>;
}

/// Processes a contiguous inclusive range of heights.
///
/// Called by the engine when its dependency wait admits new progress.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncHandler: Send + 'static {
    /// Returns the stable identifier used in logs, metrics, and status storage.
    fn id(&self) -> &str;
    /// Returns the starting synced height used when no persisted height exists.
    ///
    /// This value is an in-memory fallback owned by the handler. The engine
    /// does not persist it until a successful sync saves a new height.
    fn initial_synced_height(&self) -> SyncHeight;
    /// Processes the inclusive range `[from, to]`.
    ///
    /// The engine guarantees `from <= to` and, when the method is called,
    /// `min_sync_range() <= (to - from + 1) <= max_sync_range()`.
    ///
    /// Return `Ok(None)` to explicitly ignore the offered range. The engine
    /// will skip persistence, completion callbacks, and downstream progress
    /// publication for that range.
    ///
    /// Return `Ok(Some(height))` to report the highest height durably processed
    /// by the handler. `height` must be within `[from, to]`, except when
    /// `allow_wrap()` is enabled, in which case returning `Some(height < from)`
    /// is reserved for wrap behavior.
    ///
    /// Calls are at-least-once. The engine cancels this future when
    /// [`Self::sync_timeout`] elapses and retries the same range after errors or
    /// timeouts. Implementations must make externally visible effects
    /// idempotent, cancellation-safe, or transactional.
    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>>;

    /// Returns whether the handler is currently allowed to process new ranges.
    fn is_enabled(&self) -> bool { true }
    /// Returns the backoff used after handler, callback, or status-manager
    /// failures associated with this synchronizer.
    fn sleep_on_error(&self) -> Duration { Duration::from_millis(200) }
    /// Returns the minimum inclusive range length the handler can process.
    fn min_sync_range(&self) -> usize { 1 }
    /// Returns the maximum inclusive range length the handler can process.
    fn max_sync_range(&self) -> usize { 1 }
    /// Returns the timeout for a single `sync_range()` call.
    fn sync_timeout(&self) -> Duration { Duration::from_secs(10) }
    /// Allows [`Self::sync_range`] to return `Some(height < from)` without treating
    /// it as an error.
    fn allow_wrap(&self) -> bool { false }
}

/// Persists and restores the latest synced height for handlers.
///
/// The engine retries both methods while it remains active.
/// A given sync ID must have only one active engine writer; this interface does
/// not provide compare-and-set semantics for multi-process coordination.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncStatusManager: Send + Sync + 'static {
    /// Stores the latest durably synced height for `sync_id`.
    async fn save_synced_height(&self, sync_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()>;
    /// Loads the latest persisted synced height for `sync_id`.
    ///
    /// Returns `Ok(None)` when the sync has not been persisted yet.
    async fn load_synced_height(&self, sync_id: &str) -> SyncCoreResult<Option<SyncHeight>>;
}

/// Exposes a progress stream that other synchronizers can depend on.
pub trait SyncTrigger {
    /// Returns a watch receiver that publishes completed heights.
    ///
    /// Trigger values may decrease when an upstream synchronizer wraps. A
    /// decrease does not rewind dependants or cancel forward progress already
    /// selected by an active dependency wait. Each dependant decides whether
    /// to wrap through its own [`SyncHandler::allow_wrap`] behavior. Subsequent
    /// waits observe the trigger's current value.
    fn receiver(&self) -> SyncReceiver;
}

/// Receives notifications about engine progress and failures.
///
/// The builder does not deduplicate callback instances. The engine retries
/// callback failures with the same backoff as the owning initiator or handler
/// while the engine remains active. Callbacks must be idempotent because, when
/// several callbacks are registered, a later callback failure can replay
/// earlier ones. Delivery is not persisted and is not guaranteed across
/// shutdown, process failure, or restart.
#[rustfmt::skip]
#[async_trait::async_trait]
pub trait SyncCallback: Send + Sync + 'static {
    /// Called when `SyncInitiator::last_height()` returns an error.
    async fn on_initiator_error(&self, _sync_id: &str, _height: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after an initiator fetched a candidate next height.
    async fn on_initiator_next_height(&self, _sync_id: &str, _prev_height: SyncHeight, _next_height: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after an initiator publishes a new height to its subscribers.
    async fn on_initiator_sent(&self, _sync_id: &str, _prev_height: SyncHeight, _sent_height: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called before a synchronizer starts processing the inclusive range
    /// `[from, to]`.
    async fn on_sync_start(&self, _sync_id: &str, _from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after `sync_range()` fails, times out, or returns an invalid
    /// height.
    async fn on_sync_error(&self, _sync_id: &str, _from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
    /// Called after the synchronizer saves the new synced height.
    ///
    /// A callback failure does not roll back the saved height. Retries are
    /// process-local and stop when the engine shuts down.
    async fn on_sync_complete(&self, _sync_id: &str, _from: SyncHeight, _to: SyncHeight, _real_to: SyncHeight) -> SyncCoreResult<()> { Ok(()) }
}

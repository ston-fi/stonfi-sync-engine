use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::metrics::{SyncEngineMetrics, SyncPhase};
use crate::sync_engine::progress::{MultiReceiver, ProgressReceiver, ProgressSender};
use crate::sync_engine::traits::ProgressProvider;
use crate::sync_engine::{SyncHandler, SyncHeight, SyncStatusStore, sleep_or_cancelled};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

const SLEEP_IF_DISABLED: Duration = Duration::from_secs(1);

pub(super) struct SyncCtx {
    pub receiver: MultiReceiver,
    pub cancellation: CancellationToken,
    pub status_store: Arc<dyn SyncStatusStore>,
    pub callbacks: Arc<CallbackStore>,
    pub log_progress: fn(SyncHeight, SyncHeight) -> bool,
}

/// Wraps a [`SyncHandler`] so the engine can schedule ranges and publish
/// downstream progress.
pub struct Synchronizer {
    pub(super) handler: Box<dyn SyncHandler>,
    sender: ProgressSender,
    receiver: ProgressReceiver,
}

impl Synchronizer {
    /// Wraps a handler implementation for registration with a sync engine.
    pub fn new(handler: impl SyncHandler) -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(0);
        Self {
            handler: Box::new(handler),
            sender,
            receiver,
        }
    }

    fn publish_height(&self, log_prefix: &str, height: SyncHeight) -> bool {
        if self.sender.send(height).is_ok() {
            return true;
        }
        tracing::warn!("[{log_prefix}] failed to publish height {height}: receiver channel is closed");
        false
    }

    #[rustfmt::skip]
    pub(super) async fn run(mut self, mut ctx: SyncCtx) {
        let id = self.handler.id().to_owned();
        let log_prefix = format!("SYNC][{id}");

        let Some(mut synced_height) = self.load_synced_or_initial_loop(&ctx, &log_prefix, &id).await else {
            tracing::info!("[{log_prefix}] finished: shutdown requested");
            return;
        };

        if ctx.cancellation.is_cancelled() {
            tracing::info!("[{log_prefix}] finished: shutdown requested");
            return;
        }

        tracing::info!("[{log_prefix}] started with synced_height: {synced_height}");
        // Publish initial progress so dependants need not wait for a new height.
        if !self.publish_height(&log_prefix, synced_height) {
            return;
        }

        let mut wait_after_height = synced_height;
        loop {
            if ctx.cancellation.is_cancelled() {
                break;
            }

            let next_height = tokio::select! {
                biased;
                _ = ctx.cancellation.cancelled() => break,
                height = ctx.receiver.wait_after(wait_after_height) => height,
            };
            let next_height = match next_height {
                Some(height) => height,
                None => break, // sender was closed
            };

            let start_ts = Instant::now();

            if !self.handler.is_enabled() {
                tracing::debug!("[{log_prefix}] range [{synced_height}, {next_height}]: skipped (sync is disabled)");
                if sleep_or_cancelled(&ctx.cancellation, SLEEP_IF_DISABLED).await {
                    break;
                }
                continue;
            }


            let sync_from = synced_height + 1;
            let Some(sync_to) = self.calc_sync_to(sync_from, next_height) else {
                tracing::debug!("[{log_prefix}] range [{sync_from}, {next_height}]: skipped (range mismatch)");
                wait_after_height = next_height; // do nothing
                continue;
            };

            if !ctx.callbacks
                .on_sync_start_loop(&id, sync_from, sync_to, self.handler.retry_delay())
                .await
            {
                break;
            }

            let range_log_prefix_expected =
                format!("{log_prefix}] sync [{sync_from}, {next_height}] -> [{sync_from}, {sync_to}");

            let Some(new_synced_height) = self
                .sync_range_loop(&ctx, &range_log_prefix_expected, &id, sync_from, sync_to)
                .await
            else {
                if ctx.cancellation.is_cancelled() {
                    break;
                }
                tracing::debug!("[{log_prefix}] range [{sync_from}, {sync_to}]: skipped (ignored)");
                wait_after_height = next_height;
                continue;
            };

            let range_log_prefix_actual =
                format!("{log_prefix}] sync [{sync_from}, {next_height}] -> [{sync_from}, {new_synced_height}");

            if !self
                .save_synced_height_loop(&ctx, &range_log_prefix_actual, &id, new_synced_height)
                .await
            {
                break;
            }
            if !ctx.callbacks
                .on_sync_complete_loop(
                    &id,
                    sync_from,
                    sync_to,
                    new_synced_height,
                    self.handler.retry_delay(),
                )
                .await
            {
                break;
            }

            if !self.publish_height(&log_prefix, new_synced_height) {
                break;
            }
            synced_height = new_synced_height;
            wait_after_height = synced_height;
            let sync_duration = start_ts.elapsed();
            if new_synced_height < sync_from {
                SyncEngineMetrics::update_synced_height(&id, new_synced_height);
                tracing::info!("[{range_log_prefix_actual}]: rewound to {new_synced_height} ({sync_duration:.3?})");
            } else {
                SyncEngineMetrics::update_sync(&id, sync_from, synced_height, sync_duration);
                let synced_range = new_synced_height - sync_from + 1;
                if (ctx.log_progress)(sync_from, sync_to) {
                    tracing::info!("[{range_log_prefix_actual}]: done ({synced_range} heights, {sync_duration:.3?})");
                } else {
                    tracing::debug!("[{range_log_prefix_actual}]: done ({synced_range} heights, {sync_duration:.3?})");
                }
            }
        }
        tracing::info!("[{log_prefix}] finished: shutdown requested")
    }

    pub(super) fn calc_sync_to(&self, from: SyncHeight, to: SyncHeight) -> Option<SyncHeight> {
        if to < from {
            return None;
        }
        let available_range_size = to - from + 1;
        let min_batch_size = SyncHeight::try_from(self.handler.min_batch_size()).ok()?;
        let max_batch_size = SyncHeight::try_from(self.handler.max_batch_size()).ok()?;
        if min_batch_size == 0 || max_batch_size == 0 || available_range_size < min_batch_size {
            return None;
        }
        let range_size = std::cmp::min(available_range_size, max_batch_size);
        from.checked_add(range_size.checked_sub(1)?)
    }

    #[rustfmt::skip]
    async fn sync_range_loop(
        &mut self,
        ctx: &SyncCtx,
        log_prefix: &str,
        id: &str,
        from: SyncHeight,
        to: SyncHeight,
    ) -> Option<SyncHeight> {
        let sync_timeout = self.handler.sync_timeout();
        loop {
            if ctx.cancellation.is_cancelled() {
                return None;
            }
            let start_ts = Instant::now();

            match tokio::time::timeout(sync_timeout, self.handler.sync_range(from, to)).await {
                Ok(Ok(None)) => return None,
                Ok(Ok(Some(synced_height))) if synced_height < from && self.handler.allow_rewind() => return Some(synced_height),
                Ok(Ok(Some(synced_height))) if (from..=to).contains(&synced_height) => return Some(synced_height),
                Ok(Ok(Some(synced_height))) if synced_height > to => {
                    tracing::warn!(
                        "[{log_prefix}] Got invalid synced height: {synced_height}, expected in range [{from}, {to}] ({:.3?}). Retrying...",
                        start_ts.elapsed()
                    );
                },
                Ok(Ok(Some(synced_height))) => {
                    tracing::warn!(
                        "[{log_prefix}] Got invalid rewound synced height: {synced_height}, expected in range [{from}, {to}] or lower only when allow_rewind() is enabled ({:.3?}). Retrying...",
                        start_ts.elapsed()
                    );
                },
                Ok(Err(err)) => {
                    tracing::warn!("[{log_prefix}] Got error: {err} ({:.3?}). Retrying...", start_ts.elapsed());
                },
                Err(_) => {
                    tracing::warn!("[{log_prefix}] Timed out after {sync_timeout:.3?}. Retrying...");
                },
            }
            if !ctx
                .callbacks
                .on_sync_error_loop(id, from, to, self.handler.retry_delay())
                .await
            {
                return None;
            }
            SyncEngineMetrics::inc_retries(id, SyncPhase::SyncRange);
            if sleep_or_cancelled(&ctx.cancellation, self.handler.retry_delay()).await {
                return None;
            }
        }
    }

    async fn load_synced_or_initial_loop(&mut self, ctx: &SyncCtx, log_prefix: &str, id: &str) -> Option<SyncHeight> {
        loop {
            if ctx.cancellation.is_cancelled() {
                return None;
            }
            match ctx.status_store.load_synced_or_initial(id).await {
                Ok(height) => return Some(height),
                Err(err) => {
                    tracing::warn!("[{log_prefix}] .load_synced_or_initial() returns error: {err}. Retrying...");
                    SyncEngineMetrics::inc_retries(id, SyncPhase::LoadHeight);
                    if sleep_or_cancelled(&ctx.cancellation, self.handler.retry_delay()).await {
                        return None;
                    }
                },
            }
        }
    }

    async fn save_synced_height_loop(&mut self, ctx: &SyncCtx, log_prefix: &str, id: &str, height: SyncHeight) -> bool {
        loop {
            if ctx.cancellation.is_cancelled() {
                return false;
            }
            if let Err(err) = ctx.status_store.save_synced_height(id, height).await {
                tracing::warn!("[{log_prefix}] .save_synced_height({height}) returns error: {err}. Retrying...");
                SyncEngineMetrics::inc_retries(id, SyncPhase::SaveHeight);
                if sleep_or_cancelled(&ctx.cancellation, self.handler.retry_delay()).await {
                    return false;
                }
                continue;
            }
            return true;
        }
    }
}

impl ProgressProvider for Synchronizer {
    fn subscribe(&self) -> ProgressReceiver {
        self.receiver.clone()
    }
}

impl<T: SyncHandler> From<T> for Synchronizer {
    fn from(handler: T) -> Self {
        Self::new(handler)
    }
}

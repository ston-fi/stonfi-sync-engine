use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::colors::*;
use crate::sync_engine::metrics::{SyncEngineMetrics, SyncPhase};
use crate::sync_engine::multi_receiver::{MultiReceiver, SyncReceiver, SyncSender};
use crate::sync_engine::traits::SyncTrigger;
use crate::sync_engine::{Inner, SyncHeight, SyncStatusManager};
use crate::{SyncCallback, SyncHandler};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

const SLEEP_IF_DISABLED: Duration = Duration::from_secs(1);

pub(super) struct SyncCtx {
    pub receiver: MultiReceiver,
    pub parent: Weak<Inner>,
    pub status_manager: Arc<dyn SyncStatusManager>,
    pub callbacks: Arc<CallbackStore>,
    pub metrics: Arc<SyncEngineMetrics>,
    pub log_progress: fn(SyncHeight, SyncHeight) -> bool,
}

/// Wraps a [`SyncHandler`] so the engine can schedule ranges and publish
/// downstream progress.
pub struct Synchronizer {
    pub(super) handler: Box<dyn SyncHandler>,
    tx_rx: (SyncSender, SyncReceiver),
}

impl Synchronizer {
    /// Wraps a handler implementation for registration with a sync engine.
    pub fn new(handler: impl SyncHandler) -> Self {
        Self {
            handler: Box::new(handler),
            tx_rx: tokio::sync::watch::channel(0),
        }
    }

    fn publish_height(&self, log_prefix: &str, height: SyncHeight) -> bool {
        if self.tx_rx.0.send(height).is_ok() {
            return true;
        }
        log::warn!("[{log_prefix}] failed to publish height {height}: receiver channel is closed");
        false
    }

    #[rustfmt::skip]
    pub(super) async fn run(mut self, mut ctx: SyncCtx) {
        let sync_id = self.handler.id().to_owned();
        let log_prefix = format!("{COLOR_PINK}SYNC{COLOR_RESET}][{COLOR_PINK}{sync_id}{COLOR_RESET}");

        let mut synced_height = match self.load_synced_height_loop(&ctx, &log_prefix).await {
            Some(height) => height,
            None => {
                let initial_synced_height = self.handler.initial_synced_height();
                log::info!("[{log_prefix}] no persisted synced height, using initial: {COLOR_GREEN}{initial_synced_height}{COLOR_RESET}");
                initial_synced_height
            },
        };

        if ctx.parent.upgrade().is_none() {
            log::info!("[{log_prefix}] {COLOR_GREEN}finished{COLOR_RESET}: parent is dropped");
            return;
        }

        log::info!("[{log_prefix}] started with synced_height: {COLOR_GREEN}{synced_height}{COLOR_RESET}");
        // initial send - to trigger children if new blocks won't come for a long time
        if !self.publish_height(&log_prefix, synced_height) {
            return;
        }

        let mut wait_after_height = synced_height;
        loop {
            if ctx.parent.upgrade().is_none() {
                break; // we don't need parent - it's just cancellation marker
            }

            let next_height = match ctx.receiver.wait_after(wait_after_height).await {
                Some(height) => height,
                None => break, // sender was closed
            };

            let start_ts = Instant::now();

            // We can't drop ourself to keep channels open
            // And must check parent from time to time for graceful shutdown
            if !self.handler.is_enabled() {
                log::debug!("[{log_prefix}] range [{COLOR_RED}{synced_height}{COLOR_RESET}, {COLOR_RED}{next_height}{COLOR_RESET}]: skipped (sync is disabled)");
                tokio::time::sleep(SLEEP_IF_DISABLED).await;
                continue;
            }


            let sync_from = synced_height + 1;
            let Some(sync_to) = self.calc_sync_to(sync_from, next_height) else {
                log::debug!("[{log_prefix}] range [{COLOR_RED}{sync_from}{COLOR_RESET}, {COLOR_RED}{next_height}{COLOR_RESET}]: skipped (range mismatch)");
                wait_after_height = next_height; // do nothing
                continue;
            };

            if !self.on_sync_start_loop(&ctx, &log_prefix, sync_from, sync_to).await {
                break;
            }

            let range_log_prefix_expected =
                format!("{log_prefix}] sync [{sync_from}, {next_height}] -> [{COLOR_GREEN}{sync_from}{COLOR_RESET}, {COLOR_GREEN}{sync_to}{COLOR_RESET}");

            let Some(new_synced_height) = self.sync_range_loop(&ctx, &range_log_prefix_expected, sync_from, sync_to).await else {
                log::debug!("[{log_prefix}] range [{COLOR_GREEN}{sync_from}{COLOR_RESET}, {COLOR_RED}{sync_to}{COLOR_RESET}]: skipped (ignored)");
                wait_after_height = next_height;
                continue;
            };

            let range_log_prefix_actual =
                format!("{log_prefix}] sync [{sync_from}, {next_height}] -> [{COLOR_GREEN}{sync_from}{COLOR_RESET}, {COLOR_GREEN}{new_synced_height}{COLOR_RESET}");

            if !self
                .save_synced_height_loop(&ctx, &range_log_prefix_actual, new_synced_height)
                .await
            {
                break;
            }
            if !self
                .on_sync_complete_loop(&ctx, &log_prefix, sync_from, sync_to, new_synced_height)
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
                ctx.metrics.update_synced_height(&sync_id, new_synced_height);
                log::info!("[{range_log_prefix_actual}]: wrapped to {COLOR_GREEN}{new_synced_height}{COLOR_RESET} ({sync_duration:.3?})");
            } else {
                ctx.metrics.update_sync(&sync_id, sync_from, synced_height, sync_duration);
                let synced_range = new_synced_height - sync_from + 1;
                if (ctx.log_progress)(sync_from, sync_to) {
                    log::info!("[{range_log_prefix_actual}]: done ({COLOR_GREEN}{synced_range}{COLOR_RESET} blocks, {sync_duration:.3?})");
                } else {
                    log::debug!("[{range_log_prefix_actual}]: done ({COLOR_GREEN}{synced_range}{COLOR_RESET} blocks, {sync_duration:.3?})");
                }
            }
        }
        log::info!("[{log_prefix}] {COLOR_GREEN}finished{COLOR_RESET}: parent is dropped")
    }

    async fn on_sync_start_loop(&self, ctx: &SyncCtx, log_prefix: &str, from: SyncHeight, to: SyncHeight) -> bool {
        loop {
            if ctx.parent.upgrade().is_none() {
                return false;
            }
            match ctx.callbacks.on_sync_start(self.handler.id(), from, to).await {
                Ok(()) => return true,
                Err(err) => {
                    log::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_sync_start({}, {from}, {to}) failed with err: {err}, retrying...",
                        self.handler.id()
                    );
                    ctx.metrics.inc_retries(self.handler.id(), SyncPhase::Callback);
                    tokio::time::sleep(self.handler.sleep_on_error()).await;
                },
            }
        }
    }

    fn calc_sync_to(&self, from: SyncHeight, to: SyncHeight) -> Option<SyncHeight> {
        if to < from {
            return None;
        }
        let available_range_size = to - from + 1;
        let min_sync_range = SyncHeight::try_from(self.handler.min_sync_range()).ok()?;
        let max_sync_range = SyncHeight::try_from(self.handler.max_sync_range()).ok()?;
        if min_sync_range == 0 || max_sync_range == 0 || available_range_size < min_sync_range {
            return None;
        }
        let range_size = std::cmp::min(available_range_size, max_sync_range);
        Some(from + range_size - 1)
    }

    #[rustfmt::skip]
    async fn sync_range_loop(&mut self, ctx: &SyncCtx, log_prefix: &str, from: SyncHeight, to: SyncHeight) -> Option<SyncHeight> {
        let sync_timeout = self.handler.sync_timeout();
        loop {
            ctx.parent.upgrade()?;
            let start_ts = Instant::now();

            match tokio::time::timeout(sync_timeout, self.handler.sync_range(from, to)).await {
                Ok(Ok(None)) => return None,
                Ok(Ok(Some(synced_height))) if synced_height < from && self.handler.allow_wrap() => return Some(synced_height),
                Ok(Ok(Some(synced_height))) if (from..=to).contains(&synced_height) => return Some(synced_height),
                Ok(Ok(Some(synced_height))) if synced_height > to => {
                    log::warn!(
                        "[{log_prefix}] {COLOR_RED}Got invalid synced height: {synced_height}, expected in range [{from}, {to}] ({:.3?}). Retrying...",
                        start_ts.elapsed()
                    );
                    if !self.on_sync_error_loop(ctx, log_prefix, from, to).await { return None; }
                },
                Ok(Ok(Some(synced_height))) => {
                    log::warn!(
                        "[{log_prefix}] {COLOR_RED}Got invalid wrapped synced height: {synced_height}, expected in range [{from}, {to}] or lower only when allow_wrap() is enabled ({:.3?}). Retrying...",
                        start_ts.elapsed()
                    );
                    if !self.on_sync_error_loop(ctx, log_prefix, from, to).await { return None; }
                },
                Ok(Err(err)) => {
                    log::warn!("[{log_prefix}] {COLOR_RED}Got error: {err} ({:.3?}). Retrying...", start_ts.elapsed());
                    if !self.on_sync_error_loop(ctx, log_prefix, from, to).await { return None; }
                },
                Err(_) => {
                    log::warn!("[{log_prefix}] {COLOR_RED}Timed out after {sync_timeout:.3?}. Retrying...");
                    if !self.on_sync_error_loop(ctx, log_prefix, from, to).await { return None; }
                },
            }
            ctx.metrics.inc_retries(self.handler.id(), SyncPhase::SyncRange);
            tokio::time::sleep(self.handler.sleep_on_error()).await;
        }
    }

    #[rustfmt::skip]
    async fn on_sync_error_loop(&self, ctx: &SyncCtx, log_prefix: &str, from: SyncHeight, to: SyncHeight) -> bool {
        loop {
            if ctx.parent.upgrade().is_none() {
                return false;
            }
            if let Err(err) = ctx.callbacks.on_sync_error(self.handler.id(), from, to).await {
                log::error!(
                    "[{log_prefix}] {COLOR_RED}callback on_sync_error({}, {from}, {to}) failed with err: {err}, retrying...",
                    self.handler.id()
                );
                ctx.metrics.inc_retries(self.handler.id(), SyncPhase::Callback);
                tokio::time::sleep(self.handler.sleep_on_error()).await;
                continue;
            }
            return true;
        }
    }

    async fn load_synced_height_loop(&mut self, ctx: &SyncCtx, log_prefix: &str) -> Option<SyncHeight> {
        loop {
            ctx.parent.upgrade()?;
            match ctx.status_manager.load_synced_height(self.handler.id()).await {
                Ok(height) => break height,
                Err(err) => {
                    log::warn!("[{log_prefix}] .load_synced_height() returns error: {err}. Retrying...");
                    ctx.metrics.inc_retries(self.handler.id(), SyncPhase::LoadHeight);
                    tokio::time::sleep(self.handler.sleep_on_error()).await;
                },
            }
        }
    }

    async fn save_synced_height_loop(&mut self, ctx: &SyncCtx, log_prefix: &str, height: SyncHeight) -> bool {
        loop {
            if ctx.parent.upgrade().is_none() {
                return false;
            }
            if let Err(err) = ctx.status_manager.save_synced_height(self.handler.id(), height).await {
                log::warn!("[{log_prefix}] .save_synced_height({height}) returns error: {err}. Retrying...");
                ctx.metrics.inc_retries(self.handler.id(), SyncPhase::SaveHeight);
                tokio::time::sleep(self.handler.sleep_on_error()).await;
                continue;
            }
            return true;
        }
    }

    async fn on_sync_complete_loop(
        &self,
        ctx: &SyncCtx,
        log_prefix: &str,
        from: SyncHeight,
        to: SyncHeight,
        real_to: SyncHeight,
    ) -> bool {
        let sync_id = &self.handler.id();
        loop {
            if ctx.parent.upgrade().is_none() {
                return false;
            }
            match ctx.callbacks.on_sync_complete(sync_id, from, to, real_to).await {
                Ok(()) => return true,
                Err(err) => {
                    log::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_sync_complete({sync_id}, {from}, {to}, {real_to}) failed with err: {err}, retrying..."
                    );
                    ctx.metrics.inc_retries(sync_id, SyncPhase::Callback);
                    tokio::time::sleep(self.handler.sleep_on_error()).await;
                    if ctx.parent.upgrade().is_none() {
                        return false;
                    }
                },
            }
        }
    }
}

impl SyncTrigger for Synchronizer {
    fn receiver(&self) -> SyncReceiver {
        self.tx_rx.1.clone()
    }
}

impl<T: SyncHandler> From<T> for Synchronizer {
    fn from(handler: T) -> Self {
        Self::new(handler)
    }
}

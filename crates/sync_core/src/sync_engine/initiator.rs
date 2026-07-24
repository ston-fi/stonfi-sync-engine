use crate::SyncCallback;
use crate::sync_engine::Inner;
use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::colors::{COLOR_GREEN, COLOR_PINK, COLOR_RED, COLOR_RESET};
use crate::sync_engine::metrics::{SyncEngineMetrics, SyncPhase};
use crate::sync_engine::multi_receiver::{SyncReceiver, SyncSender};
use crate::sync_engine::traits::{SyncInitiator, SyncTrigger};
use crate::sync_engine::{SyncHeight, SyncID};
use std::sync::{Arc, Weak};

/// Wraps a [`SyncInitiator`] so it can be registered in a [`crate::SyncEngine`].
pub struct Initiator {
    sync_initiator: Box<dyn SyncInitiator>,
    tx_rx: (SyncSender, SyncReceiver),
}

pub(super) struct InitiatorCtx {
    pub parent: Weak<Inner>,
    pub metrics: &'static SyncEngineMetrics,
    pub callbacks: Arc<CallbackStore>,
    pub log_progress: fn(SyncHeight, SyncHeight) -> bool,
}

impl Initiator {
    /// Wraps an initiator implementation for registration with a sync engine.
    pub fn new(inner: impl SyncInitiator) -> Self {
        Self {
            sync_initiator: Box::new(inner),
            tx_rx: tokio::sync::watch::channel(0),
        }
    }

    pub(super) fn id(&self) -> &SyncID {
        self.sync_initiator.id()
    }

    fn publish_height(&self, log_prefix: &str, height: SyncHeight) -> bool {
        if self.tx_rx.0.send(height).is_ok() {
            return true;
        }
        log::warn!("[{log_prefix}] failed to publish height {height}: receiver channel is closed");
        false
    }

    #[rustfmt::skip]
    pub(super) async fn run(mut self, ctx: InitiatorCtx) {
        let initiator_id = self.sync_initiator.id().to_owned();
        let log_prefix = format!("{COLOR_PINK}SYNC_INIT{COLOR_RESET}][{COLOR_PINK}{initiator_id}{COLOR_RESET}");

        let mut cur_height = loop {
            if ctx.parent.upgrade().is_none() {
                log::info!("[{log_prefix}] {COLOR_GREEN}finished{COLOR_RESET}: parent is dropped");
                return;
            }
            match self.sync_initiator.last_height(0).await {
                Ok(height) => break height,
                Err(err) => {
                    log::error!("[{log_prefix}] {COLOR_RED}Fail to load initial height: {err}, retrying...");
                    ctx.metrics.inc_retries(&initiator_id, SyncPhase::Initiator);
                    self.on_initiator_error_loop(&ctx, &log_prefix, &initiator_id, 0).await;
                    tokio::time::sleep(self.sync_initiator.sleep_on_error()).await;
                },
            }
        };
        ctx.metrics.update_initiator(&initiator_id, cur_height);

        log::info!("[{log_prefix}] started with last_height: {COLOR_GREEN}{cur_height}{COLOR_RESET}");
        if cur_height > 0 && !self.publish_height(&log_prefix, cur_height) {
            return;
        }
        loop {
            if ctx.parent.upgrade().is_none() {
                break; // we don't need parent - it's just cancellation marker
            }

            let new_height = match self.sync_initiator.last_height(cur_height).await {
                Ok(height) => height,
                Err(err) => {
                    log::warn!("[{log_prefix}] {COLOR_RED}last_height() failed with err: {err}");
                    ctx.metrics.inc_retries(&initiator_id, SyncPhase::Initiator);
                    self.on_initiator_error_loop(&ctx, &log_prefix, &initiator_id, cur_height).await;
                    tokio::time::sleep(self.sync_initiator.sleep_on_error()).await;
                    continue;
                },
            };
            if !self
                .on_initiator_next_height_loop(&ctx, &log_prefix, cur_height, new_height)
                .await
            {
                break;
            }
            if new_height <= cur_height {
                log::debug!("[{log_prefix}] got height <= cur_height ({new_height} <= {cur_height}), waiting for the next poll");
                tokio::time::sleep(self.sync_initiator.sleep_on_error()).await;
                continue;
            }

            if !self.publish_height(&log_prefix, new_height) {
                break;
            }
            ctx.metrics.update_initiator(&initiator_id, new_height);

            if (ctx.log_progress)(new_height, new_height) {
                log::info!("[{log_prefix}] sent new height: {COLOR_GREEN}{new_height}{COLOR_RESET}");
            } else {
                log::debug!("[{log_prefix}] sent new height: {COLOR_GREEN}{new_height}{COLOR_RESET}");
            }

            self.on_initiator_sent_loop(&ctx, &log_prefix, &initiator_id, cur_height, new_height).await;
            cur_height = new_height;
        }
        log::info!("[{log_prefix}] {COLOR_GREEN}finished{COLOR_RESET}: parent is dropped")
    }

    async fn on_initiator_next_height_loop(
        &self,
        ctx: &InitiatorCtx,
        log_prefix: &str,
        previous_height: SyncHeight,
        next_height: SyncHeight,
    ) -> bool {
        loop {
            if ctx.parent.upgrade().is_none() {
                return false;
            }
            match ctx
                .callbacks
                .on_initiator_next_height(self.sync_initiator.id(), previous_height, next_height)
                .await
            {
                Ok(()) => return true,
                Err(err) => {
                    log::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_initiator_next_height({}, {previous_height}, {next_height}) failed with err: {err}, retrying...",
                        self.sync_initiator.id()
                    );
                    ctx.metrics.inc_retries(self.sync_initiator.id(), SyncPhase::Callback);
                    tokio::time::sleep(self.sync_initiator.sleep_on_error()).await;
                },
            }
        }
    }

    async fn on_initiator_error_loop(&self, ctx: &InitiatorCtx, log_prefix: &str, initiator_id: &SyncID, height: u32) {
        loop {
            if ctx.parent.upgrade().is_none() {
                return;
            }
            match ctx.callbacks.on_initiator_error(initiator_id, height).await {
                Ok(()) => return,
                Err(err) => {
                    ctx.metrics.inc_retries(initiator_id, SyncPhase::Callback);
                    log::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_initiator_error({initiator_id}, {height}) failed with err: {err}, retrying..."
                    );
                    tokio::time::sleep(self.sync_initiator.sleep_on_error()).await;
                    if ctx.parent.upgrade().is_none() {
                        return;
                    }
                },
            }
        }
    }

    async fn on_initiator_sent_loop(
        &self,
        ctx: &InitiatorCtx,
        log_prefix: &str,
        initiator_id: &SyncID,
        previous_height: SyncHeight,
        sent_height: SyncHeight,
    ) {
        loop {
            if ctx.parent.upgrade().is_none() {
                return;
            }
            match ctx
                .callbacks
                .on_initiator_sent(initiator_id, previous_height, sent_height)
                .await
            {
                Ok(()) => return,
                Err(err) => {
                    log::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_initiator_sent({initiator_id}, {previous_height}, {sent_height}) failed with err: {err}, retrying..."
                    );
                    ctx.metrics.inc_retries(initiator_id, SyncPhase::Callback);
                    tokio::time::sleep(self.sync_initiator.sleep_on_error()).await;
                    if ctx.parent.upgrade().is_none() {
                        return;
                    }
                },
            }
        }
    }
}

impl SyncTrigger for Initiator {
    fn receiver(&self) -> SyncReceiver {
        self.tx_rx.1.clone()
    }
}

impl<T: SyncInitiator> From<T> for Initiator {
    fn from(sync_initiator: T) -> Self {
        Self::new(sync_initiator)
    }
}

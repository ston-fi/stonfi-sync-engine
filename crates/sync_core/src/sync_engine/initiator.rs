use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::colors::{COLOR_GREEN, COLOR_PINK, COLOR_RED, COLOR_RESET};
use crate::sync_engine::metrics::{SyncEngineMetrics, SyncPhase};
use crate::sync_engine::multi_receiver::{SyncReceiver, SyncSender};
use crate::sync_engine::traits::{SyncInitiator, SyncTrigger};
use crate::sync_engine::{SyncCallback, SyncHeight, sleep_or_cancelled};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Wraps a [`SyncInitiator`] so it can be registered in a
/// [`SyncEngine`](crate::sync_engine::SyncEngine).
pub struct Initiator {
    sync_initiator: Box<dyn SyncInitiator>,
    sender: SyncSender,
    receiver: SyncReceiver,
}

pub(super) struct InitiatorCtx {
    pub cancellation: CancellationToken,
    pub callbacks: Arc<CallbackStore>,
    pub log_progress: fn(SyncHeight, SyncHeight) -> bool,
}

impl Initiator {
    /// Wraps an initiator implementation for registration with a sync engine.
    pub fn new(inner: impl SyncInitiator) -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(0);
        Self {
            sync_initiator: Box::new(inner),
            sender,
            receiver,
        }
    }

    pub(super) fn id(&self) -> &str {
        self.sync_initiator.id()
    }

    fn publish_height(&self, log_prefix: &str, height: SyncHeight) -> bool {
        if self.sender.send(height).is_ok() {
            return true;
        }
        tracing::warn!("[{log_prefix}] failed to publish height {height}: receiver channel is closed");
        false
    }

    #[rustfmt::skip]
    pub(super) async fn run(mut self, ctx: InitiatorCtx) {
        let initiator_id = self.sync_initiator.id().to_owned();
        let log_prefix = format!("SYNC_INIT][{COLOR_PINK}{initiator_id}{COLOR_RESET}");

        let mut cur_height = loop {
            if ctx.cancellation.is_cancelled() {
                tracing::info!("[{log_prefix}] {COLOR_GREEN}finished{COLOR_RESET}: shutdown requested");
                return;
            }
            match self.sync_initiator.latest_height(0).await {
                Ok(height) => break height,
                Err(err) => {
                    tracing::error!("[{log_prefix}] {COLOR_RED}Fail to load initial height: {err}, retrying...");
                    SyncEngineMetrics::inc_retries(&initiator_id, SyncPhase::Initiator);
                    if !self
                        .on_initiator_error_loop(&ctx, &log_prefix, 0)
                        .await
                        || sleep_or_cancelled(&ctx.cancellation, self.sync_initiator.retry_delay()).await
                    {
                        return;
                    }
                },
            }
        };
        SyncEngineMetrics::update_initiator(&initiator_id, cur_height);

        tracing::info!("[{log_prefix}] started with latest_height: {COLOR_GREEN}{cur_height}{COLOR_RESET}");
        if cur_height > 0 && !self.publish_height(&log_prefix, cur_height) {
            return;
        }
        loop {
            if ctx.cancellation.is_cancelled() {
                break;
            }

            let new_height = match self.sync_initiator.latest_height(cur_height).await {
                Ok(height) => height,
                Err(err) => {
                    tracing::warn!("[{log_prefix}] {COLOR_RED}latest_height() failed with err: {err}");
                    SyncEngineMetrics::inc_retries(&initiator_id, SyncPhase::Initiator);
                    if !self
                        .on_initiator_error_loop(&ctx, &log_prefix, cur_height)
                        .await
                        || sleep_or_cancelled(&ctx.cancellation, self.sync_initiator.retry_delay()).await
                    {
                        break;
                    }
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
                tracing::debug!("[{log_prefix}] got height <= cur_height ({new_height} <= {cur_height}), waiting for the next poll");
                if sleep_or_cancelled(&ctx.cancellation, self.sync_initiator.retry_delay()).await {
                    break;
                }
                continue;
            }

            if !self.publish_height(&log_prefix, new_height) {
                break;
            }
            SyncEngineMetrics::update_initiator(&initiator_id, new_height);

            if (ctx.log_progress)(new_height, new_height) {
                tracing::info!("[{log_prefix}] sent new height: {COLOR_GREEN}{new_height}{COLOR_RESET}");
            } else {
                tracing::debug!("[{log_prefix}] sent new height: {COLOR_GREEN}{new_height}{COLOR_RESET}");
            }

            if !self
                .on_initiator_sent_loop(&ctx, &log_prefix, cur_height, new_height)
                .await
            {
                break;
            }
            cur_height = new_height;
        }
        tracing::info!("[{log_prefix}] {COLOR_GREEN}finished{COLOR_RESET}: shutdown requested")
    }

    async fn on_initiator_next_height_loop(
        &mut self,
        ctx: &InitiatorCtx,
        log_prefix: &str,
        previous_height: SyncHeight,
        next_height: SyncHeight,
    ) -> bool {
        loop {
            if ctx.cancellation.is_cancelled() {
                return false;
            }
            match ctx
                .callbacks
                .on_initiator_next_height(self.sync_initiator.id(), previous_height, next_height)
                .await
            {
                Ok(()) => return true,
                Err(err) => {
                    let initiator_id = self.sync_initiator.id();
                    tracing::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_initiator_next_height({initiator_id}, {previous_height}, {next_height}) failed with err: {err}, retrying..."
                    );
                    SyncEngineMetrics::inc_retries(initiator_id, SyncPhase::Callback);
                    if sleep_or_cancelled(&ctx.cancellation, self.sync_initiator.retry_delay()).await {
                        return false;
                    }
                },
            }
        }
    }

    async fn on_initiator_error_loop(&mut self, ctx: &InitiatorCtx, log_prefix: &str, height: SyncHeight) -> bool {
        loop {
            if ctx.cancellation.is_cancelled() {
                return false;
            }
            match ctx.callbacks.on_initiator_error(self.sync_initiator.id(), height).await {
                Ok(()) => return true,
                Err(err) => {
                    let initiator_id = self.sync_initiator.id();
                    SyncEngineMetrics::inc_retries(initiator_id, SyncPhase::Callback);
                    tracing::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_initiator_error({initiator_id}, {height}) failed with err: {err}, retrying..."
                    );
                    if sleep_or_cancelled(&ctx.cancellation, self.sync_initiator.retry_delay()).await {
                        return false;
                    }
                },
            }
        }
    }

    async fn on_initiator_sent_loop(
        &mut self,
        ctx: &InitiatorCtx,
        log_prefix: &str,
        previous_height: SyncHeight,
        sent_height: SyncHeight,
    ) -> bool {
        loop {
            if ctx.cancellation.is_cancelled() {
                return false;
            }
            match ctx
                .callbacks
                .on_initiator_sent(self.sync_initiator.id(), previous_height, sent_height)
                .await
            {
                Ok(()) => return true,
                Err(err) => {
                    let initiator_id = self.sync_initiator.id();
                    tracing::error!(
                        "[{log_prefix}] {COLOR_RED}callback on_initiator_sent({initiator_id}, {previous_height}, {sent_height}) failed with err: {err}, retrying..."
                    );
                    SyncEngineMetrics::inc_retries(initiator_id, SyncPhase::Callback);
                    if sleep_or_cancelled(&ctx.cancellation, self.sync_initiator.retry_delay()).await {
                        return false;
                    }
                },
            }
        }
    }
}

impl SyncTrigger for Initiator {
    fn receiver(&self) -> SyncReceiver {
        self.receiver.clone()
    }
}

impl<T: SyncInitiator> From<T> for Initiator {
    fn from(sync_initiator: T) -> Self {
        Self::new(sync_initiator)
    }
}

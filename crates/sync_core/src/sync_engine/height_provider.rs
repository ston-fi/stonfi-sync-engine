use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::metrics::{SyncEngineMetrics, SyncPhase};
use crate::sync_engine::progress::{ProgressReceiver, ProgressSender};
use crate::sync_engine::traits::{HeightLoader, ProgressProvider};
use crate::sync_engine::{SyncHeight, sleep_or_cancelled};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Polls a [`HeightLoader`] and publishes available heights to dependent
/// synchronizers.
pub struct HeightProvider {
    height_loader: Box<dyn HeightLoader>,
    sender: ProgressSender,
    receiver: ProgressReceiver,
}

pub(super) struct HeightProviderCtx {
    pub cancellation: CancellationToken,
    pub callbacks: Arc<CallbackStore>,
    pub log_progress: fn(SyncHeight, SyncHeight) -> bool,
}

impl HeightProvider {
    /// Creates an engine-owned provider from a height loader.
    pub fn new(height_loader: impl HeightLoader) -> Self {
        let (sender, receiver) = tokio::sync::watch::channel(0);
        Self {
            height_loader: Box::new(height_loader),
            sender,
            receiver,
        }
    }

    pub(super) fn id(&self) -> &str {
        self.height_loader.id()
    }

    fn publish_height(&self, log_prefix: &str, height: SyncHeight) -> bool {
        if self.sender.send(height).is_ok() {
            return true;
        }
        tracing::warn!("[{log_prefix}] failed to publish height {height}: receiver channel is closed");
        false
    }

    #[rustfmt::skip]
    pub(super) async fn run(mut self, ctx: HeightProviderCtx) {
        let id = self.height_loader.id().to_owned();
        let log_prefix = format!("HEIGHT_PROVIDER][{id}");

        let mut cur_height = loop {
            if ctx.cancellation.is_cancelled() {
                tracing::info!("[{log_prefix}] finished: shutdown requested");
                return;
            }
            match self.height_loader.latest_height(0).await {
                Ok(height) => break height,
                Err(err) => {
                    tracing::error!("[{log_prefix}] Fail to load initial height: {err}, retrying...");
                    SyncEngineMetrics::inc_retries(&id, SyncPhase::HeightLoad);
                    if !ctx.callbacks
                        .on_height_load_error_loop(&id, 0, self.height_loader.retry_delay())
                        .await
                        || sleep_or_cancelled(&ctx.cancellation, self.height_loader.retry_delay()).await
                    {
                        return;
                    }
                },
            }
        };
        SyncEngineMetrics::update_loaded_height(&id, cur_height);
        if !ctx.callbacks
            .on_height_loaded_loop(&id, 0, cur_height, self.height_loader.retry_delay())
            .await
        {
            return;
        }

        tracing::info!("[{log_prefix}] started with latest_height: {cur_height}");
        if cur_height > 0 {
            if !self.publish_height(&log_prefix, cur_height) {
                return;
            }
            if !ctx.callbacks
                .on_height_published_loop(&id, 0, cur_height, self.height_loader.retry_delay())
                .await
            {
                return;
            }
        }
        loop {
            if ctx.cancellation.is_cancelled() {
                break;
            }

            let loaded_height = match self.height_loader.latest_height(cur_height).await {
                Ok(height) => height,
                Err(err) => {
                    tracing::warn!("[{log_prefix}] latest_height() failed with err: {err}");
                    SyncEngineMetrics::inc_retries(&id, SyncPhase::HeightLoad);
                    if !ctx.callbacks
                        .on_height_load_error_loop(&id, cur_height, self.height_loader.retry_delay())
                        .await
                        || sleep_or_cancelled(&ctx.cancellation, self.height_loader.retry_delay()).await
                    {
                        break;
                    }
                    continue;
                },
            };
            if !ctx.callbacks
                .on_height_loaded_loop(&id, cur_height, loaded_height, self.height_loader.retry_delay())
                .await
            {
                break;
            }
            if loaded_height <= cur_height {
                tracing::debug!("[{log_prefix}] got height <= cur_height ({loaded_height} <= {cur_height}), waiting for the next poll");
                if sleep_or_cancelled(&ctx.cancellation, self.height_loader.retry_delay()).await {
                    break;
                }
                continue;
            }

            if !self.publish_height(&log_prefix, loaded_height) {
                break;
            }
            SyncEngineMetrics::update_loaded_height(&id, loaded_height);

            if (ctx.log_progress)(loaded_height, loaded_height) {
                tracing::info!("[{log_prefix}] published new height: {loaded_height}");
            } else {
                tracing::debug!("[{log_prefix}] published new height: {loaded_height}");
            }

            if !ctx.callbacks
                .on_height_published_loop(&id, cur_height, loaded_height, self.height_loader.retry_delay())
                .await
            {
                break;
            }
            cur_height = loaded_height;
        }
        tracing::info!("[{log_prefix}] finished: shutdown requested")
    }
}

impl ProgressProvider for HeightProvider {
    fn subscribe(&self) -> ProgressReceiver {
        self.receiver.clone()
    }
}

impl<T: HeightLoader> From<T> for HeightProvider {
    fn from(height_loader: T) -> Self {
        Self::new(height_loader)
    }
}

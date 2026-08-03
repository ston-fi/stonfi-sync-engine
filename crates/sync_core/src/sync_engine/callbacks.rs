use crate::errors::SyncCoreResult;
use crate::sync_engine::metrics::{SyncEngineMetrics, SyncPhase};
use crate::sync_engine::{SyncCallback, SyncHeight, sleep_or_cancelled};
use futures::future::BoxFuture;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub(super) struct CallbackStore {
    callbacks: Vec<Arc<dyn SyncCallback>>,
    cancellation: CancellationToken,
}

impl CallbackStore {
    pub(super) fn new(callbacks: Vec<Arc<dyn SyncCallback>>, cancellation: CancellationToken) -> Self {
        Self {
            callbacks,
            cancellation,
        }
    }

    pub(super) async fn on_height_load_error_loop(&self, id: &str, height: SyncHeight, retry_delay: Duration) -> bool {
        self.callback_loop(id, "on_height_load_error", retry_delay, |callback, id| {
            callback.on_height_load_error(id, height)
        })
        .await
    }

    pub(super) async fn on_height_loaded_loop(
        &self,
        id: &str,
        previous_height: SyncHeight,
        loaded_height: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(id, "on_height_loaded", retry_delay, |callback, id| {
            callback.on_height_loaded(id, previous_height, loaded_height)
        })
        .await
    }

    pub(super) async fn on_height_published_loop(
        &self,
        id: &str,
        previous_height: SyncHeight,
        published_height: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(id, "on_height_published", retry_delay, |callback, id| {
            callback.on_height_published(id, previous_height, published_height)
        })
        .await
    }

    pub(super) async fn on_sync_start_loop(
        &self,
        id: &str,
        from: SyncHeight,
        to: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(id, "on_sync_start", retry_delay, |callback, id| {
            callback.on_sync_start(id, from, to)
        })
        .await
    }

    pub(super) async fn on_sync_error_loop(
        &self,
        id: &str,
        from: SyncHeight,
        to: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(id, "on_sync_error", retry_delay, |callback, id| {
            callback.on_sync_error(id, from, to)
        })
        .await
    }

    pub(super) async fn on_sync_complete_loop(
        &self,
        id: &str,
        from: SyncHeight,
        to: SyncHeight,
        processed_to: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(id, "on_sync_complete", retry_delay, |callback, id| {
            callback.on_sync_complete(id, from, to, processed_to)
        })
        .await
    }

    async fn callback_loop<F>(&self, id: &str, callback_name: &'static str, retry_delay: Duration, invoke: F) -> bool
    where
        F: for<'a> Fn(&'a dyn SyncCallback, &'a str) -> BoxFuture<'a, SyncCoreResult<()>>,
    {
        loop {
            if self.cancellation.is_cancelled() {
                return false;
            }

            let result: SyncCoreResult<()> = async {
                for callback in &self.callbacks {
                    invoke(callback.as_ref(), id).await?;
                }
                Ok(())
            }
            .await;

            match result {
                Ok(()) => return !self.cancellation.is_cancelled(),
                Err(err) => {
                    tracing::error!("[CALLBACK][{id}] {callback_name} failed with err: {err}, retrying...");
                    SyncEngineMetrics::inc_retries(id, SyncPhase::Callback);
                    if sleep_or_cancelled(&self.cancellation, retry_delay).await {
                        return false;
                    }
                },
            }
        }
    }
}

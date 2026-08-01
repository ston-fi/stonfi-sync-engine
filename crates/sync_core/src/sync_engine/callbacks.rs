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

    pub(super) async fn on_height_load_error_loop(
        &self,
        component_id: &str,
        height: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(component_id, "on_height_load_error", retry_delay, |callback, component_id| {
            callback.on_height_load_error(component_id, height)
        })
        .await
    }

    pub(super) async fn on_height_loaded_loop(
        &self,
        component_id: &str,
        previous_height: SyncHeight,
        loaded_height: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(component_id, "on_height_loaded", retry_delay, |callback, component_id| {
            callback.on_height_loaded(component_id, previous_height, loaded_height)
        })
        .await
    }

    pub(super) async fn on_height_published_loop(
        &self,
        component_id: &str,
        previous_height: SyncHeight,
        published_height: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(component_id, "on_height_published", retry_delay, |callback, component_id| {
            callback.on_height_published(component_id, previous_height, published_height)
        })
        .await
    }

    pub(super) async fn on_sync_start_loop(
        &self,
        component_id: &str,
        from: SyncHeight,
        to: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(component_id, "on_sync_start", retry_delay, |callback, component_id| {
            callback.on_sync_start(component_id, from, to)
        })
        .await
    }

    pub(super) async fn on_sync_error_loop(
        &self,
        component_id: &str,
        from: SyncHeight,
        to: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(component_id, "on_sync_error", retry_delay, |callback, component_id| {
            callback.on_sync_error(component_id, from, to)
        })
        .await
    }

    pub(super) async fn on_sync_complete_loop(
        &self,
        component_id: &str,
        from: SyncHeight,
        to: SyncHeight,
        processed_to: SyncHeight,
        retry_delay: Duration,
    ) -> bool {
        self.callback_loop(component_id, "on_sync_complete", retry_delay, |callback, component_id| {
            callback.on_sync_complete(component_id, from, to, processed_to)
        })
        .await
    }

    async fn callback_loop<F>(
        &self,
        component_id: &str,
        callback_name: &'static str,
        retry_delay: Duration,
        invoke: F,
    ) -> bool
    where
        F: for<'a> Fn(&'a dyn SyncCallback, &'a str) -> BoxFuture<'a, SyncCoreResult<()>>,
    {
        loop {
            if self.cancellation.is_cancelled() {
                return false;
            }

            let result: SyncCoreResult<()> = async {
                for callback in &self.callbacks {
                    invoke(callback.as_ref(), component_id).await?;
                }
                Ok(())
            }
            .await;

            match result {
                Ok(()) => return !self.cancellation.is_cancelled(),
                Err(err) => {
                    tracing::error!("[CALLBACK][{component_id}] {callback_name} failed with err: {err}, retrying...");
                    SyncEngineMetrics::inc_retries(component_id, SyncPhase::Callback);
                    if sleep_or_cancelled(&self.cancellation, retry_delay).await {
                        return false;
                    }
                },
            }
        }
    }
}

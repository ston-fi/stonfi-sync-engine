#[cfg(test)]
mod _test_builder;
#[cfg(test)]
mod _test_height_provider;
#[cfg(test)]
mod _test_lifecycle;
#[cfg(test)]
mod _test_support;
#[cfg(test)]
mod _test_synchronizer;
mod builder;
mod callbacks;
mod height_provider;
mod metrics;
mod progress;
mod synchronizer;
mod traits;

pub use builder::*;
pub use height_provider::*;
pub use progress::*;
pub use synchronizer::*;
pub use traits::*;

use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::progress::MultiReceiver;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Height unit tracked by the engine.
///
/// `0` means no progress. Metrics may lose precision above `2^53`, while engine
/// processing and persistence retain the full `u64` value.
pub type SyncHeight = u64;

/// Coordinates height providers and synchronizers and runs the dependency graph.
pub struct SyncEngine {
    progress_store: Arc<dyn SyncProgressStore>,
    callbacks: Vec<Arc<dyn SyncCallback>>,
    log_progress: fn(SyncHeight, SyncHeight) -> bool,
    height_providers: Vec<HeightProvider>,
    synchronizers: Vec<(Synchronizer, MultiReceiver)>,
    shutdown_timeout: Duration,
}

impl SyncEngine {
    /// Creates a builder backed by `progress_store`.
    #[must_use]
    pub fn builder(progress_store: Arc<dyn SyncProgressStore>) -> Builder {
        Builder::new(progress_store)
    }

    /// Starts all registered handlers and returns their runtime owner.
    ///
    /// # Panics
    ///
    /// Panics outside Tokio or when metrics were not initialized.
    pub fn run(self) -> RunHandle {
        let Self {
            progress_store,
            callbacks,
            log_progress,
            height_providers,
            synchronizers,
            shutdown_timeout,
        } = self;
        let cancellation = CancellationToken::new();
        let callbacks = Arc::new(CallbackStore::new(callbacks, cancellation.clone()));
        let tasks = FuturesUnordered::new();

        for height_provider in height_providers {
            let ctx = HeightProviderCtx {
                cancellation: cancellation.clone(),
                callbacks: callbacks.clone(),
                log_progress,
            };
            tasks.push(tokio::spawn(height_provider.run(ctx)));
        }
        for (sync, receiver) in synchronizers {
            let ctx = SyncCtx {
                receiver,
                cancellation: cancellation.clone(),
                progress_store: progress_store.clone(),
                callbacks: callbacks.clone(),
                log_progress,
            };
            tasks.push(tokio::spawn(sync.run(ctx)));
        }

        RunHandle {
            cancellation,
            tasks,
            shutdown_timeout,
        }
    }
}

/// Owns tasks spawned by [`SyncEngine::run`].
///
/// Dropping it requests shutdown without waiting.
#[must_use = "dropping the run handle immediately requests engine shutdown"]
pub struct RunHandle {
    cancellation: CancellationToken,
    tasks: FuturesUnordered<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl RunHandle {
    /// Requests shutdown, waits until the configured timeout, then aborts
    /// remaining tasks. Consumer futures remain cooperative until they yield.
    ///
    /// # Errors
    ///
    /// Returns an error on task failure or shutdown timeout.
    pub async fn shutdown(mut self) -> SyncCoreResult<()> {
        self.cancellation.cancel();
        let shutdown_timeout = self.shutdown_timeout;
        match tokio::time::timeout(shutdown_timeout, self.join_tasks()).await {
            Ok(result) => result,
            Err(_) => {
                for task in self.tasks.iter() {
                    task.abort();
                }
                Err(SyncCoreError::system(format!(
                    "sync engine shutdown exceeded {shutdown_timeout:.3?}"
                )))
            },
        }
    }

    /// Waits for natural completion without requesting shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error on task failure.
    pub async fn wait(mut self) -> SyncCoreResult<()> {
        self.join_tasks().await
    }

    async fn join_tasks(&mut self) -> SyncCoreResult<()> {
        let mut first_error = None;
        while let Some(result) = self.tasks.next().await {
            if let Err(error) = result {
                if first_error.is_none() {
                    first_error = Some(SyncCoreError::system(format!("sync engine task failed to join: {error}")));
                    self.cancellation.cancel();
                    for task in self.tasks.iter() {
                        task.abort();
                    }
                } else {
                    tracing::warn!("[SYNC_ENGINE] additional task join failure: {error}");
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for RunHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

pub(super) async fn sleep_or_cancelled(cancellation: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

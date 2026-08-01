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
/// Height `0` is reserved as the initial no-progress sentinel. Synchronizers
/// therefore process chain heights starting from `1` unless a consumer maps a
/// zero-based chain coordinate into this domain.
///
/// Prometheus exposes numeric samples as `f64`, so height metrics may lose unit
/// precision above `2^53` even though engine processing and persistence retain
/// the full `u64` value.
pub type SyncHeight = u64;

/// Coordinates height providers and synchronizers and runs the dependency graph.
pub struct SyncEngine {
    status_store: Arc<dyn SyncStatusStore>,
    callbacks: Vec<Arc<dyn SyncCallback>>,
    log_progress: fn(SyncHeight, SyncHeight) -> bool,
    height_providers: Vec<HeightProvider>,
    synchronizers: Vec<(Synchronizer, MultiReceiver)>,
    shutdown_timeout: Duration,
}

impl SyncEngine {
    /// Creates a builder backed by `status_store`.
    #[must_use]
    pub fn builder(status_store: Arc<dyn SyncStatusStore>) -> Builder {
        Builder::new(status_store)
    }

    /// Consumes the engine definition and starts every registered height
    /// provider and synchronizer on the current Tokio runtime.
    ///
    /// The returned [`RunHandle`] owns the running tasks. Use
    /// [`RunHandle::shutdown`] for bounded awaited shutdown or
    /// [`RunHandle::wait`] to wait for the tasks to finish naturally.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, following
    /// [`tokio::spawn`] semantics. Spawned tasks also panic when application
    /// startup has not called `stonfi_metrics::init_metrics!`.
    pub fn run(self) -> RunHandle {
        let Self {
            status_store,
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
                status_store: status_store.clone(),
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

/// Join handles for tasks spawned by [`SyncEngine::run`].
///
/// Dropping this handle signals cooperative shutdown but does not wait for task
/// completion. Use [`RunHandle::shutdown`] when completion must be confirmed.
#[must_use = "dropping the run handle immediately requests engine shutdown"]
pub struct RunHandle {
    cancellation: CancellationToken,
    tasks: FuturesUnordered<JoinHandle<()>>,
    shutdown_timeout: Duration,
}

impl RunHandle {
    /// Signals cooperative shutdown and waits up to the configured timeout for
    /// all spawned tasks to finish.
    ///
    /// Engine-owned progress waits and retry sleeps are interrupted. Active
    /// consumer-provided futures remain cooperative until the configured
    /// shutdown timeout, after which task abortion is requested and this method
    /// returns without another unbounded join. Tokio applies abortion when a
    /// task next yields and cannot preempt consumer code that never yields.
    ///
    /// # Errors
    ///
    /// Returns an error if a spawned task panicked or was cancelled, or when
    /// shutdown exceeds the timeout. Remaining tasks are aborted on timeout.
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

    /// Waits for all spawned tasks to finish without requesting shutdown.
    ///
    /// This is useful when every progress provider can close naturally. Engines
    /// with polling height providers normally require [`RunHandle::shutdown`]
    /// instead.
    ///
    /// # Errors
    ///
    /// Returns an error if a spawned task panicked or was cancelled.
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
        biased;
        _ = cancellation.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

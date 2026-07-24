#[cfg(test)]
mod _tests;
mod builder;
mod callbacks;
mod colors;
mod initiator;
mod metrics;
mod multi_receiver;
mod synchronizer;
mod traits;

pub use builder::*;
pub use initiator::*;
pub use multi_receiver::*;
pub use synchronizer::*;
pub use traits::*;

use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::metrics::SyncEngineMetrics;
use crate::sync_engine::multi_receiver::MultiReceiver;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Height unit tracked by the engine.
pub type SyncHeight = u32;

/// Coordinates initiators and synchronizers and runs the dependency graph.
pub struct SyncEngine {
    status_manager: Arc<dyn SyncStatusManager>,
    callbacks: Arc<CallbackStore>,
    metrics: &'static SyncEngineMetrics,
    log_progress: fn(SyncHeight, SyncHeight) -> bool,
    initiators: Vec<Initiator>,
    synchronizers: Vec<(Synchronizer, MultiReceiver)>,
}

impl SyncEngine {
    /// Creates a builder backed by `status_manager`.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine's metrics cannot be initialized and
    /// registered with the default Prometheus registry.
    pub fn builder(status_manager: Arc<dyn SyncStatusManager>) -> SyncCoreResult<Builder> {
        Builder::new(status_manager)
    }

    /// Consumes the engine definition and starts every registered initiator and
    /// synchronizer on the current Tokio runtime.
    ///
    /// The returned [`RunHandle`] owns the running tasks. Use
    /// [`RunHandle::shutdown`] for awaited cooperative shutdown or
    /// [`RunHandle::wait`] to wait for the tasks to finish naturally.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, following
    /// [`tokio::spawn`] semantics.
    pub fn run(self) -> RunHandle {
        let Self {
            status_manager,
            callbacks,
            metrics,
            log_progress,
            initiators,
            synchronizers,
        } = self;
        let cancellation = CancellationToken::new();
        let mut tasks = Vec::new();

        for initiator in initiators {
            let ctx = InitiatorCtx {
                cancellation: cancellation.clone(),
                metrics,
                callbacks: callbacks.clone(),
                log_progress,
            };
            tasks.push(tokio::spawn(initiator.run(ctx)));
        }
        for (sync, rcv) in synchronizers {
            let ctx = SyncCtx {
                receiver: rcv,
                cancellation: cancellation.clone(),
                status_manager: status_manager.clone(),
                callbacks: callbacks.clone(),
                metrics,
                log_progress,
            };
            tasks.push(tokio::spawn(sync.run(ctx)));
        }

        RunHandle { cancellation, tasks }
    }
}

/// Join handles for tasks spawned by [`SyncEngine::run`].
///
/// Dropping this handle signals cooperative shutdown but does not wait for task
/// completion. Use [`RunHandle::shutdown`] when completion must be confirmed.
#[must_use = "dropping the run handle immediately requests engine shutdown"]
pub struct RunHandle {
    cancellation: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl RunHandle {
    /// Signals cooperative shutdown and waits for all spawned tasks to finish.
    ///
    /// Engine-owned trigger waits and retry sleeps are interrupted. An active
    /// consumer-provided future is not preempted and must return before its task
    /// can observe shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if a spawned task panicked or was cancelled.
    pub async fn shutdown(mut self) -> SyncCoreResult<()> {
        self.cancellation.cancel();
        self.join_tasks().await
    }

    /// Waits for all spawned tasks to finish without requesting shutdown.
    ///
    /// This is useful when every trigger can close naturally. Engines with
    /// polling initiators normally require [`RunHandle::shutdown`] instead.
    ///
    /// # Errors
    ///
    /// Returns an error if a spawned task panicked or was cancelled.
    pub async fn wait(mut self) -> SyncCoreResult<()> {
        self.join_tasks().await
    }

    async fn join_tasks(&mut self) -> SyncCoreResult<()> {
        let mut first_error = None;
        for task in self.tasks.drain(..) {
            if let Err(error) = task.await {
                if first_error.is_none() {
                    first_error = Some(SyncCoreError::system(format!("sync engine task failed to join: {error}")));
                } else {
                    log::warn!("[SYNC_ENGINE] additional task join failure: {error}");
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

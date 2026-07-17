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

use crate::errors::SyncCoreResult;
use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::metrics::SyncEngineMetrics;
use crate::sync_engine::multi_receiver::MultiReceiver;
use parking_lot::Mutex;
use std::sync::Arc;
use stonfi_commons_metrics::metrics_provider::{BoxableCollector, MetricsProvider};
use tokio::task::JoinHandle;

/// Synchronizer and initiator identifier used in logs, metrics, and storage.
pub type SyncID = String;
/// Height unit tracked by the engine.
pub type SyncHeight = u32;

/// Coordinates initiators and synchronizers and runs the dependency graph.
pub struct SyncEngine(Arc<Inner>);

impl SyncEngine {
    /// Creates a builder backed by `status_manager`.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine's metrics cannot be constructed.
    pub fn builder(status_manager: Arc<dyn SyncStatusManager>) -> SyncCoreResult<Builder> {
        Builder::new(status_manager)
    }

    /// Starts every registered initiator and synchronizer on the current Tokio
    /// runtime and returns handles for observing their completion.
    ///
    /// Dropping the engine is the cooperative shutdown signal. Consumer
    /// futures already being polled are not preempted; they must return before
    /// the task can observe shutdown.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, following
    /// [`tokio::spawn`] semantics.
    pub fn run(&self) -> RunHandle {
        let inner_weak = Arc::downgrade(&self.0);
        let mut tasks = Vec::new();

        for initiator in self.0.initiators.lock().drain(..) {
            let ctx = InitiatorCtx {
                parent: inner_weak.clone(),
                metrics: self.0.metrics.clone(),
                callbacks: self.0.callbacks.clone(),
                log_progress: self.0.log_progress,
            };
            tasks.push(tokio::spawn(initiator.run(ctx)));
        }
        for (sync, rcv) in self.0.synchronizers.lock().drain(..) {
            let ctx = SyncCtx {
                receiver: rcv,
                parent: inner_weak.clone(),
                status_manager: self.0.status_manager.clone(),
                callbacks: self.0.callbacks.clone(),
                metrics: self.0.metrics.clone(),
                log_progress: self.0.log_progress,
            };
            tasks.push(tokio::spawn(sync.run(ctx)));
        }

        RunHandle { tasks }
    }
}

/// Join handles for tasks spawned by [`SyncEngine::run`].
///
/// Drop the engine first, then call [`RunHandle::wait`] to observe cooperative
/// shutdown. Dropping this handle detaches the tasks; it does not stop them.
pub struct RunHandle {
    tasks: Vec<JoinHandle<()>>,
}

impl RunHandle {
    /// Waits for all spawned tasks to finish.
    ///
    /// Task panics and cancellations are logged after all handles have been
    /// awaited.
    pub async fn wait(self) {
        for task in self.tasks {
            if let Err(err) = task.await {
                log::warn!("[SYNC_ENGINE] spawned task finished with join error: {err}");
            }
        }
    }
}

impl MetricsProvider for SyncEngine {
    fn provide_metrics(&self) -> Vec<&dyn BoxableCollector> {
        self.0.metrics.provide_metrics()
    }
}

struct Inner {
    status_manager: Arc<dyn SyncStatusManager>,
    initiators: Mutex<Vec<Initiator>>,
    synchronizers: Mutex<Vec<(Synchronizer, MultiReceiver)>>,
    callbacks: Arc<CallbackStore>,
    metrics: Arc<SyncEngineMetrics>,
    log_progress: fn(SyncHeight, SyncHeight) -> bool,
}

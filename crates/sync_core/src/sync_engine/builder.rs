use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::callbacks::CallbackStore;
use crate::sync_engine::initiator::Initiator;
use crate::sync_engine::metrics::SyncEngineMetrics;
use crate::sync_engine::multi_receiver::MultiReceiver;
use crate::sync_engine::synchronizer::Synchronizer;
use crate::sync_engine::traits::SyncTrigger;
use crate::sync_engine::{Inner, SyncEngine, SyncID, SyncStatusManager};
use crate::{SyncCallback, SyncHeight};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

/// Builds a [`SyncEngine`] with initiators, synchronizers, and callbacks.
pub struct Builder {
    inner: Inner,
    callbacks: Mutex<CallbackStore>,
    registered_ids: HashSet<SyncID>,
}

impl Builder {
    pub(super) fn new(status_manager: Arc<dyn SyncStatusManager>) -> SyncCoreResult<Self> {
        let builder = Self {
            inner: Inner {
                status_manager,
                initiators: Default::default(),
                synchronizers: Default::default(),
                callbacks: Default::default(),
                metrics: SyncEngineMetrics::initialize()?,
                log_progress: |_, _| true, // always print progress
            },
            callbacks: Default::default(),
            registered_ids: Default::default(),
        };
        Ok(builder)
    }

    /// Registers an initiator.
    ///
    /// # Errors
    ///
    /// Returns an error when another registered entity has the same ID.
    pub fn add_initiator(mut self, initiator: Initiator) -> SyncCoreResult<Self> {
        let initiator_id = initiator.id().clone();
        if !self.registered_ids.insert(initiator_id.clone()) {
            return Err(SyncCoreError::logic(format!(
                "Sync entity with id {initiator_id} is already registered"
            )));
        }
        self.inner.initiators.lock().push(initiator);
        Ok(self)
    }

    /// Registers a synchronizer and the triggers it depends on.
    ///
    /// # Errors
    ///
    /// Returns an error for missing triggers, duplicate IDs, or invalid range
    /// limits.
    pub fn add_sync(mut self, sync: Synchronizer, triggers: &[&dyn SyncTrigger]) -> SyncCoreResult<Self> {
        let sync_id = sync.handler.id();
        let min_sync_range = sync.handler.min_sync_range();
        let max_sync_range = sync.handler.max_sync_range();
        if min_sync_range == 0
            || max_sync_range == 0
            || min_sync_range > max_sync_range
            || SyncHeight::try_from(min_sync_range).is_err()
            || SyncHeight::try_from(max_sync_range).is_err()
        {
            let err_msg = format!(
                "Synchronizer {sync_id} has invalid sync range: min_sync_range={min_sync_range}, max_sync_range={max_sync_range}"
            );
            return Err(SyncCoreError::Logic(err_msg));
        }

        if !self.registered_ids.insert(sync_id.clone()) {
            let err_msg = format!("Sync entity with id {sync_id} is already registered");
            return Err(SyncCoreError::Logic(err_msg));
        }

        let receivers = triggers.iter().map(|x| x.receiver()).collect();
        let multi_receiver = MultiReceiver::new(receivers)?;
        self.inner.synchronizers.lock().push((sync, multi_receiver));
        Ok(self)
    }

    /// Registers a callback that will observe engine events.
    ///
    /// # Errors
    ///
    /// This method currently cannot fail; the result keeps builder chaining
    /// consistent with other registration methods.
    pub fn add_callback(self, callback: Arc<dyn SyncCallback>) -> SyncCoreResult<Self> {
        self.callbacks.lock().add(callback);
        Ok(self)
    }

    /// Control log::info frequency. If returns false, progress will be printed in debug
    /// By default, always return true
    pub fn with_log_progress(mut self, log_progress: fn(SyncHeight, SyncHeight) -> bool) -> Self {
        self.inner.log_progress = log_progress;
        self
    }

    /// Finalizes the builder and returns the engine.
    pub fn build(mut self) -> SyncEngine {
        self.inner.callbacks = Arc::new(self.callbacks.into_inner());
        SyncEngine(Arc::new(self.inner))
    }
}

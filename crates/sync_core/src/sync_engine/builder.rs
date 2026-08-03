use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::height_provider::HeightProvider;
use crate::sync_engine::progress::MultiReceiver;
use crate::sync_engine::synchronizer::Synchronizer;
use crate::sync_engine::traits::ProgressProvider;
use crate::sync_engine::{INITIAL_HEIGHT, SyncCallback, SyncEngine, SyncHeight, SyncStatusStore};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Builds a [`SyncEngine`] with height providers, synchronizers, and callbacks.
pub struct Builder {
    status_store: Arc<dyn SyncStatusStore>,
    log_progress: fn(SyncHeight, SyncHeight) -> bool,
    height_providers: Vec<HeightProvider>,
    synchronizers: Vec<(Synchronizer, MultiReceiver)>,
    callbacks: Vec<Arc<dyn SyncCallback>>,
    registered_ids: HashSet<String>,
    shutdown_timeout: Duration,
}

impl Builder {
    pub(super) fn new(status_store: Arc<dyn SyncStatusStore>) -> Self {
        Self {
            status_store,
            log_progress: |_, _| true,
            height_providers: Default::default(),
            synchronizers: Default::default(),
            callbacks: Default::default(),
            registered_ids: Default::default(),
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    /// Registers a height provider.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler ID is invalid, reserved, or duplicated.
    pub fn add_height_provider(mut self, provider: HeightProvider) -> SyncCoreResult<Self> {
        let id = provider.id().to_owned();
        validate_id(&id)?;
        if !self.registered_ids.insert(id.clone()) {
            return Err(SyncCoreError::logic(format!("handler ID {id} is already registered")));
        }
        self.height_providers.push(provider);
        Ok(self)
    }

    /// Registers a synchronizer and its upstream progress providers.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid, reserved, or duplicate handler ID,
    /// missing progress providers, or invalid range limits.
    pub fn add_synchronizer(mut self, sync: Synchronizer, providers: &[&dyn ProgressProvider]) -> SyncCoreResult<Self> {
        let id = sync.handler.id().to_owned();
        validate_id(&id)?;
        let min_batch_size = sync.handler.min_batch_size();
        let max_batch_size = sync.handler.max_batch_size();
        if min_batch_size == 0
            || max_batch_size == 0
            || min_batch_size > max_batch_size
            || SyncHeight::try_from(min_batch_size).is_err()
            || SyncHeight::try_from(max_batch_size).is_err()
        {
            let err_msg = format!(
                "Synchronizer {id} has invalid batch size: min_batch_size={min_batch_size}, max_batch_size={max_batch_size}"
            );
            return Err(SyncCoreError::Logic(err_msg));
        }

        if !self.registered_ids.insert(id.clone()) {
            let err_msg = format!("handler ID {id} is already registered");
            return Err(SyncCoreError::Logic(err_msg));
        }

        let receivers = providers.iter().map(|provider| provider.subscribe()).collect();
        let receiver = MultiReceiver::new(receivers)?;
        self.synchronizers.push((sync, receiver));
        Ok(self)
    }

    /// Registers a callback that will observe engine events.
    pub fn add_callback(mut self, callback: Arc<dyn SyncCallback>) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Sets the predicate that selects info-level progress logs.
    ///
    /// The predicate receives the inclusive range bounds. Returning `false`
    /// writes that progress event at debug level instead. Progress is logged at
    /// info level by default.
    pub fn with_log_progress(mut self, log_progress: fn(SyncHeight, SyncHeight) -> bool) -> Self {
        self.log_progress = log_progress;
        self
    }

    /// Sets the maximum duration for [`RunHandle::shutdown`](crate::sync_engine::RunHandle::shutdown).
    ///
    /// # Errors
    ///
    /// Returns an error when `timeout` is zero or too large for an instant.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> SyncCoreResult<Self> {
        if timeout.is_zero() {
            return Err(SyncCoreError::invalid_args("engine shutdown timeout must be positive"));
        }
        if Instant::now().checked_add(timeout).is_none() {
            return Err(SyncCoreError::invalid_args("engine shutdown timeout is too large"));
        }
        self.shutdown_timeout = timeout;
        Ok(self)
    }

    /// Finalizes the builder and returns the engine.
    pub fn build(self) -> SyncEngine {
        SyncEngine {
            status_store: self.status_store,
            callbacks: self.callbacks,
            log_progress: self.log_progress,
            height_providers: self.height_providers,
            synchronizers: self.synchronizers,
            shutdown_timeout: self.shutdown_timeout,
        }
    }
}

fn validate_id(id: &str) -> SyncCoreResult<()> {
    if id.is_empty() {
        return Err(SyncCoreError::invalid_args("handler ID must not be empty"));
    }
    if id.trim() != id {
        return Err(SyncCoreError::invalid_args(
            "handler ID must not have leading or trailing whitespace",
        ));
    }
    if id == INITIAL_HEIGHT {
        return Err(SyncCoreError::invalid_args(format!(
            "handler ID {INITIAL_HEIGHT} is reserved for the initial synced height"
        )));
    }
    Ok(())
}

use super::{ErasedHandler, Inner, Worker};
use crate::traits::DistributedHandler;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;

const DEFAULT_POLLING_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Builder for [`Worker`].
#[non_exhaustive]
pub struct Builder {
    endpoint: String,
    parallelism: Option<NonZeroUsize>,
    service_tasks_enabled: bool,
    stats_logging_period: Duration,
    polling_timeout: Duration,
    reconnect_delay: Duration,
    shutdown_timeout: Duration,
    handlers: HashMap<String, Arc<dyn ErasedHandler>>,
}

impl Builder {
    pub(super) fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            parallelism: None,
            service_tasks_enabled: false,
            stats_logging_period: Duration::ZERO,
            polling_timeout: DEFAULT_POLLING_TIMEOUT,
            reconnect_delay: DEFAULT_RECONNECT_DELAY,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            handlers: HashMap::new(),
        }
    }

    /// Overrides task parallelism, which defaults to available CPU parallelism.
    #[must_use]
    pub fn with_parallelism(mut self, parallelism: NonZeroUsize) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Enables or disables polling for service tasks.
    ///
    /// All polling loops share this eligibility. The coordinator dispatches at
    /// most one service assignment across its workers and pauses further dispatch
    /// to its owner until completion or deadline. Service execution still owns
    /// all worker task permits exclusively; other workers can process regular tasks.
    #[must_use]
    pub fn with_service_tasks_enabled(mut self, enabled: bool) -> Self {
        self.service_tasks_enabled = enabled;
        self
    }

    /// Sets the task processing summary logging period.
    ///
    /// A zero duration, which is the default, disables summary logging.
    #[must_use]
    pub fn with_stats_logging_period(mut self, period: Duration) -> Self {
        self.stats_logging_period = period;
        self
    }

    /// Sets the duration of each long-poll request.
    #[must_use]
    pub fn with_polling_timeout(mut self, timeout: Duration) -> Self {
        self.polling_timeout = timeout;
        self
    }

    /// Sets the delay between failed connection attempts.
    #[must_use]
    pub fn with_reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay;
        self
    }

    /// Sets the maximum graceful shutdown duration.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Registers one handler implementation.
    ///
    /// # Errors
    ///
    /// Returns an error when the handler ID is already registered.
    pub fn add_handler<H>(mut self, handler: H) -> SyncCoreResult<Self>
    where
        H: DistributedHandler,
    {
        let id = handler.id().to_owned();
        match self.handlers.entry(id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(handler));
                Ok(self)
            },
            Entry::Occupied(_) => Err(SyncCoreError::logic(format!("worker handler '{id}' is already registered"))),
        }
    }

    /// Validates the configuration and builds a worker without starting it.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid transport configuration, an unavailable CPU
    /// parallelism value, unavailable randomness for worker identity, or no handlers.
    pub fn build(self) -> SyncCoreResult<Worker> {
        if self.handlers.is_empty() {
            return Err(SyncCoreError::invalid_args("worker requires at least one handler"));
        }
        let endpoint = Endpoint::from_shared(self.endpoint).map_err(SyncCoreError::invalid_args)?;
        if endpoint.uri().scheme_str() != Some("http") {
            return Err(SyncCoreError::invalid_args(
                "worker endpoint must use the trusted-network http scheme",
            ));
        }
        let parallelism = match self.parallelism {
            Some(parallelism) => parallelism,
            None => std::thread::available_parallelism().map_err(SyncCoreError::system)?,
        };
        if parallelism.get() > Semaphore::MAX_PERMITS {
            return Err(SyncCoreError::invalid_args(format!(
                "worker parallelism exceeds the supported maximum of {}",
                Semaphore::MAX_PERMITS
            )));
        }
        let parallelism_u32 = u32::try_from(parallelism.get())
            .map_err(|_| SyncCoreError::invalid_args("worker parallelism exceeds u32"))?;
        // Process IDs and counters collide across containers. All polling loops
        // need one identity that also distinguishes independently built workers.
        let mut worker_id = [0; 16];
        getrandom::fill(&mut worker_id).map_err(SyncCoreError::system)?;
        let worker_id = format!("worker-{:032x}", u128::from_be_bytes(worker_id));

        Ok(Worker {
            inner: Arc::new(Inner {
                worker_id,
                endpoint,
                service_tasks_enabled: self.service_tasks_enabled,
                polling_timeout: self.polling_timeout,
                reconnect_delay: self.reconnect_delay,
                shutdown_timeout: self.shutdown_timeout,
                parallelism: parallelism_u32,
                active_tasks: Arc::new(Semaphore::new(parallelism.get())),
                handlers: self.handlers,
            }),
            stats_logging_period: self.stats_logging_period,
        })
    }
}

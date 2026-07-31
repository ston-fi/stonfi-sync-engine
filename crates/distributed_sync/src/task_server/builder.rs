#[cfg(test)]
use super::PollObserver;
use super::{DEFAULT_SHUTDOWN_TIMEOUT, TaskServer};
use crate::coordinator::Coordinator;
use crate::timeout_deadline;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::Arc;
use std::time::Duration;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use tokio::net::TcpListener;

/// Builder for [`TaskServer`].
#[non_exhaustive]
pub struct Builder {
    coordinator: Coordinator,
    listen_address: Option<SocketAddr>,
    shutdown_timeout: Duration,
}

impl Builder {
    pub(super) fn new(coordinator: Coordinator) -> Self {
        Self {
            coordinator,
            listen_address: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    /// Sets the address the server binds to.
    #[must_use]
    pub fn with_listen_address(mut self, listen_address: SocketAddr) -> Self {
        self.listen_address = Some(listen_address);
        self
    }

    /// Sets the maximum graceful shutdown duration.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, shutdown_timeout: Duration) -> Self {
        self.shutdown_timeout = shutdown_timeout;
        self
    }

    /// Binds the configured address and builds the server.
    ///
    /// # Errors
    ///
    /// Returns an error when the listen address is missing, the shutdown
    /// timeout is invalid, or the socket cannot be bound or inspected.
    ///
    /// # Panics
    ///
    /// Panics when polled outside a Tokio runtime with I/O enabled.
    pub async fn build(self) -> SyncCoreResult<TaskServer> {
        let listen_address = self
            .listen_address
            .ok_or_else(|| SyncCoreError::invalid_args("task server listen address is required"))?;
        let _ = timeout_deadline(self.shutdown_timeout, "task server shutdown timeout")?;
        let listener = TcpListener::bind(listen_address).await.map_err(SyncCoreError::net)?;
        let local_address = listener.local_addr().map_err(SyncCoreError::net)?;
        Ok(TaskServer {
            listener,
            local_address,
            coordinator: self.coordinator,
            shutdown_timeout: self.shutdown_timeout,
            #[cfg(test)]
            poll_observer: None,
        })
    }

    #[cfg(test)]
    pub(crate) async fn build_with_poll_observer(self, poll_observer: Arc<PollObserver>) -> SyncCoreResult<TaskServer> {
        let mut server = self.build().await?;
        server.poll_observer = Some(poll_observer);
        Ok(server)
    }
}

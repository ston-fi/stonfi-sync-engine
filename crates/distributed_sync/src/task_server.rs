mod builder;
mod task_service_impl;

use crate::coordinator::Coordinator;
use crate::proto::task_service_server::TaskServiceServer;
use builder::Builder;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use stonfi_metrics::MetricsCell;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::prometheus::{self, HistogramVec, IntCounterVec};
use stonfi_metrics::utils::format_duration_ms;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
pub(crate) use task_service_impl::TaskServiceImpl;
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

static METRICS: MetricsCell<TaskServerMetrics> = MetricsCell::new();

stonfi_metrics::register_metrics!(TaskServerMetrics, METRICS);

/// Bound coordinator-side gRPC server.
pub struct TaskServer {
    listener: TcpListener,
    local_address: SocketAddr,
    coordinator: Coordinator,
    shutdown_timeout: Duration,
    #[cfg(test)]
    poll_observer: Option<Arc<PollObserver>>,
}

impl TaskServer {
    /// Starts configuring a server backed by `coordinator`.
    #[must_use]
    pub fn builder(coordinator: Coordinator) -> Builder {
        Builder::new(coordinator)
    }

    /// Returns the bound socket address.
    #[must_use]
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Starts the server on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, following
    /// [`tokio::spawn`] semantics.
    #[must_use = "the run handle owns server shutdown and task completion"]
    pub fn run(self) -> TaskServerRunHandle {
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        let timeout = self.shutdown_timeout;
        let task = tokio::spawn(self.serve(shutdown));
        TaskServerRunHandle {
            cancellation,
            task,
            shutdown_timeout: timeout,
        }
    }

    async fn serve(self, cancellation: CancellationToken) -> SyncCoreResult<()> {
        let service = TaskServiceServer::new(TaskServiceImpl {
            coordinator: self.coordinator,
            #[cfg(test)]
            poll_observer: self.poll_observer,
        });
        tracing::info!("[DISTRIBUTED_SYNC][SERVER] listening on {}", self.local_address);
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(self.listener), cancellation.cancelled_owned())
            .await
            .map_err(SyncCoreError::net)
    }
}

/// Owns the task spawned by [`TaskServer::run`].
pub struct TaskServerRunHandle {
    cancellation: CancellationToken,
    task: JoinHandle<SyncCoreResult<()>>,
    shutdown_timeout: Duration,
}

impl TaskServerRunHandle {
    /// Requests graceful shutdown and waits for the server task.
    ///
    /// # Errors
    ///
    /// Returns an error on server or task failure or timeout. A timed-out task
    /// is aborted.
    pub async fn shutdown(mut self) -> SyncCoreResult<()> {
        self.cancellation.cancel();
        match tokio::time::timeout(self.shutdown_timeout, &mut self.task).await {
            Ok(result) => flatten_server_join(result),
            Err(_) => {
                self.task.abort();
                Err(SyncCoreError::system(format!(
                    "task server shutdown exceeded {:.3?}",
                    self.shutdown_timeout
                )))
            },
        }
    }

    /// Waits for natural server termination without requesting shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error on server or task failure.
    pub async fn wait(mut self) -> SyncCoreResult<()> {
        flatten_server_join((&mut self.task).await)
    }
}

impl Drop for TaskServerRunHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

fn flatten_server_join(result: Result<SyncCoreResult<()>, tokio::task::JoinError>) -> SyncCoreResult<()> {
    result.map_err(|error| SyncCoreError::system(format!("task server task failed to join: {error}")))?
}

#[cfg(test)]
pub(crate) struct PollObserver {
    count: AtomicUsize,
    changed: Notify,
}

#[cfg(test)]
impl PollObserver {
    pub(crate) fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            changed: Notify::new(),
        }
    }

    pub(super) fn poll_started(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }

    pub(crate) async fn wait_for(&self, expected: usize) {
        while self.count.load(Ordering::SeqCst) < expected {
            let changed = self.changed.notified();
            if self.count.load(Ordering::SeqCst) >= expected {
                return;
            }
            changed.await;
        }
    }
}

pub(super) struct TaskServerMetrics {
    requests: IntCounterVec,
    request_duration_ms: HistogramVec,
}

impl TaskServerMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            requests: prometheus::register_int_counter_vec!(
                "stonfi_distributed_sync_server_requests_total",
                "Distributed task server RPC outcomes",
                &["method", "status"],
            )?,
            request_duration_ms: prometheus::register_histogram_vec!(
                "stonfi_distributed_sync_server_request_duration_ms",
                "Distributed task server RPC duration in milliseconds",
                &["method", "status"],
                DURATION_BUCKETS_1MS_20S.clone(),
            )?,
        })
    }

    pub(super) fn observe(method: &str, success: bool, duration: Duration) {
        let status = if success { "ok" } else { "error" };
        METRICS.requests.with_label_values(&[method, status]).inc();
        METRICS
            .request_duration_ms
            .with_label_values(&[method, status])
            .observe(format_duration_ms(duration));
    }
}

#[cfg(test)]
mod tests {
    use super::TaskServerRunHandle;
    use std::sync::mpsc;
    use std::time::Duration;
    use stonfi_sync_core::errors::SyncCoreResult;
    use tokio_util::sync::CancellationToken;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shutdown_does_not_wait_for_aborted_task() -> anyhow::Result<()> {
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let task = tokio::spawn(async move {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            SyncCoreResult::Ok(())
        });
        started_rx.recv_timeout(Duration::from_secs(1))?;

        let handle = TaskServerRunHandle {
            cancellation: CancellationToken::new(),
            task,
            shutdown_timeout: Duration::from_millis(10),
        };
        let result = tokio::time::timeout(Duration::from_secs(1), handle.shutdown()).await;
        release_tx.send(())?;

        let shutdown = result.map_err(|_| anyhow::anyhow!("server shutdown remained blocked after abort"))?;
        assert!(shutdown.is_err());
        Ok(())
    }
}

mod builder;

use crate::coordinator::Coordinator;
use crate::proto::task_service_server::{TaskService, TaskServiceServer};
use crate::proto::{CompleteRequest, CompleteResponse, PollRequest, PollResponse};
use builder::Builder;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use stonfi_metrics::MetricsCell;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::prometheus::{self, HistogramVec, IntCounterVec};
use stonfi_metrics::utils::format_duration_ms;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

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
    ///
    /// Set a listen address before calling the builder's asynchronous `build`
    /// method.
    #[must_use]
    pub fn builder(coordinator: Coordinator) -> Builder {
        Builder::new(coordinator)
    }

    /// Returns the socket address bound by the builder's `build` method.
    #[must_use]
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// Starts the server on the current Tokio runtime.
    ///
    /// The returned handle owns the spawned server task. Dropping it requests
    /// best-effort graceful shutdown.
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
#[must_use = "dropping the run handle requests server shutdown without waiting"]
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
    /// Returns an error when the server fails, the task panics or is cancelled,
    /// or graceful shutdown exceeds the configured timeout. A timed-out server
    /// task is aborted before this method returns.
    pub async fn shutdown(mut self) -> SyncCoreResult<()> {
        self.cancellation.cancel();
        match tokio::time::timeout(self.shutdown_timeout, &mut self.task).await {
            Ok(result) => flatten_server_join(result),
            Err(_) => {
                self.task.abort();
                let _ = (&mut self.task).await;
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
    /// Returns an error when the server fails or its task panics or is
    /// cancelled.
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

pub(crate) struct TaskServiceImpl {
    coordinator: Coordinator,
    #[cfg(test)]
    poll_observer: Option<Arc<PollObserver>>,
}

impl TaskServiceImpl {
    #[cfg(test)]
    pub(crate) fn new(coordinator: Coordinator) -> Self {
        Self {
            coordinator,
            poll_observer: None,
        }
    }

    async fn poll_inner(&self, request: PollRequest) -> Result<PollResponse, Status> {
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id must not be empty"));
        }
        if request.polling_timeout_ms == 0 {
            return Err(Status::invalid_argument("polling_timeout_ms must be positive"));
        }
        #[cfg(test)]
        if let Some(observer) = &self.poll_observer {
            observer.poll_started();
        }
        let task = self
            .coordinator
            .poll(Duration::from_millis(request.polling_timeout_ms), request.service_tasks_enabled)
            .await
            .map_err(sync_error_to_status)?;
        Ok(PollResponse { task })
    }

    fn complete_inner(&self, request: CompleteRequest) -> Result<CompleteResponse, Status> {
        if request.worker_id.trim().is_empty() {
            return Err(Status::invalid_argument("worker_id must not be empty"));
        }
        self.coordinator.complete(request).map_err(sync_error_to_status)?;
        Ok(CompleteResponse {})
    }
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

    fn poll_started(&self) {
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

#[tonic::async_trait]
impl TaskService for TaskServiceImpl {
    async fn poll(&self, request: Request<PollRequest>) -> Result<Response<PollResponse>, Status> {
        let started_at = Instant::now();
        let result = self.poll_inner(request.into_inner()).await;
        TaskServerMetrics::observe("poll", result.is_ok(), started_at.elapsed());
        result.map(Response::new)
    }

    async fn complete(&self, request: Request<CompleteRequest>) -> Result<Response<CompleteResponse>, Status> {
        let started_at = Instant::now();
        let result = self.complete_inner(request.into_inner());
        TaskServerMetrics::observe("complete", result.is_ok(), started_at.elapsed());
        result.map(Response::new)
    }
}

fn sync_error_to_status(error: SyncCoreError) -> Status {
    match error {
        SyncCoreError::InvalidArgs(message) => Status::invalid_argument(message),
        SyncCoreError::NetError(message) => Status::unavailable(message),
        other => Status::internal(other.to_string()),
    }
}

struct TaskServerMetrics {
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

    fn observe(method: &str, success: bool, duration: Duration) {
        let status = if success { "ok" } else { "error" };
        METRICS.requests.with_label_values(&[method, status]).inc();
        METRICS
            .request_duration_ms
            .with_label_values(&[method, status])
            .observe(format_duration_ms(duration));
    }
}

#[cfg(test)]
use super::PollObserver;
use super::TaskServerMetrics;
use crate::coordinator::Coordinator;
use crate::proto::task_service_server::TaskService;
use crate::proto::{CompleteRequest, CompleteResponse, PollRequest, PollResponse};
#[cfg(test)]
use std::sync::Arc;
use std::time::{Duration, Instant};
use stonfi_sync_core::errors::SyncCoreError;
use tonic::{Request, Response, Status};

pub(crate) struct TaskServiceImpl {
    pub(super) coordinator: Coordinator,
    #[cfg(test)]
    pub(super) poll_observer: Option<Arc<PollObserver>>,
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
        #[cfg(test)]
        if let Some(observer) = &self.poll_observer {
            observer.poll_started();
        }
        let polling_timeout = Duration::from_millis(request.polling_timeout_ms);
        let task = self
            .coordinator
            .poll(polling_timeout, request.service_tasks_enabled)
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

#[cfg(test)]
mod tests {
    use super::TaskServiceImpl;
    use crate::coordinator::Coordinator;
    use crate::proto::PollRequest;
    use crate::utils::timeout_millis;
    use std::time::Duration;

    #[tokio::test]
    async fn test_zero_polling_timeout_round_trips_as_zero() -> anyhow::Result<()> {
        stonfi_metrics::init_metrics!()?;
        assert_eq!(timeout_millis(Duration::ZERO), 0);

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            TaskServiceImpl::new(Coordinator::new()).poll_inner(PollRequest {
                worker_id: "worker".to_owned(),
                polling_timeout_ms: 0,
                service_tasks_enabled: false,
            }),
        )
        .await??;

        assert!(response.task.is_none());
        Ok(())
    }
}

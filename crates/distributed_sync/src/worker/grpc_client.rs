use crate::proto::task_service_client::TaskServiceClient;
use crate::proto::{CompleteRequest, PollRequest, TaskAssignment};
use crate::utils::timeout_millis;
use std::time::Duration;
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use tonic::transport::{Channel, Endpoint};

pub(super) struct GrpcClient {
    client: TaskServiceClient<Channel>,
    poll_request: PollRequest,
}

impl GrpcClient {
    pub(super) async fn connect(
        endpoint: Endpoint,
        worker_id: String,
        polling_timeout: Duration,
        service_tasks_enabled: bool,
    ) -> SyncCoreResult<Self> {
        let polling_timeout_ms = timeout_millis(polling_timeout, "worker polling timeout")?;
        let channel = endpoint.connect().await.map_err(SyncCoreError::net)?;
        Ok(Self {
            client: TaskServiceClient::new(channel),
            poll_request: PollRequest {
                worker_id,
                polling_timeout_ms,
                service_tasks_enabled,
            },
        })
    }

    pub(super) async fn poll(&mut self) -> SyncCoreResult<Option<TaskAssignment>> {
        let response = self
            .client
            .poll(self.poll_request.clone())
            .await
            .map_err(SyncCoreError::net)?
            .into_inner();
        Ok(response.task)
    }

    pub(super) async fn complete(&mut self, request: CompleteRequest) -> SyncCoreResult<()> {
        self.client.complete(request).await.map_err(SyncCoreError::net)?;
        Ok(())
    }
}

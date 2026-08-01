use crate::coordinator::{Coordinator, TaskPriority};
use crate::handler::{DistributedSyncHandler, TaskPayload};
use crate::{timeout_deadline, timeout_millis};
use futures::future::try_join_all;
use std::sync::Arc;
use std::time::Duration;
use stonfi_sync_core::errors::SyncCoreResult;
use stonfi_sync_core::sync_engine::{SyncHandler, SyncHeight};

/// Adapts a [`DistributedSyncHandler`] to `stonfi_sync_core`.
#[derive(Clone)]
pub struct DistributedSynchronizer {
    handler: Arc<dyn ErasedHandler>,
    coordinator: Coordinator,
}

impl DistributedSynchronizer {
    /// Creates a coordinator-side adapter for `handler`.
    ///
    /// The same `Arc` can be registered through the builder returned by
    /// [`Worker::builder`](crate::worker::Worker::builder).
    ///
    /// # Errors
    ///
    /// Returns an error when the handler ID or synchronization timeout cannot
    /// be represented by the distributed protocol.
    pub fn new<H>(handler: Arc<H>, coordinator: Coordinator) -> SyncCoreResult<Self>
    where
        H: DistributedSyncHandler,
    {
        validate_handler_id(handler.id())?;
        let _ = timeout_millis(handler.sync_timeout(), "distributed handler synchronization timeout")?;
        Ok(Self { handler, coordinator })
    }
}

pub(crate) fn validate_handler_id(handler_id: &str) -> SyncCoreResult<()> {
    if handler_id.is_empty() {
        return Err(stonfi_sync_core::errors::SyncCoreError::invalid_args(
            "distributed handler ID must not be empty",
        ));
    }
    if handler_id.trim() != handler_id {
        return Err(stonfi_sync_core::errors::SyncCoreError::invalid_args(
            "distributed handler ID must not have leading or trailing whitespace",
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
impl SyncHandler for DistributedSynchronizer {
    fn id(&self) -> &str {
        self.handler.id()
    }

    fn initial_synced_height(&self) -> SyncHeight {
        self.handler.initial_synced_height()
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        let batch_timeout = self.handler.sync_timeout();
        let batch_deadline = timeout_deadline(batch_timeout, "distributed batch timeout")?;
        let Some((synced_height, payloads)) = self.handler.create_tasks_bytes(from, to).await? else {
            return Ok(None);
        };

        let result_payloads = try_join_all(payloads.into_iter().map(|payload| {
            self.coordinator
                .handle_task(self.handler.clone(), payload, batch_deadline, batch_timeout)
        }))
        .await?;

        self.handler.handle_results_bytes(synced_height, result_payloads).await?;
        Ok(Some(synced_height))
    }

    fn is_enabled(&self) -> bool {
        self.handler.is_enabled()
    }

    fn sleep_on_error(&self) -> Duration {
        self.handler.sleep_on_error()
    }

    fn min_sync_range(&self) -> usize {
        self.handler.min_sync_range()
    }

    fn max_sync_range(&self) -> usize {
        self.handler.max_sync_range()
    }

    fn sync_timeout(&self) -> Duration {
        self.handler.sync_timeout()
    }

    fn allow_wrap(&self) -> bool {
        self.handler.allow_wrap()
    }
}

#[async_trait::async_trait]
pub(crate) trait ErasedHandler: Send + Sync {
    fn id(&self) -> &str;
    fn initial_synced_height(&self) -> SyncHeight;
    async fn create_tasks_bytes(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<(SyncHeight, Vec<Vec<u8>>)>>;
    async fn process_task_bytes(&self, task_payload: &[u8]) -> SyncCoreResult<Vec<u8>>;
    async fn handle_results_bytes(
        &self,
        synced_height: SyncHeight,
        result_payloads: Vec<Vec<u8>>,
    ) -> SyncCoreResult<()>;
    fn task_priority(&self) -> TaskPriority;
    fn is_service_task(&self) -> bool;
    fn is_enabled(&self) -> bool;
    fn sleep_on_error(&self) -> Duration;
    fn min_sync_range(&self) -> usize;
    fn max_sync_range(&self) -> usize;
    fn sync_timeout(&self) -> Duration;
    fn allow_wrap(&self) -> bool;
}

#[async_trait::async_trait]
impl<T> ErasedHandler for T
where
    T: DistributedSyncHandler,
{
    fn id(&self) -> &str {
        DistributedSyncHandler::id(self)
    }

    fn initial_synced_height(&self) -> SyncHeight {
        DistributedSyncHandler::initial_synced_height(self)
    }

    async fn create_tasks_bytes(
        &self,
        from: SyncHeight,
        to: SyncHeight,
    ) -> SyncCoreResult<Option<(SyncHeight, Vec<Vec<u8>>)>> {
        let Some(batch) = DistributedSyncHandler::create_tasks(self, from, to).await? else {
            return Ok(None);
        };
        let (synced_height, tasks) = batch.into_parts();
        let payloads = tasks
            .into_iter()
            .map(|task| task.encode())
            .collect::<SyncCoreResult<Vec<_>>>()?;
        Ok(Some((synced_height, payloads)))
    }

    async fn process_task_bytes(&self, task_payload: &[u8]) -> SyncCoreResult<Vec<u8>> {
        let task = T::Task::decode(task_payload)?;
        DistributedSyncHandler::process_task(self, task).await?.encode()
    }

    async fn handle_results_bytes(
        &self,
        synced_height: SyncHeight,
        result_payloads: Vec<Vec<u8>>,
    ) -> SyncCoreResult<()> {
        let results = result_payloads
            .iter()
            .map(|payload| T::TaskResult::decode(payload))
            .collect::<SyncCoreResult<Vec<_>>>()?;
        DistributedSyncHandler::handle_results(self, synced_height, results).await
    }

    fn task_priority(&self) -> TaskPriority {
        DistributedSyncHandler::task_priority(self)
    }

    fn is_service_task(&self) -> bool {
        DistributedSyncHandler::is_service_task(self)
    }

    fn is_enabled(&self) -> bool {
        DistributedSyncHandler::is_enabled(self)
    }

    fn sleep_on_error(&self) -> Duration {
        DistributedSyncHandler::sleep_on_error(self)
    }

    fn min_sync_range(&self) -> usize {
        DistributedSyncHandler::min_sync_range(self)
    }

    fn max_sync_range(&self) -> usize {
        DistributedSyncHandler::max_sync_range(self)
    }

    fn sync_timeout(&self) -> Duration {
        DistributedSyncHandler::sync_timeout(self)
    }

    fn allow_wrap(&self) -> bool {
        DistributedSyncHandler::allow_wrap(self)
    }
}

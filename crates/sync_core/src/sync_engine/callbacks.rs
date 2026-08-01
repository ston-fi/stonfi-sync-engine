use crate::errors::SyncCoreResult;
use crate::sync_engine::{SyncCallback, SyncHeight};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(super) struct CallbackStore {
    pub callbacks: Vec<Arc<dyn SyncCallback>>,
}

impl CallbackStore {
    pub fn add(&mut self, callback: Arc<dyn SyncCallback>) {
        self.callbacks.push(callback);
    }
}

#[async_trait::async_trait]
impl SyncCallback for CallbackStore {
    async fn on_initiator_error(&self, id: &str, height: SyncHeight) -> SyncCoreResult<()> {
        for callback in &self.callbacks {
            callback.on_initiator_error(id, height).await?;
        }
        Ok(())
    }
    async fn on_initiator_next_height(
        &self,
        id: &str,
        prev_height: SyncHeight,
        next_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        for callback in &self.callbacks {
            callback.on_initiator_next_height(id, prev_height, next_height).await?;
        }
        Ok(())
    }
    async fn on_initiator_sent(
        &self,
        id: &str,
        prev_height: SyncHeight,
        sent_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        for callback in &self.callbacks {
            callback.on_initiator_sent(id, prev_height, sent_height).await?;
        }
        Ok(())
    }
    async fn on_sync_start(&self, sync_id: &str, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<()> {
        for callback in &self.callbacks {
            callback.on_sync_start(sync_id, from, to).await?;
        }
        Ok(())
    }
    async fn on_sync_error(&self, sync_id: &str, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<()> {
        for callback in &self.callbacks {
            callback.on_sync_error(sync_id, from, to).await?;
        }
        Ok(())
    }
    async fn on_sync_complete(
        &self,
        sync_id: &str,
        from: SyncHeight,
        to: SyncHeight,
        real_to: SyncHeight,
    ) -> SyncCoreResult<()> {
        for callback in &self.callbacks {
            callback.on_sync_complete(sync_id, from, to, real_to).await?;
        }
        Ok(())
    }
}

use crate::errors::SyncCoreResult;
use crate::sync_engine::{SyncHeight, SyncStatusStore};
use parking_lot::RwLock;
use std::collections::HashMap;

/// In-memory [`SyncStatusStore`] implementation for tests and ephemeral runs.
pub struct MemStatusStore {
    initial_synced_height: SyncHeight,
    storage: RwLock<HashMap<String, SyncHeight>>,
}

impl MemStatusStore {
    /// Creates an empty in-memory status store with the configured initial height.
    pub fn new(initial_synced_height: SyncHeight) -> Self {
        Self {
            initial_synced_height,
            storage: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemStatusStore {
    fn default() -> Self {
        Self::new(0)
    }
}

#[async_trait::async_trait]
impl SyncStatusStore for MemStatusStore {
    fn initial_synced_height(&self) -> SyncHeight {
        self.initial_synced_height
    }

    async fn save_synced_height(&self, handler_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()> {
        self.storage.write().insert(handler_id.to_owned(), sync_height);
        Ok(())
    }

    async fn load_synced_height(&self, handler_id: &str) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(self.storage.read().get(handler_id).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_save_synced_height_overwrites_existing_value() -> anyhow::Result<()> {
        let status_store = MemStatusStore::new(0);
        let id = "sync_1".to_string();

        status_store.save_synced_height(&id, 1).await?;
        status_store.save_synced_height(&id, 2).await?;

        assert_eq!(Some(2), status_store.load_synced_height(&id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_load_synced_height_returns_none_for_unknown_id() -> anyhow::Result<()> {
        let status_store = MemStatusStore::new(0);
        let id = "missing_sync".to_string();

        assert_eq!(None, status_store.load_synced_height(&id).await?);
        Ok(())
    }
}

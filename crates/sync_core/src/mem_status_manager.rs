use crate::errors::SyncCoreResult;
use crate::sync_engine::{SyncHeight, SyncStatusStore};
use parking_lot::RwLock;
use std::collections::HashMap;

/// In-memory [`SyncStatusStore`] implementation for tests and ephemeral runs.
pub struct MemStatusManager {
    storage: RwLock<HashMap<String, SyncHeight>>,
}

impl MemStatusManager {
    /// Creates an empty in-memory status manager.
    pub fn new() -> Self {
        Self {
            storage: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemStatusManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SyncStatusStore for MemStatusManager {
    async fn save_synced_height(&self, sync_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()> {
        self.storage.write().insert(sync_id.to_owned(), sync_height);
        Ok(())
    }

    async fn load_synced_height(&self, sync_id: &str) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(self.storage.read().get(sync_id).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_save_synced_height_overwrites_existing_value() -> anyhow::Result<()> {
        let status_manager = MemStatusManager::new();
        let sync_id = "sync_1".to_string();

        status_manager.save_synced_height(&sync_id, 1).await?;
        status_manager.save_synced_height(&sync_id, 2).await?;

        assert_eq!(Some(2), status_manager.load_synced_height(&sync_id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_load_synced_height_returns_none_for_unknown_id() -> anyhow::Result<()> {
        let status_manager = MemStatusManager::new();
        let sync_id = "missing_sync".to_string();

        assert_eq!(None, status_manager.load_synced_height(&sync_id).await?);
        Ok(())
    }
}

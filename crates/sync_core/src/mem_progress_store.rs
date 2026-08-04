use crate::errors::SyncCoreResult;
use crate::sync_engine::{INITIAL_HEIGHT, SyncHeight, SyncProgressStore};
use parking_lot::RwLock;
use std::collections::HashMap;

/// In-memory [`SyncProgressStore`] implementation for tests and ephemeral runs.
pub struct MemProgressStore {
    storage: RwLock<HashMap<String, SyncHeight>>,
}

impl MemProgressStore {
    /// Creates an in-memory progress store with the supplied initial height.
    pub fn new(initial_height: SyncHeight) -> Self {
        Self {
            storage: RwLock::new(HashMap::from([(INITIAL_HEIGHT.to_owned(), initial_height)])),
        }
    }
}

impl Default for MemProgressStore {
    fn default() -> Self {
        Self::new(0)
    }
}

#[async_trait::async_trait]
impl SyncProgressStore for MemProgressStore {
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
        let progress_store = MemProgressStore::new(0);
        let id = "sync_1".to_string();

        progress_store.save_synced_height(&id, 1).await?;
        progress_store.save_synced_height(&id, 2).await?;

        assert_eq!(Some(2), progress_store.load_synced_height(&id).await?);
        Ok(())
    }

    #[tokio::test]
    async fn test_load_synced_height_returns_none_for_unknown_id() -> anyhow::Result<()> {
        let progress_store = MemProgressStore::new(0);
        let id = "missing_sync".to_string();

        assert_eq!(None, progress_store.load_synced_height(&id).await?);
        Ok(())
    }
}

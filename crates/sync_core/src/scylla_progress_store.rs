//! ScyllaDB-backed synchronization progress storage.
//!
//! [`ScyllaProgressStore::builder`](crate::scylla_progress_store::ScyllaProgressStore::builder)
//! accepts either a preconfigured
//! [`ScyllaClient`](stonfi_scylla_client::client::ScyllaClient) or the endpoints
//! and existing keyspace needed to construct one. Building the store applies
//! its idempotent table migration and selects the configured keyspace before
//! returning.

use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::{SyncHeight, SyncProgressStore};
use std::error::Error;
use std::sync::Arc;
use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::errors::ScyllaClientError;

/// Configuration and construction for [`ScyllaProgressStore`].
pub mod builder;

/// Persists synchronization heights in a ScyllaDB table.
///
/// Handler IDs are stored unchanged. The CQL `bigint` height column supports
/// values through [`i64::MAX`]; larger [`SyncHeight`] values are rejected.
/// This store does not provide compare-and-set or multi-writer coordination.
pub struct ScyllaProgressStore {
    client: ScyllaClient,
    initial_height: SyncHeight,
    load_query: String,
    save_query: String,
}

impl ScyllaProgressStore {
    /// Starts configuring a store with the required initial-height fallback.
    pub fn builder(initial_height: SyncHeight) -> builder::Builder {
        builder::Builder::new(initial_height)
    }
}

#[async_trait::async_trait]
impl SyncProgressStore for ScyllaProgressStore {
    fn initial_synced_height(&self) -> SyncHeight {
        self.initial_height
    }

    async fn save_synced_height(&self, handler_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()> {
        let height = i64::try_from(sync_height).map_err(|_| {
            SyncCoreError::invalid_args(format!(
                "Scylla progress height {sync_height} exceeds the CQL bigint maximum {}",
                i64::MAX
            ))
        })?;
        self.client
            .insert(self.save_query.clone(), (handler_id, height), "save_synced_height")
            .await
            .map_err(map_scylla_error)
    }

    async fn load_synced_height(&self, handler_id: &str) -> SyncCoreResult<Option<SyncHeight>> {
        let row = self
            .client
            .select_one::<(i64,)>(self.load_query.clone(), (handler_id,), "load_synced_height")
            .await
            .map_err(map_scylla_error)?;

        row.map(|(height,)| {
            SyncHeight::try_from(height).map_err(|_| {
                SyncCoreError::system(format!(
                    "Scylla progress for handler {handler_id:?} contains negative height {height}"
                ))
            })
        })
        .transpose()
    }
}

pub(super) fn map_scylla_error(error: ScyllaClientError) -> SyncCoreError {
    let source: Arc<dyn Error + Send + Sync + 'static> = Arc::new(error);
    SyncCoreError::external(source)
}

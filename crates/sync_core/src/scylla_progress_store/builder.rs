use super::{ScyllaProgressStore, map_scylla_error};
use crate::errors::{SyncCoreError, SyncCoreResult};
use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::simple_migrator::SimpleMigrator;

const DEFAULT_TABLE_NAME: &str = "sync_progress";
const CREATE_TABLE_MIGRATION: &str = "CREATE TABLE IF NOT EXISTS [[KEYSPACE_NAME]].[[TABLE_NAME]] (\n\
    handler_id text PRIMARY KEY,\n\
    height bigint\n\
);";

/// Builds a [`ScyllaProgressStore`] from a supplied or newly created client.
///
/// Configure exactly one connection mode: either [`Self::with_scylla_client`],
/// or both [`Self::with_endpoints`] and [`Self::with_keyspace`]. The latter
/// uses the defaults provided by [`ScyllaClient::builder`].
#[must_use]
pub struct Builder {
    client: Option<ScyllaClient>,
    endpoints: Option<String>,
    keyspace: Option<String>,
    table_name: String,
}

impl Builder {
    pub(super) fn new() -> Self {
        Self {
            client: None,
            endpoints: None,
            keyspace: None,
            table_name: DEFAULT_TABLE_NAME.to_owned(),
        }
    }

    /// Uses an already configured Scylla client.
    pub fn with_scylla_client(mut self, client: ScyllaClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Sets the comma-separated Scylla contact endpoints used to create a client.
    pub fn with_endpoints(mut self, endpoints: impl Into<String>) -> Self {
        self.endpoints = Some(endpoints.into());
        self
    }

    /// Sets the existing, unquoted CQL keyspace used to create a client.
    pub fn with_keyspace(mut self, keyspace: impl Into<String>) -> Self {
        self.keyspace = Some(keyspace.into());
        self
    }

    /// Overrides the progress table name, which defaults to `sync_progress`.
    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    /// Validates the configuration, applies the table migration, and builds the store.
    ///
    /// The configured keyspace must already exist. The migration creates only
    /// the progress table and is replayed safely on every build.
    ///
    /// # Errors
    ///
    /// Returns [`SyncCoreError::InvalidArgs`] for an invalid table name or a
    /// missing, incomplete, or conflicting connection mode. Scylla client and
    /// migration failures are returned as [`SyncCoreError::External`] with
    /// their source preserved.
    pub async fn build(self) -> SyncCoreResult<ScyllaProgressStore> {
        validate_table_name(&self.table_name)?;
        let client = self.resolve_client().await?;
        let migration = CREATE_TABLE_MIGRATION.replace("[[TABLE_NAME]]", &self.table_name);
        SimpleMigrator::new(client.clone())
            .apply_all(std::slice::from_ref(&migration))
            .await
            .map_err(map_scylla_error)?;
        client.use_keyspace().await.map_err(map_scylla_error)?;
        Ok(ScyllaProgressStore {
            client,
            load_query: format!("SELECT height FROM {} WHERE handler_id = ?", self.table_name),
            save_query: format!("INSERT INTO {} (handler_id, height) VALUES (?, ?)", self.table_name),
        })
    }

    async fn resolve_client(&self) -> SyncCoreResult<ScyllaClient> {
        match (&self.client, &self.endpoints, &self.keyspace) {
            (Some(client), None, None) => Ok(client.clone()),
            (Some(_), _, _) => Err(SyncCoreError::invalid_args(
                "with_scylla_client cannot be combined with endpoints or keyspace",
            )),
            (None, Some(endpoints), Some(keyspace)) => ScyllaClient::builder(endpoints, keyspace)
                .build()
                .await
                .map_err(map_scylla_error),
            (None, None, None) => Err(SyncCoreError::invalid_args(
                "configure with_scylla_client or both endpoints and keyspace",
            )),
            (None, _, _) => Err(SyncCoreError::invalid_args(
                "endpoints and keyspace must be configured together",
            )),
        }
    }
}

fn validate_table_name(table_name: &str) -> SyncCoreResult<()> {
    let mut chars = table_name.chars();
    let valid_first = chars
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
    let valid_rest = chars.all(|character| character == '_' || character.is_ascii_alphanumeric());

    if valid_first && valid_rest {
        Ok(())
    } else {
        Err(SyncCoreError::invalid_args(
            "table name must be an unquoted CQL identifier matching [A-Za-z_][A-Za-z0-9_]*",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_build_rejects_missing_or_incomplete_connection_mode() {
        for builder in [
            Builder::new(),
            Builder::new().with_endpoints("127.0.0.1:9042"),
            Builder::new().with_keyspace("sync"),
        ] {
            assert!(matches!(builder.build().await, Err(SyncCoreError::InvalidArgs(_))));
        }
    }

    #[test]
    fn test_validate_table_name_rejects_cql_injection() {
        for table_name in [
            "",
            "1progress",
            "sync-progress",
            "sync progress",
            "sync_progress; DROP TABLE users",
        ] {
            assert!(matches!(validate_table_name(table_name), Err(SyncCoreError::InvalidArgs(_))));
        }
    }
}

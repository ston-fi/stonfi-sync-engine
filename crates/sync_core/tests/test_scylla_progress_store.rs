use std::time::Duration;

use stonfi_scylla_client::client::ScyllaClient;
use stonfi_scylla_client::simple_migrator::SimpleMigrator;
use stonfi_sync_core::errors::SyncCoreError;
use stonfi_sync_core::scylla_progress_store::ScyllaProgressStore;
use stonfi_sync_core::sync_engine::{INITIAL_HEIGHT, SyncProgressStore};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const CREATE_KEYSPACE: &str = "
CREATE KEYSPACE IF NOT EXISTS [[KEYSPACE_NAME]]
WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : [[REPLICATION_FACTOR]]}
AND durable_writes = true;
";

#[tokio::test]
async fn test_scylla_progress_store_end_to_end() -> anyhow::Result<()> {
    stonfi_metrics::init_metrics!()?;

    let container = GenericImage::new("scylladb/scylla", "6.0")
        .with_exposed_port(9042.tcp())
        .with_wait_for(WaitFor::message_on_stderr("initialization completed"))
        .with_cmd([
            "--smp",
            "1",
            "--memory",
            "512M",
            "--skip-wait-for-gossip-to-settle",
            "0",
            "--reactor-backend",
            "epoll",
            "--minimum-replication-factor-warn-threshold",
            "1",
            "--enable-tablets",
            "false",
            "--developer-mode",
            "1",
        ])
        .start()
        .await?;
    let port = container.get_host_port_ipv4(9042.tcp()).await?;

    let endpoints = format!("127.0.0.1:{port}");
    let keyspace = format!("sync_progress_store_test_{}", std::process::id());
    let client = ScyllaClient::builder(&endpoints, &keyspace)
        .with_max_parallel_queries(4)
        .with_replication_factor(1)
        .with_request_timeout(Duration::from_secs(5))
        .with_retry_count(3)
        .with_retry_min_delay(Duration::from_millis(25))
        .with_retry_max_delay(Duration::from_millis(250))
        .build()
        .await?;
    SimpleMigrator::new(client.clone())
        .apply_all(&[CREATE_KEYSPACE.to_owned()])
        .await?;

    let store = ScyllaProgressStore::builder(7)
        .with_endpoints(&endpoints)
        .with_keyspace(&keyspace)
        .build()
        .await?;
    assert_eq!(None, store.load_synced_height("missing").await?);
    assert_eq!(7, store.load_synced_or_initial("handler").await?);
    assert_eq!(Some(7), store.load_synced_height(INITIAL_HEIGHT).await?);

    store.save_synced_height("handler", 11).await?;
    store.save_synced_height("handler", 12).await?;
    assert_eq!(Some(12), store.load_synced_height("handler").await?);

    let max_height = i64::MAX as u64;
    store.save_synced_height("max", max_height).await?;
    assert_eq!(Some(max_height), store.load_synced_height("max").await?);
    assert!(matches!(
        store.save_synced_height("overflow", max_height + 1).await,
        Err(SyncCoreError::InvalidArgs(_))
    ));

    let reopened = ScyllaProgressStore::builder(0)
        .with_endpoints(&endpoints)
        .with_keyspace(&keyspace)
        .build()
        .await?;
    assert_eq!(Some(12), reopened.load_synced_height("handler").await?);

    let custom_store = ScyllaProgressStore::builder(3)
        .with_scylla_client(client.clone())
        .with_table_name("custom_sync_progress")
        .build()
        .await?;
    custom_store.save_synced_height("custom", 21).await?;
    assert_eq!(Some(21), custom_store.load_synced_height("custom").await?);

    client
        .insert(
            "INSERT INTO custom_sync_progress (handler_id, height) VALUES (?, ?)",
            ("negative", -1_i64),
            "insert_negative_progress_height",
        )
        .await?;
    assert!(matches!(
        custom_store.load_synced_height("negative").await,
        Err(SyncCoreError::System(message)) if message.contains("negative height -1")
    ));

    let conflicting = ScyllaProgressStore::builder(0)
        .with_scylla_client(client)
        .with_endpoints(&endpoints)
        .with_keyspace(&keyspace)
        .build()
        .await;
    assert!(matches!(conflicting, Err(SyncCoreError::InvalidArgs(_))));

    Ok(())
}

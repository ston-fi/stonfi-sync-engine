use stonfi_sync_core::scylla_progress_store::ScyllaProgressStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    stonfi_metrics::init_metrics!()?;

    let endpoints = std::env::var("SCYLLA_ENDPOINTS").unwrap_or_else(|_| "127.0.0.1:9042".to_owned());
    let keyspace = std::env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "sync_example".to_owned());
    let _store = ScyllaProgressStore::builder(0)
        .with_endpoints(endpoints)
        .with_keyspace(keyspace)
        .build()
        .await?;

    Ok(())
}

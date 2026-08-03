use super::_test_support::*;
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

#[tokio::test]
async fn test_shutdown_completes_while_custom_progress_sender_is_alive() -> anyhow::Result<()> {
    init_test_runtime()?;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0);
    let progress_provider = TestProgressProvider(progress_rx);
    let sync: Synchronizer = TestSync::new("live_custom_progress_provider", 0).into();
    let mut sync_progress = sync.subscribe();
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_synchronizer(sync, &[&progress_provider])?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), sync_progress.changed()).await??;
    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;

    drop(progress_tx);
    Ok(())
}

#[tokio::test]
async fn test_wait_returns_when_custom_progress_provider_closes() -> anyhow::Result<()> {
    init_test_runtime()?;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0);
    let progress_provider = TestProgressProvider(progress_rx);
    let sync: Synchronizer = TestSync::new("closing_custom_progress_provider", 0).into();
    let mut sync_progress = sync.subscribe();
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_synchronizer(sync, &[&progress_provider])?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), sync_progress.changed()).await??;
    drop(progress_tx);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await??;
    Ok(())
}

#[tokio::test]
async fn test_wait_returns_task_join_failure() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct PendingHeightLoader;

    #[async_trait::async_trait]
    impl HeightLoader for PendingHeightLoader {
        fn id(&self) -> &str {
            "pending_height_provider"
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            std::future::pending().await
        }
    }

    struct PanickingHeightLoader;

    #[async_trait::async_trait]
    impl HeightLoader for PanickingHeightLoader {
        fn id(&self) -> &str {
            "panicking_height_provider"
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            panic!("intentional task panic")
        }
    }

    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_height_provider(PendingHeightLoader)?
        .add_height_provider(PanickingHeightLoader)?
        .build();

    let result = tokio::time::timeout(Duration::from_millis(500), engine.run().wait()).await?;
    match result {
        Err(SyncCoreError::System(message)) => {
            assert!(message.contains("task failed to join"));
        },
        Err(error) => return Err(anyhow::anyhow!("unexpected run error: {error}")),
        Ok(()) => return Err(anyhow::anyhow!("task panic should be returned")),
    }
    Ok(())
}

#[tokio::test]
async fn test_shutdown_aborts_a_stuck_consumer_after_timeout() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct PendingHeightLoader {
        started: Arc<Notify>,
        dropped: Arc<AtomicBool>,
    }

    struct DropGuard(Arc<AtomicBool>);

    impl Drop for DropGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl HeightLoader for PendingHeightLoader {
        fn id(&self) -> &str {
            "pending_shutdown"
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            let _guard = DropGuard(self.dropped.clone());
            self.started.notify_one();
            std::future::pending().await
        }
    }

    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .with_shutdown_timeout(Duration::from_millis(20))?
        .add_height_provider(PendingHeightLoader {
            started: started.clone(),
            dropped: dropped.clone(),
        })?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(250), started.notified()).await?;
    let result = tokio::time::timeout(Duration::from_millis(250), run_handle.shutdown()).await?;
    match result {
        Err(SyncCoreError::System(message)) => assert!(message.contains("shutdown exceeded")),
        Err(error) => return Err(anyhow::anyhow!("unexpected shutdown error: {error}")),
        Ok(()) => return Err(anyhow::anyhow!("stuck consumer shutdown should time out")),
    }
    tokio::time::timeout(Duration::from_millis(250), async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

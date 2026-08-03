use super::_test_support::*;
use super::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct TestSyncRanged {
    id: String,
    delay: Duration,
}
impl TestSyncRanged {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSyncRanged {
    fn id(&self) -> &str {
        &self.id
    }
    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        tokio::time::sleep(self.delay).await;
        Ok(Some(to))
    }
    fn min_batch_size(&self) -> usize {
        5
    }
    fn max_batch_size(&self) -> usize {
        5
    }
}

#[tokio::test]
async fn test_sync_respects_fixed_batch_size() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init1_ranged", 5).into();
    let status_store = Arc::new(TestStatusStore::new(0));

    let id = "sync_ranged".to_string();
    let sync = TestSyncRanged::new(&id, 20);

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build();

    shutdown_engine_after(engine.run(), Duration::from_millis(300)).await?;

    let sync_statuses = status_store
        .storage
        .read()
        .get(&id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing statuses for {id}"))?;
    let mut expected_height = 0;
    for height in &sync_statuses {
        expected_height += 5;
        assert_eq!(*height, expected_height);
    }
    Ok(())
}

#[tokio::test]
async fn test_success_callbacks_are_invoked() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init1_callback", 5).into();
    let status_store = Arc::new(TestStatusStore::new(0));

    let id = "sync_callback".to_string();
    let sync = TestSyncRanged::new(&id, 20);

    const LOADED: usize = 1;
    const PUBLISHED: usize = 1 << 1;
    const SYNC_START: usize = 1 << 2;
    const SYNC_COMPLETE: usize = 1 << 3;

    struct TestCallback(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl SyncCallback for TestCallback {
        async fn on_height_loaded(&self, _: &str, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.0.fetch_or(LOADED, Ordering::SeqCst);
            Ok(())
        }

        async fn on_height_published(&self, _: &str, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.0.fetch_or(PUBLISHED, Ordering::SeqCst);
            Ok(())
        }

        async fn on_sync_start(&self, _: &str, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.0.fetch_or(SYNC_START, Ordering::SeqCst);
            Ok(())
        }

        async fn on_sync_complete(&self, _: &str, _: SyncHeight, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.0.fetch_or(SYNC_COMPLETE, Ordering::SeqCst);
            Ok(())
        }
    }

    let events = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestCallback(events.clone()));

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .add_callback(callback)
        .build();

    shutdown_engine_after(engine.run(), Duration::from_millis(300)).await?;

    assert_eq!(LOADED | PUBLISHED | SYNC_START | SYNC_COMPLETE, events.load(Ordering::SeqCst));
    Ok(())
}

struct TestSyncPartial {
    id: String,
    delay: Duration,
}
impl TestSyncPartial {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSyncPartial {
    fn id(&self) -> &str {
        &self.id
    }
    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        tokio::time::sleep(self.delay).await;
        Ok(Some(std::cmp::min(from, to)))
    }
    fn min_batch_size(&self) -> usize {
        3
    }
    fn max_batch_size(&self) -> usize {
        3
    }
}

#[tokio::test]
async fn test_partial_sync_propagates_processed_height_to_children() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init_partial", 2).into();
    let status_store = Arc::new(TestStatusStore::new(0));

    let parent_id = "sync_parent_partial".to_string();
    let child_id = "sync_child_partial".to_string();
    let sync_a: Synchronizer = TestSyncPartial::new(&parent_id, 1).into();
    let sync_a_progress = TestProgressProvider(sync_a.subscribe());
    let sync_b = TestSync::new(&child_id, 1);

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync_a, &[&height_provider])?
        .add_synchronizer(sync_b, &[&sync_a_progress])?
        .add_height_provider(height_provider)?
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    let parent_height = status_store
        .load_synced_height(&parent_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing synced height for {parent_id}"))?;
    let child_height = status_store
        .load_synced_height(&child_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing synced height for {child_id}"))?;
    assert!(parent_height > 0);
    assert!(child_height > 0);
    assert!(child_height <= parent_height);
    Ok(())
}

struct TestIgnoreThenSync {
    id: String,
    calls: Arc<AtomicUsize>,
    ranges: Arc<Mutex<Vec<(SyncHeight, SyncHeight)>>>,
}

impl TestIgnoreThenSync {
    fn new(id: &str, calls: Arc<AtomicUsize>, ranges: Arc<Mutex<Vec<(SyncHeight, SyncHeight)>>>) -> Self {
        Self {
            id: id.to_string(),
            calls,
            ranges,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestIgnoreThenSync {
    fn id(&self) -> &str {
        &self.id
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.ranges.lock().push((from, to));
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(None)
        } else {
            Ok(Some(to))
        }
    }

    fn min_batch_size(&self) -> usize {
        2
    }

    fn max_batch_size(&self) -> usize {
        4
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_ignored_range() -> anyhow::Result<()> {
    init_test_runtime()?;

    let status_store = Arc::new(TestStatusStore::new(0));
    let parent_calls = Arc::new(AtomicUsize::new(0));
    let parent_ranges = Arc::new(Mutex::new(vec![]));
    let parent_id = "sync_ignore_parent".to_string();
    let child_id = "sync_ignore_child".to_string();

    let parent_sync: Synchronizer =
        TestIgnoreThenSync::new(&parent_id, parent_calls.clone(), parent_ranges.clone()).into();
    let parent_progress = TestProgressProvider(parent_sync.subscribe());
    let child_sync = TestSync::new(&child_id, 1);

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(0);
    let progress_provider = TestProgressProvider(progress_rx);

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(parent_sync, &[&progress_provider])?
        .add_synchronizer(child_sync, &[&parent_progress])?
        .build();

    let run_handle = engine.run();

    progress_tx.send(3)?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(1, parent_calls.load(Ordering::SeqCst));
    assert_eq!(None, status_store.load_synced_height(&parent_id).await?);
    assert_eq!(None, status_store.load_synced_height(&child_id).await?);

    progress_tx.send(6)?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    drop(progress_tx);
    shutdown_engine_after(run_handle, Duration::from_millis(50)).await?;

    // The configured range limits intentionally produce this sequence.
    assert_eq!(3, parent_calls.load(Ordering::SeqCst));
    assert_eq!(vec![(1, 3), (1, 4), (5, 6)], *parent_ranges.lock());
    assert_eq!(Some(6), status_store.load_synced_height(&parent_id).await?);
    assert!(status_store.load_synced_height(&child_id).await?.unwrap_or_default() > 0);
    Ok(())
}

#[tokio::test]
async fn test_reenabled_sync_uses_existing_upstream_height() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct EnabledSync {
        id: String,
        enabled: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl SyncHandler for EnabledSync {
        fn id(&self) -> &str {
            &self.id
        }

        fn is_enabled(&self) -> bool {
            self.enabled.load(Ordering::SeqCst)
        }

        async fn sync_range(&mut self, _: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            Ok(Some(to))
        }
    }

    let enabled = Arc::new(AtomicBool::new(false));
    let id = "reenabled_sync".to_string();
    let sync: Synchronizer = EnabledSync {
        id: id.clone(),
        enabled: enabled.clone(),
    }
    .into();
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(1);
    let progress_provider = TestProgressProvider(progress_rx);
    let status_store = Arc::new(TestStatusStore::new(0));
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&progress_provider])?
        .build();

    let run_handle = engine.run();
    tokio::time::sleep(Duration::from_millis(50)).await;
    enabled.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_millis(1500), async {
        while status_store.load_synced_height(&id).await? != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<(), SyncCoreError>(())
    })
    .await??;

    drop(progress_tx);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await??;
    Ok(())
}

struct TestSyncFailFirst {
    id: String,
    attempts: Arc<AtomicUsize>,
}
impl TestSyncFailFirst {
    fn new(id: &str, attempts: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            attempts,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSyncFailFirst {
    fn id(&self) -> &str {
        &self.id
    }
    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(SyncCoreError::custom("fail_once"))
        } else {
            Ok(Some(to))
        }
    }
    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }
}

struct TestSyncErrorCallback {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SyncCallback for TestSyncErrorCallback {
    async fn on_sync_error(&self, _: &str, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_on_sync_error_callback_is_invoked() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init_sync_error_callback", 2).into();
    let status_store = Arc::new(TestStatusStore::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let id = "sync_error_callback".to_string();
    let sync = TestSyncFailFirst::new(&id, attempts);
    let sync_error_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestSyncErrorCallback {
        calls: sync_error_calls.clone(),
    });

    let engine = SyncEngine::builder(status_store)
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .add_callback(callback)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    assert!(sync_error_calls.load(Ordering::SeqCst) > 0);
    Ok(())
}

struct TestSyncInvalidFirst {
    id: String,
    calls: Arc<AtomicUsize>,
}
impl TestSyncInvalidFirst {
    fn new(id: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSyncInvalidFirst {
    fn id(&self) -> &str {
        &self.id
    }
    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Some(to + 1))
        } else {
            Ok(Some(std::cmp::max(from, to)))
        }
    }
    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_invalid_synced_height_is_retried_and_not_saved() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init_invalid_height", 2).into();
    let status_store = Arc::new(TestStatusStore::new(0));
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let id = "sync_invalid_height".to_string();
    let sync = TestSyncInvalidFirst::new(&id, sync_calls.clone());

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    let statuses = status_store.storage.read().get(&id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(1, statuses[0]);
    assert!(sync_calls.load(Ordering::SeqCst) >= 2);
    Ok(())
}

struct TestSyncTimesOutFirst {
    ranges: Arc<Mutex<Vec<(SyncHeight, SyncHeight)>>>,
}

#[async_trait::async_trait]
impl SyncHandler for TestSyncTimesOutFirst {
    fn id(&self) -> &str {
        "sync_timeout_retry"
    }

    async fn sync_range(&mut self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        let first_attempt = {
            let mut ranges = self.ranges.lock();
            ranges.push((from, to));
            ranges.len() == 1
        };
        if first_attempt {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(Some(to))
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(1)
    }

    fn sync_timeout(&self) -> Duration {
        Duration::from_millis(10)
    }
}

#[tokio::test]
async fn test_timed_out_range_is_retried_at_least_once() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init_timeout_retry", 2).into();
    let status_store = Arc::new(TestStatusStore::new(0));
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let sync: Synchronizer = TestSyncTimesOutFirst { ranges: ranges.clone() }.into();
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build();
    let run_handle = engine.run();

    tokio::time::timeout(Duration::from_millis(500), async {
        while status_store.storage.read().get("sync_timeout_retry").is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    run_handle.shutdown().await?;

    let ranges = ranges.lock();
    assert!(ranges.len() >= 2);
    assert_eq!(ranges[0], ranges[1]);
    Ok(())
}

struct TestRewindSync {
    id: String,
    calls: Arc<AtomicUsize>,
}

impl TestRewindSync {
    fn new(id: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestRewindSync {
    fn id(&self) -> &str {
        &self.id
    }

    async fn sync_range(&mut self, from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(from - 1))
    }

    fn allow_rewind(&self) -> bool {
        true
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_allow_rewind_accepts_rewound_height_without_retry() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct StopAfterFirstSyncCallback {
        progress_sender: Arc<Mutex<Option<tokio::sync::watch::Sender<SyncHeight>>>>,
    }

    #[async_trait::async_trait]
    impl SyncCallback for StopAfterFirstSyncCallback {
        async fn on_sync_complete(&self, _: &str, _: SyncHeight, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.progress_sender.lock().take();
            Ok(())
        }
    }

    let status_store = Arc::new(TestStatusStore::new(0));
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let id = "sync_allow_rewind".to_string();
    let sync = TestRewindSync::new(&id, sync_calls.clone());
    let (progress_tx, progress_rx) = tokio::sync::watch::channel(1);
    let progress_provider = TestProgressProvider(progress_rx);
    let callback = Arc::new(StopAfterFirstSyncCallback {
        progress_sender: Arc::new(Mutex::new(Some(progress_tx))),
    });

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&progress_provider])?
        .add_callback(callback)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(200)).await?;

    let statuses = status_store.storage.read().get(&id).cloned().unwrap_or_default();
    assert_eq!(vec![0], statuses);
    assert_eq!(1, sync_calls.load(Ordering::SeqCst));
    Ok(())
}

struct TestNoRewindSync {
    id: String,
    calls: Arc<AtomicUsize>,
}

impl TestNoRewindSync {
    fn new(id: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestNoRewindSync {
    fn id(&self) -> &str {
        &self.id
    }

    async fn sync_range(&mut self, from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Some(from - 1))
        } else {
            Ok(Some(from))
        }
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_rewound_height_is_retried_when_allow_rewind_is_false() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init_no_rewind", 2).into();
    let status_store = Arc::new(TestStatusStore::new(0));
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let id = "sync_no_rewind".to_string();
    let sync = TestNoRewindSync::new(&id, sync_calls.clone());

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .with_log_progress(|_from, to| to % 10 == 0)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    let statuses = status_store.storage.read().get(&id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(1, statuses[0]);
    assert!(sync_calls.load(Ordering::SeqCst) >= 2);
    Ok(())
}

struct TestStepHeightLoader {
    id: String,
    latest_height_calls: Arc<AtomicUsize>,
}

impl TestStepHeightLoader {
    fn new(id: &str, latest_height_calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            latest_height_calls,
        }
    }
}

#[async_trait::async_trait]
impl HeightLoader for TestStepHeightLoader {
    fn id(&self) -> &str {
        &self.id
    }
    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.latest_height_calls.fetch_add(1, Ordering::SeqCst);
        Ok(match after {
            0 => 1,
            1 => 2,
            _ => after,
        })
    }
}

struct TestCompleteCallback {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SyncCallback for TestCompleteCallback {
    async fn on_sync_complete(&self, _: &str, _: SyncHeight, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(SyncCoreError::custom("fail_once_complete"))
        } else {
            Ok(())
        }
    }
}

struct TestCountingSync {
    id: String,
    calls: Arc<AtomicUsize>,
}

impl TestCountingSync {
    fn new(id: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestCountingSync {
    fn id(&self) -> &str {
        &self.id
    }

    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(to))
    }

    fn retry_delay(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_on_sync_complete_callback_failure_does_not_rerun_sync_range() -> anyhow::Result<()> {
    init_test_runtime()?;
    let status_store = Arc::new(TestStatusStore::new(0));
    let height_loader_calls = Arc::new(AtomicUsize::new(0));
    let height_provider: HeightProvider =
        TestStepHeightLoader::new("test_init_complete_callback", height_loader_calls.clone()).into();
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync = TestCountingSync::new("sync_complete_callback", sync_calls.clone());
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestCompleteCallback {
        calls: callback_calls.clone(),
    });

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .add_callback(callback)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(250)).await?;

    assert_eq!(2, sync_calls.load(Ordering::SeqCst));
    assert_eq!(3, callback_calls.load(Ordering::SeqCst));
    assert!(height_loader_calls.load(Ordering::SeqCst) >= 2);
    let saved = status_store
        .storage
        .read()
        .get("sync_complete_callback")
        .cloned()
        .unwrap_or_default();
    assert_eq!(vec![1, 2], saved);
    Ok(())
}

#[tokio::test]
async fn test_missing_persisted_height_uses_and_stores_configured_initial_height() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("test_init_initial_height", 2).into();
    let status_store = Arc::new(TestStatusStore::new(7));
    let id = "sync_initial_height".to_string();
    let sync = TestSync::new(&id, 0);

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(250)).await?;

    let statuses = status_store.storage.read().get(&id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(8, statuses[0]);
    assert_eq!(Some(7), status_store.load_synced_height(INITIAL_HEIGHT).await?);
    Ok(())
}

#[tokio::test]
async fn test_persisted_sync_and_initial_heights_take_precedence_over_config() -> anyhow::Result<()> {
    let status_store = TestStatusStore::new(99);
    status_store.save_synced_height(INITIAL_HEIGHT, 7).await?;

    assert_eq!(7, status_store.load_synced_or_initial("missing_sync").await?);

    status_store.save_synced_height("persisted_sync", 11).await?;
    assert_eq!(11, status_store.load_synced_or_initial("persisted_sync").await?);
    Ok(())
}

#[tokio::test]
async fn test_initial_height_save_failure_is_retried() -> anyhow::Result<()> {
    init_test_runtime()?;
    let height_provider: HeightProvider = TestHeightLoader::new("initial_save_retry_progress", 2).into();
    let status_store = Arc::new(TestStatusStore::new(3).with_initial_save_failures(1));
    let sync: Synchronizer = TestSync::new("initial_save_retry_sync", 0).into();

    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build();
    shutdown_engine_after(engine.run(), Duration::from_millis(500)).await?;

    assert_eq!(0, status_store.initial_save_failures.load(Ordering::SeqCst));
    assert_eq!(Some(3), status_store.load_synced_height(INITIAL_HEIGHT).await?);
    assert!(status_store.load_synced_height("initial_save_retry_sync").await?.is_some());
    Ok(())
}

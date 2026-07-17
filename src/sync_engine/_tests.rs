use super::*;
use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::traits::{SyncHandler, SyncInitiator, SyncTrigger};
use assertables::assert_gt;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

static LOG_INIT: std::sync::Once = std::sync::Once::new();
pub(crate) fn init_logging() {
    LOG_INIT.call_once(|| {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Warn)
            .try_init();
    });
}

struct TestInitiator {
    id: String,
    delay: Duration,
}
impl TestInitiator {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
        }
    }
}

#[async_trait::async_trait]
impl SyncInitiator for TestInitiator {
    fn id(&self) -> &SyncID {
        &self.id
    }
    async fn last_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        tokio::time::sleep(self.delay).await;
        Ok(after + 1)
    }
}

struct TestSync {
    id: String,
    delay: Duration,
}
impl TestSync {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSync {
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }
    async fn sync_range(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        tokio::time::sleep(self.delay).await;
        Ok(Some(to))
    }
}

struct TestStatusManager {
    storage: RwLock<HashMap<SyncID, Vec<SyncHeight>>>,
}

impl TestStatusManager {
    pub fn new() -> Self {
        Self {
            storage: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl SyncStatusManager for TestStatusManager {
    async fn save_synced_height(&self, sync_id: &SyncID, sync_height: SyncHeight) -> SyncCoreResult<()> {
        self.storage.write().entry(sync_id.clone()).or_default().push(sync_height);
        Ok(())
    }

    async fn load_synced_height(&self, sync_id: &SyncID) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(self.storage.read().get(sync_id).and_then(|heights| heights.last().copied()))
    }
}

#[tokio::test]
async fn test_sync_engine() -> anyhow::Result<()> {
    init_logging();
    let test_init1 = TestInitiator::new("test_init1_base", 5).into();
    let test_init2 = TestInitiator::new("test_init2_base", 10).into();

    let status_manager = Arc::new(TestStatusManager::new());

    let sync_id = "test_sync_1_base".to_string();
    let test_sync1 = TestSync::new(&sync_id, 50).into();

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(test_sync1, &[&test_init1, &test_init2])?
        .add_initiator(test_init1)?
        .add_initiator(test_init2)?
        .build();

    engine.run();
    shutdown_engine(engine).await;

    let sync_statuses = status_manager.storage.read().get(&sync_id).unwrap().clone();
    let synced_height = status_manager.load_synced_height(&sync_id).await?.unwrap();
    assert_eq!(sync_statuses.last().unwrap(), &synced_height);
    assert_gt!(synced_height, 0);
    let mut prev_height = *sync_statuses.first().unwrap();
    for height in sync_statuses.iter().skip(1) {
        assert_gt!(*height, prev_height);
        prev_height = *height;
    }
    Ok(())
}

#[tokio::test]
async fn test_initial_initiator_height_is_published() -> anyhow::Result<()> {
    struct FixedInitiator {
        id: SyncID,
    }

    #[async_trait::async_trait]
    impl SyncInitiator for FixedInitiator {
        fn id(&self) -> &SyncID {
            &self.id
        }

        fn sleep_on_error(&self) -> Duration {
            Duration::from_millis(10)
        }

        async fn last_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            Ok(7)
        }
    }

    let status_manager = Arc::new(TestStatusManager::new());
    let initiator: Initiator = FixedInitiator {
        id: "fixed_initial_height".to_string(),
    }
    .into();
    let sync_id = "sync_initial_publish".to_string();
    let sync = TestSync::new(&sync_id, 0).into();
    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initiator])?
        .add_initiator(initiator)?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), async {
        while status_manager.load_synced_height(&sync_id).await? != Some(7) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<(), SyncCoreError>(())
    })
    .await??;

    drop(engine);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await?;
    Ok(())
}

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
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }
    async fn sync_range(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        tokio::time::sleep(self.delay).await;
        Ok(Some(to))
    }
    fn min_sync_range(&self) -> usize {
        5
    }
    fn max_sync_range(&self) -> usize {
        5
    }
}

#[tokio::test]
async fn test_sync_engine_ranged() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init1_ranged", 5).into();
    let status_manager = Arc::new(TestStatusManager::new());

    let sync_id = "sync_ranged".to_string();
    let sync = TestSyncRanged::new(&sync_id, 20).into();

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();

    engine.run();
    shutdown_engine(engine).await;

    let sync_statuses = status_manager.storage.read().get(&sync_id).unwrap().clone();
    let mut expected_height = 0;
    for height in &sync_statuses {
        expected_height += 5;
        assert_eq!(*height, expected_height);
    }
    Ok(())
}

#[tokio::test]
async fn test_sync_engine_with_callback() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init1_callback", 5).into();
    let status_manager = Arc::new(TestStatusManager::new());

    let sync_id = "sync_callback".to_string();
    let sync = TestSyncRanged::new(&sync_id, 20).into();

    struct TestCallback(Arc<DashMap<String, usize>>);
    #[async_trait::async_trait]
    impl SyncCallback for TestCallback {
        async fn on_initiator_error(&self, _: &SyncID, _: SyncHeight) -> SyncCoreResult<()> {
            let mut counter = self.0.entry("error_count".to_string()).or_default();
            *counter.value_mut() += 1;
            Ok(())
        }
        async fn on_initiator_next_height(&self, _: &SyncID, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            let mut counter = self.0.entry("next_height_count".to_string()).or_default();
            *counter.value_mut() += 1;
            Ok(())
        }
        async fn on_initiator_sent(&self, _: &SyncID, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            let mut counter = self.0.entry("sent_count".to_string()).or_default();
            *counter.value_mut() += 1;
            Ok(())
        }
        async fn on_sync_start(&self, _: &SyncID, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            let mut counter = self.0.entry("sync_start_count".to_string()).or_default();
            *counter.value_mut() += 1;
            Ok(())
        }

        async fn on_sync_error(&self, _: &SyncID, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            let mut counter = self.0.entry("sync_error_count".to_string()).or_default();
            *counter.value_mut() += 1;
            Ok(())
        }
        async fn on_sync_complete(
            &self,
            _: &SyncID,
            _: SyncHeight,
            _: SyncHeight,
            _: SyncHeight,
        ) -> SyncCoreResult<()> {
            let mut counter = self.0.entry("sync_complete_count".to_string()).or_default();
            *counter.value_mut() += 1;
            Ok(())
        }
    }
    let store = Arc::new(DashMap::<String, usize>::new());
    let callback = Arc::new(TestCallback(store.clone()));

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initializer])?
        .add_initiator(initializer)?
        .add_callback(callback)?
        .build();

    engine.run();
    shutdown_engine(engine).await;

    assert_eq!(store.len(), 4);
    for value in store.iter().map(|x| *x.value()) {
        assert_gt!(value, 0);
    }
    Ok(())
}

#[tokio::test]
async fn test_builder_rejects_duplicate_sync_ids() -> anyhow::Result<()> {
    let initializer: Initiator = TestInitiator::new("test_init_dup_sync", 5).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_1 = TestSync::new("sync_duplicate", 5).into();
    let sync_2 = TestSync::new("sync_duplicate", 5).into();

    let builder = SyncEngine::builder(status_manager)?.add_sync(sync_1, &[&initializer])?;
    let err = match builder.add_sync(sync_2, &[&initializer]) {
        Ok(_) => return Err(anyhow::anyhow!("duplicate sync id should fail")),
        Err(err) => err,
    };
    assert!(matches!(err, SyncCoreError::Logic(_)));

    let init_1 = TestInitiator::new("duplicate_entity", 5).into();
    let init_2 = TestInitiator::new("duplicate_entity", 5).into();
    let builder = SyncEngine::builder(Arc::new(TestStatusManager::new()))?.add_initiator(init_1)?;
    assert!(matches!(builder.add_initiator(init_2), Err(SyncCoreError::Logic(_))));

    let trigger: Initiator = TestInitiator::new("trigger", 5).into();
    let sync = TestSync::new("shared_entity", 5).into();
    let colliding_initiator = TestInitiator::new("shared_entity", 5).into();
    let builder = SyncEngine::builder(Arc::new(TestStatusManager::new()))?.add_sync(sync, &[&trigger])?;
    assert!(matches!(
        builder.add_initiator(colliding_initiator),
        Err(SyncCoreError::Logic(_))
    ));
    Ok(())
}

#[tokio::test]
async fn test_builder_rejects_invalid_sync_ranges() -> anyhow::Result<()> {
    struct InvalidRangeSync {
        id: SyncID,
        min: usize,
        max: usize,
    }

    #[async_trait::async_trait]
    impl SyncHandler for InvalidRangeSync {
        fn id(&self) -> &SyncID {
            &self.id
        }

        fn initial_synced_height(&self) -> SyncHeight {
            0
        }

        async fn sync_range(&self, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            Ok(None)
        }

        fn min_sync_range(&self) -> usize {
            self.min
        }

        fn max_sync_range(&self) -> usize {
            self.max
        }
    }

    let trigger: Initiator = TestInitiator::new("range_trigger", 5).into();
    let mut invalid_ranges = vec![(0, 1), (1, 0), (2, 1)];
    if let Ok(oversized) = usize::try_from(u64::from(SyncHeight::MAX) + 1) {
        invalid_ranges.push((1, oversized));
    }

    for (index, (min, max)) in invalid_ranges.into_iter().enumerate() {
        let sync: Synchronizer = InvalidRangeSync {
            id: format!("invalid_range_{index}"),
            min,
            max,
        }
        .into();
        let builder = SyncEngine::builder(Arc::new(TestStatusManager::new()))?;
        assert!(matches!(builder.add_sync(sync, &[&trigger]), Err(SyncCoreError::Logic(_))));
    }
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
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }
    async fn sync_range(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        tokio::time::sleep(self.delay).await;
        Ok(Some(std::cmp::min(from, to)))
    }
    fn min_sync_range(&self) -> usize {
        3
    }
    fn max_sync_range(&self) -> usize {
        3
    }
}

#[tokio::test]
async fn test_partial_sync_propagates_real_height_to_children() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init_partial", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());

    struct TestTrigger(SyncReceiver);
    impl SyncTrigger for TestTrigger {
        fn receiver(&self) -> SyncReceiver {
            self.0.clone()
        }
    }

    let sync_a_id = "sync_parent_partial".to_string();
    let sync_b_id = "sync_child_partial".to_string();
    let sync_a: Synchronizer = TestSyncPartial::new(&sync_a_id, 1).into();
    let sync_a_trigger = TestTrigger(sync_a.receiver());
    let sync_b = TestSync::new(&sync_b_id, 1).into();

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync_a, &[&initializer])?
        .add_sync(sync_b, &[&sync_a_trigger])?
        .add_initiator(initializer)?
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(400)).await;

    let parent_height = status_manager.load_synced_height(&sync_a_id).await?.unwrap();
    let child_height = status_manager.load_synced_height(&sync_b_id).await?.unwrap();
    assert_gt!(parent_height, 0);
    assert_gt!(child_height, 0);
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
    fn id(&self) -> &SyncID {
        &self.id
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.ranges.lock().push((from, to));
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(None)
        } else {
            Ok(Some(to))
        }
    }

    fn min_sync_range(&self) -> usize {
        2
    }

    fn max_sync_range(&self) -> usize {
        4
    }

    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_ignored_range() -> anyhow::Result<()> {
    init_logging();

    struct TestFixedTrigger(SyncReceiver);
    impl SyncTrigger for TestFixedTrigger {
        fn receiver(&self) -> SyncReceiver {
            self.0.clone()
        }
    }

    struct TestParentTrigger(SyncReceiver);
    impl SyncTrigger for TestParentTrigger {
        fn receiver(&self) -> SyncReceiver {
            self.0.clone()
        }
    }

    let status_manager = Arc::new(TestStatusManager::new());
    let parent_calls = Arc::new(AtomicUsize::new(0));
    let parent_ranges = Arc::new(Mutex::new(vec![]));
    let parent_sync_id = "sync_ignore_parent".to_string();
    let child_sync_id = "sync_ignore_child".to_string();

    let parent_sync: Synchronizer =
        TestIgnoreThenSync::new(&parent_sync_id, parent_calls.clone(), parent_ranges.clone()).into();
    let parent_trigger = TestParentTrigger(parent_sync.receiver());
    let child_sync = TestSync::new(&child_sync_id, 1).into();

    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(0);
    let trigger = TestFixedTrigger(trigger_rx);

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(parent_sync, &[&trigger])?
        .add_sync(child_sync, &[&parent_trigger])?
        .build();

    engine.run();

    trigger_tx.send(3)?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(None, status_manager.load_synced_height(&parent_sync_id).await?);
    assert_eq!(None, status_manager.load_synced_height(&child_sync_id).await?);

    trigger_tx.send(6)?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    drop(trigger_tx);
    shutdown_engine_after(engine, Duration::from_millis(50)).await;

    // min_max range impl were choosen to behave like that
    assert_eq!(3, parent_calls.load(Ordering::SeqCst));
    assert_eq!(vec![(1, 3), (1, 4), (5, 6)], *parent_ranges.lock());
    assert_eq!(Some(6), status_manager.load_synced_height(&parent_sync_id).await?);
    assert_gt!(status_manager.load_synced_height(&child_sync_id).await?.unwrap_or_default(), 0);
    Ok(())
}

#[tokio::test]
async fn test_ignored_range_waits_for_new_upstream_height() -> anyhow::Result<()> {
    struct FixedTrigger(SyncReceiver);
    impl SyncTrigger for FixedTrigger {
        fn receiver(&self) -> SyncReceiver {
            self.0.clone()
        }
    }

    struct AlwaysIgnore {
        id: SyncID,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SyncHandler for AlwaysIgnore {
        fn id(&self) -> &SyncID {
            &self.id
        }

        fn initial_synced_height(&self) -> SyncHeight {
            0
        }

        async fn sync_range(&self, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn max_sync_range(&self) -> usize {
            2
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let sync: Synchronizer = AlwaysIgnore {
        id: "always_ignore".to_string(),
        calls: calls.clone(),
    }
    .into();
    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(6);
    let trigger = FixedTrigger(trigger_rx);
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))?
        .add_sync(sync, &[&trigger])?
        .build();

    let run_handle = engine.run();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(1, calls.load(Ordering::SeqCst));

    drop(trigger_tx);
    drop(engine);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await?;
    Ok(())
}

#[tokio::test]
async fn test_reenabled_sync_uses_existing_upstream_height() -> anyhow::Result<()> {
    struct FixedTrigger(SyncReceiver);
    impl SyncTrigger for FixedTrigger {
        fn receiver(&self) -> SyncReceiver {
            self.0.clone()
        }
    }

    struct EnabledSync {
        id: SyncID,
        enabled: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl SyncHandler for EnabledSync {
        fn id(&self) -> &SyncID {
            &self.id
        }

        fn initial_synced_height(&self) -> SyncHeight {
            0
        }

        fn is_enabled(&self) -> bool {
            self.enabled.load(Ordering::SeqCst)
        }

        async fn sync_range(&self, _: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            Ok(Some(to))
        }
    }

    let enabled = Arc::new(AtomicBool::new(false));
    let sync_id = "reenabled_sync".to_string();
    let sync: Synchronizer = EnabledSync {
        id: sync_id.clone(),
        enabled: enabled.clone(),
    }
    .into();
    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(1);
    let trigger = FixedTrigger(trigger_rx);
    let status_manager = Arc::new(TestStatusManager::new());
    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&trigger])?
        .build();

    let run_handle = engine.run();
    tokio::time::sleep(Duration::from_millis(50)).await;
    enabled.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_millis(1500), async {
        while status_manager.load_synced_height(&sync_id).await? != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<(), SyncCoreError>(())
    })
    .await??;

    drop(trigger_tx);
    drop(engine);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await?;
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
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }
    async fn sync_range(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(SyncCoreError::custom("fail_once"))
        } else {
            Ok(Some(to))
        }
    }
    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }
}

struct TestSyncErrorCallback {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SyncCallback for TestSyncErrorCallback {
    async fn on_sync_error(&self, _: &SyncID, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn test_on_sync_error_callback_is_invoked() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init_sync_error_callback", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_error_callback".to_string();
    let sync = TestSyncFailFirst::new(&sync_id, attempts).into();
    let sync_error_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestSyncErrorCallback {
        calls: sync_error_calls.clone(),
    });

    let engine = SyncEngine::builder(status_manager)?
        .add_sync(sync, &[&initializer])?
        .add_initiator(initializer)?
        .add_callback(callback)?
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(400)).await;

    assert_gt!(sync_error_calls.load(Ordering::SeqCst), 0);
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
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }
    async fn sync_range(&self, from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Some(to + 1))
        } else {
            Ok(Some(std::cmp::max(from, to)))
        }
    }
    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_invalid_synced_height_is_retried_and_not_saved() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init_invalid_height", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_invalid_height".to_string();
    let sync = TestSyncInvalidFirst::new(&sync_id, sync_calls.clone()).into();

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(400)).await;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(1, statuses[0]);
    assert!(sync_calls.load(Ordering::SeqCst) >= 2);
    Ok(())
}

struct TestWrapSync {
    id: String,
    calls: Arc<AtomicUsize>,
}

impl TestWrapSync {
    fn new(id: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestWrapSync {
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&self, from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(from - 1))
    }

    fn allow_wrap(&self) -> bool {
        true
    }

    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_allow_wrap_accepts_wrapped_height_without_retry() -> anyhow::Result<()> {
    init_logging();

    struct TestFixedTrigger(SyncReceiver);
    impl SyncTrigger for TestFixedTrigger {
        fn receiver(&self) -> SyncReceiver {
            self.0.clone()
        }
    }

    struct StopAfterFirstSyncCallback {
        trigger_sender: Arc<Mutex<Option<tokio::sync::watch::Sender<SyncHeight>>>>,
    }

    #[async_trait::async_trait]
    impl SyncCallback for StopAfterFirstSyncCallback {
        async fn on_sync_complete(
            &self,
            _: &SyncID,
            _: SyncHeight,
            _: SyncHeight,
            _: SyncHeight,
        ) -> SyncCoreResult<()> {
            self.trigger_sender.lock().take();
            Ok(())
        }
    }

    let status_manager = Arc::new(TestStatusManager::new());
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_allow_wrap".to_string();
    let sync = TestWrapSync::new(&sync_id, sync_calls.clone()).into();
    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(1);
    let trigger = TestFixedTrigger(trigger_rx);
    let callback = Arc::new(StopAfterFirstSyncCallback {
        trigger_sender: Arc::new(Mutex::new(Some(trigger_tx))),
    });

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&trigger])?
        .add_callback(callback)?
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(200)).await;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
    assert_eq!(vec![0], statuses);
    assert_eq!(1, sync_calls.load(Ordering::SeqCst));
    Ok(())
}

struct TestNoWrapSync {
    id: String,
    calls: Arc<AtomicUsize>,
}

impl TestNoWrapSync {
    fn new(id: &str, calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestNoWrapSync {
    fn id(&self) -> &SyncID {
        &self.id
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&self, from: SyncHeight, _to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Some(from - 1))
        } else {
            Ok(Some(from))
        }
    }

    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_wrapped_height_is_retried_when_allow_wrap_is_false() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init_no_wrap", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_no_wrap".to_string();
    let sync = TestNoWrapSync::new(&sync_id, sync_calls.clone()).into();

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initializer])?
        .add_initiator(initializer)?
        .with_log_progress(|_from, to| to % 10 == 0)
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(400)).await;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(1, statuses[0]);
    assert!(sync_calls.load(Ordering::SeqCst) >= 2);
    Ok(())
}

struct TestInitiatorFast {
    id: String,
    sleep_on_error: Duration,
}
impl TestInitiatorFast {
    fn new(id: &str, sleep_on_error_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            sleep_on_error: Duration::from_millis(sleep_on_error_ms),
        }
    }
}

#[async_trait::async_trait]
impl SyncInitiator for TestInitiatorFast {
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn sleep_on_error(&self) -> Duration {
        self.sleep_on_error
    }
    async fn last_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        Ok(after + 1)
    }
}

struct TestFailingNextHeightCallback {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl SyncCallback for TestFailingNextHeightCallback {
    async fn on_initiator_next_height(&self, _: &SyncID, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(SyncCoreError::custom("fail_next_height"))
    }
}

#[tokio::test]
async fn test_initiator_callback_failure_uses_backoff() -> anyhow::Result<()> {
    init_logging();
    let status_manager = Arc::new(TestStatusManager::new());
    let initiator = TestInitiatorFast::new("test_init_callback_backoff", 50).into();
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestFailingNextHeightCallback {
        calls: callback_calls.clone(),
    });

    let engine = SyncEngine::builder(status_manager)?
        .add_initiator(initiator)?
        .add_callback(callback)?
        .build();

    let run_handle = engine.run();
    tokio::time::sleep(Duration::from_millis(260)).await;
    drop(engine);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await?;

    let calls = callback_calls.load(Ordering::SeqCst);
    assert_gt!(calls, 0);
    assert!(calls <= 12, "callback retries are too frequent: {calls}");
    Ok(())
}

#[tokio::test]
async fn test_initiator_callback_retries_same_event() -> anyhow::Result<()> {
    struct StepInitiator {
        id: SyncID,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SyncInitiator for StepInitiator {
        fn id(&self) -> &SyncID {
            &self.id
        }

        fn sleep_on_error(&self) -> Duration {
            Duration::from_millis(10)
        }

        async fn last_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(if call == 0 { 1 } else { 2 })
        }
    }

    struct RetryCallback {
        attempts: AtomicUsize,
        events: Mutex<Vec<(SyncHeight, SyncHeight)>>,
        second_attempt: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl SyncCallback for RetryCallback {
        async fn on_initiator_next_height(
            &self,
            _: &SyncID,
            previous_height: SyncHeight,
            next_height: SyncHeight,
        ) -> SyncCoreResult<()> {
            self.events.lock().push((previous_height, next_height));
            match self.attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Err(SyncCoreError::custom("retry same event")),
                1 => {
                    self.second_attempt.notify_one();
                    self.release.notified().await;
                    Ok(())
                },
                _ => Ok(()),
            }
        }
    }

    let initiator_calls = Arc::new(AtomicUsize::new(0));
    let initiator: Initiator = StepInitiator {
        id: "same_callback_event".to_string(),
        calls: initiator_calls.clone(),
    }
    .into();
    let callback = Arc::new(RetryCallback {
        attempts: AtomicUsize::new(0),
        events: Mutex::new(Vec::new()),
        second_attempt: Notify::new(),
        release: Notify::new(),
    });
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))?
        .add_initiator(initiator)?
        .add_callback(callback.clone())?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.second_attempt.notified()).await?;
    assert_eq!(2, initiator_calls.load(Ordering::SeqCst));
    assert_eq!(vec![(1, 2), (1, 2)], *callback.events.lock());

    callback.release.notify_one();
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(engine);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await?;
    Ok(())
}

struct TestStepInitiator {
    id: String,
    last_height_calls: Arc<AtomicUsize>,
}

impl TestStepInitiator {
    fn new(id: &str, last_height_calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            last_height_calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncInitiator for TestStepInitiator {
    fn id(&self) -> &SyncID {
        &self.id
    }
    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }

    async fn last_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.last_height_calls.fetch_add(1, Ordering::SeqCst);
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
    async fn on_sync_complete(&self, _: &SyncID, _: SyncHeight, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
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
    fn id(&self) -> &SyncID {
        &self.id
    }

    fn initial_synced_height(&self) -> SyncHeight {
        0
    }

    async fn sync_range(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(Some(to))
    }

    fn sleep_on_error(&self) -> Duration {
        Duration::from_millis(20)
    }
}

#[tokio::test]
async fn test_on_sync_complete_callback_failure_does_not_rerun_sync_range() -> anyhow::Result<()> {
    init_logging();
    let status_manager = Arc::new(TestStatusManager::new());
    let initiator_calls = Arc::new(AtomicUsize::new(0));
    let initiator = TestStepInitiator::new("test_init_complete_callback", initiator_calls.clone()).into();
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync = TestCountingSync::new("sync_complete_callback", sync_calls.clone()).into();
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestCompleteCallback {
        calls: callback_calls.clone(),
    });

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initiator])?
        .add_initiator(initiator)?
        .add_callback(callback)?
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(250)).await;

    assert_eq!(2, sync_calls.load(Ordering::SeqCst));
    assert_eq!(3, callback_calls.load(Ordering::SeqCst));
    assert!(initiator_calls.load(Ordering::SeqCst) >= 2);
    let saved = status_manager
        .storage
        .read()
        .get("sync_complete_callback")
        .cloned()
        .unwrap_or_default();
    assert_eq!(vec![1, 2], saved);
    Ok(())
}

#[tokio::test]
async fn test_missing_persisted_height_uses_handler_initial_height() -> anyhow::Result<()> {
    init_logging();
    let initializer = TestInitiator::new("test_init_initial_height", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_id = "sync_initial_height".to_string();

    struct TestInitialHeightSync {
        id: String,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SyncHandler for TestInitialHeightSync {
        fn id(&self) -> &SyncID {
            &self.id
        }

        fn initial_synced_height(&self) -> SyncHeight {
            7
        }

        async fn sync_range(&self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(to))
        }

        fn sleep_on_error(&self) -> Duration {
            Duration::from_millis(20)
        }
    }

    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync = TestInitialHeightSync {
        id: sync_id.clone(),
        calls: sync_calls.clone(),
    }
    .into();

    let engine = SyncEngine::builder(status_manager.clone())?
        .add_sync(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();

    engine.run();
    shutdown_engine_after(engine, Duration::from_millis(250)).await;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(8, statuses[0]);
    assert_gt!(sync_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

async fn shutdown_engine(engine: SyncEngine) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(engine);
    tokio::time::sleep(Duration::from_secs(1)).await;
}

async fn shutdown_engine_after(engine: SyncEngine, run_for: Duration) {
    tokio::time::sleep(run_for).await;
    drop(engine);
    tokio::time::sleep(Duration::from_millis(100)).await;
}

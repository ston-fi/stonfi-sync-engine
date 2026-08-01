use super::*;
use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::traits::{SyncHandler, SyncInitiator, SyncTrigger};
use parking_lot::{Mutex, RwLock};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

static TRACING_INIT: std::sync::Once = std::sync::Once::new();
pub(crate) fn init_test_runtime() -> anyhow::Result<()> {
    TRACING_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::WARN)
            .try_init();
    });
    stonfi_metrics::init_metrics!()?;
    Ok(())
}

// `Cell` keeps the common fixtures `Send` but not `Sync`, so regular workflow
// tests also guard the engine's task-local ownership contract.
struct TestInitiator {
    id: String,
    delay: Duration,
    polls: Cell<usize>,
}
impl TestInitiator {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
            polls: Cell::new(0),
        }
    }
}

#[async_trait::async_trait]
impl SyncInitiator for TestInitiator {
    fn id(&self) -> &str {
        &self.id
    }
    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        self.polls.set(self.polls.get() + 1);
        tokio::time::sleep(self.delay).await;
        Ok(after + 1)
    }
}

struct TestSync {
    id: String,
    delay: Duration,
    ranges: Cell<usize>,
}
impl TestSync {
    fn new(id: &str, delay_ms: u64) -> Self {
        Self {
            id: id.to_string(),
            delay: Duration::from_millis(delay_ms),
            ranges: Cell::new(0),
        }
    }
}

#[async_trait::async_trait]
impl SyncHandler for TestSync {
    fn id(&self) -> &str {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
    }
    async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
        self.ranges.set(self.ranges.get() + 1);
        tokio::time::sleep(self.delay).await;
        Ok(Some(to))
    }
}

struct TestStatusManager {
    storage: RwLock<HashMap<String, Vec<SyncHeight>>>,
}

impl TestStatusManager {
    pub fn new() -> Self {
        Self {
            storage: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl SyncStatusStore for TestStatusManager {
    async fn save_synced_height(&self, sync_id: &str, sync_height: SyncHeight) -> SyncCoreResult<()> {
        self.storage.write().entry(sync_id.to_owned()).or_default().push(sync_height);
        Ok(())
    }

    async fn load_synced_height(&self, sync_id: &str) -> SyncCoreResult<Option<SyncHeight>> {
        Ok(self.storage.read().get(sync_id).and_then(|heights| heights.last().copied()))
    }
}

struct TestTrigger(SyncReceiver);

impl SyncTrigger for TestTrigger {
    fn receiver(&self) -> SyncReceiver {
        self.0.clone()
    }
}

#[tokio::test]
async fn test_initial_initiator_height_is_published() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct FixedInitiator {
        id: String,
    }

    #[async_trait::async_trait]
    impl SyncInitiator for FixedInitiator {
        fn id(&self) -> &str {
            &self.id
        }

        fn retry_delay(&self) -> Duration {
            Duration::from_millis(10)
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
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
    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initiator])?
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

    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;
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
    fn id(&self) -> &str {
        &self.id
    }
    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let initializer: Initiator = TestInitiator::new("test_init1_ranged", 5).into();
    let status_manager = Arc::new(TestStatusManager::new());

    let sync_id = "sync_ranged".to_string();
    let sync = TestSyncRanged::new(&sync_id, 20).into();

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();

    shutdown_engine_after(engine.run(), Duration::from_millis(300)).await?;

    let sync_statuses = status_manager
        .storage
        .read()
        .get(&sync_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing statuses for {sync_id}"))?;
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
    let initializer: Initiator = TestInitiator::new("test_init1_callback", 5).into();
    let status_manager = Arc::new(TestStatusManager::new());

    let sync_id = "sync_callback".to_string();
    let sync = TestSyncRanged::new(&sync_id, 20).into();

    const NEXT_HEIGHT: usize = 1;
    const SENT: usize = 1 << 1;
    const SYNC_START: usize = 1 << 2;
    const SYNC_COMPLETE: usize = 1 << 3;

    struct TestCallback(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl SyncCallback for TestCallback {
        async fn on_initiator_next_height(&self, _: &str, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.0.fetch_or(NEXT_HEIGHT, Ordering::SeqCst);
            Ok(())
        }

        async fn on_initiator_sent(&self, _: &str, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.0.fetch_or(SENT, Ordering::SeqCst);
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

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
        .add_callback(callback)
        .build();

    shutdown_engine_after(engine.run(), Duration::from_millis(300)).await?;

    assert_eq!(NEXT_HEIGHT | SENT | SYNC_START | SYNC_COMPLETE, events.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn test_builder_rejects_duplicate_sync_ids() -> anyhow::Result<()> {
    init_test_runtime()?;

    let initializer: Initiator = TestInitiator::new("test_init_dup_sync", 5).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_1 = TestSync::new("sync_duplicate", 5).into();
    let sync_2 = TestSync::new("sync_duplicate", 5).into();

    let builder = SyncEngine::builder(status_manager).add_synchronizer(sync_1, &[&initializer])?;
    let err = match builder.add_synchronizer(sync_2, &[&initializer]) {
        Ok(_) => return Err(anyhow::anyhow!("duplicate sync id should fail")),
        Err(err) => err,
    };
    assert!(matches!(err, SyncCoreError::Logic(_)));

    let init_1 = TestInitiator::new("duplicate_entity", 5).into();
    let init_2 = TestInitiator::new("duplicate_entity", 5).into();
    let builder = SyncEngine::builder(Arc::new(TestStatusManager::new())).add_initiator(init_1)?;
    assert!(matches!(builder.add_initiator(init_2), Err(SyncCoreError::Logic(_))));

    let trigger: Initiator = TestInitiator::new("trigger", 5).into();
    let sync = TestSync::new("shared_entity", 5).into();
    let colliding_initiator = TestInitiator::new("shared_entity", 5).into();
    let builder = SyncEngine::builder(Arc::new(TestStatusManager::new())).add_synchronizer(sync, &[&trigger])?;
    assert!(matches!(
        builder.add_initiator(colliding_initiator),
        Err(SyncCoreError::Logic(_))
    ));
    Ok(())
}

#[tokio::test]
async fn test_builder_rejects_invalid_batch_sizes() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct InvalidRangeSync {
        id: String,
        min: usize,
        max: usize,
    }

    #[async_trait::async_trait]
    impl SyncHandler for InvalidRangeSync {
        fn id(&self) -> &str {
            &self.id
        }

        fn initial_synced_height(&self) -> SyncHeight {
            0
        }

        async fn sync_range(&mut self, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            Ok(None)
        }

        fn min_batch_size(&self) -> usize {
            self.min
        }

        fn max_batch_size(&self) -> usize {
            self.max
        }
    }

    let trigger: Initiator = TestInitiator::new("range_trigger", 5).into();
    let invalid_ranges = [(0, 1), (1, 0), (2, 1)];

    for (index, (min, max)) in invalid_ranges.into_iter().enumerate() {
        let sync: Synchronizer = InvalidRangeSync {
            id: format!("invalid_range_{index}"),
            min,
            max,
        }
        .into();
        let builder = SyncEngine::builder(Arc::new(TestStatusManager::new()));
        assert!(matches!(
            builder.add_synchronizer(sync, &[&trigger]),
            Err(SyncCoreError::Logic(_))
        ));
    }
    Ok(())
}

#[test]
fn test_maximum_remaining_range_does_not_overflow() {
    struct MaximumRangeSync;

    #[async_trait::async_trait]
    impl SyncHandler for MaximumRangeSync {
        fn id(&self) -> &str {
            "maximum_range"
        }

        fn initial_synced_height(&self) -> SyncHeight {
            0
        }

        async fn sync_range(&mut self, _: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            Ok(Some(to))
        }

        fn max_batch_size(&self) -> usize {
            usize::MAX
        }
    }

    let synchronizer = Synchronizer::new(MaximumRangeSync);
    let expected_to = SyncHeight::try_from(usize::MAX).unwrap_or(SyncHeight::MAX);
    assert_eq!(synchronizer.calc_sync_to(1, SyncHeight::MAX), Some(expected_to));
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
    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let initializer: Initiator = TestInitiator::new("test_init_partial", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());

    let sync_a_id = "sync_parent_partial".to_string();
    let sync_b_id = "sync_child_partial".to_string();
    let sync_a: Synchronizer = TestSyncPartial::new(&sync_a_id, 1).into();
    let sync_a_trigger = TestTrigger(sync_a.receiver());
    let sync_b = TestSync::new(&sync_b_id, 1).into();

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync_a, &[&initializer])?
        .add_synchronizer(sync_b, &[&sync_a_trigger])?
        .add_initiator(initializer)?
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    let parent_height = status_manager
        .load_synced_height(&sync_a_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing synced height for {sync_a_id}"))?;
    let child_height = status_manager
        .load_synced_height(&sync_b_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing synced height for {sync_b_id}"))?;
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

    fn initial_synced_height(&self) -> SyncHeight {
        0
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

    let status_manager = Arc::new(TestStatusManager::new());
    let parent_calls = Arc::new(AtomicUsize::new(0));
    let parent_ranges = Arc::new(Mutex::new(vec![]));
    let parent_sync_id = "sync_ignore_parent".to_string();
    let child_sync_id = "sync_ignore_child".to_string();

    let parent_sync: Synchronizer =
        TestIgnoreThenSync::new(&parent_sync_id, parent_calls.clone(), parent_ranges.clone()).into();
    let parent_trigger = TestTrigger(parent_sync.receiver());
    let child_sync = TestSync::new(&child_sync_id, 1).into();

    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(0);
    let trigger = TestTrigger(trigger_rx);

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(parent_sync, &[&trigger])?
        .add_synchronizer(child_sync, &[&parent_trigger])?
        .build();

    let run_handle = engine.run();

    trigger_tx.send(3)?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(1, parent_calls.load(Ordering::SeqCst));
    assert_eq!(None, status_manager.load_synced_height(&parent_sync_id).await?);
    assert_eq!(None, status_manager.load_synced_height(&child_sync_id).await?);

    trigger_tx.send(6)?;
    tokio::time::sleep(Duration::from_millis(150)).await;

    drop(trigger_tx);
    shutdown_engine_after(run_handle, Duration::from_millis(50)).await?;

    // The configured range limits intentionally produce this sequence.
    assert_eq!(3, parent_calls.load(Ordering::SeqCst));
    assert_eq!(vec![(1, 3), (1, 4), (5, 6)], *parent_ranges.lock());
    assert_eq!(Some(6), status_manager.load_synced_height(&parent_sync_id).await?);
    assert!(status_manager.load_synced_height(&child_sync_id).await?.unwrap_or_default() > 0);
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

        fn initial_synced_height(&self) -> SyncHeight {
            0
        }

        fn is_enabled(&self) -> bool {
            self.enabled.load(Ordering::SeqCst)
        }

        async fn sync_range(&mut self, _: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
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
    let trigger = TestTrigger(trigger_rx);
    let status_manager = Arc::new(TestStatusManager::new());
    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&trigger])?
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
    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let initializer: Initiator = TestInitiator::new("test_init_sync_error_callback", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_error_callback".to_string();
    let sync = TestSyncFailFirst::new(&sync_id, attempts).into();
    let sync_error_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestSyncErrorCallback {
        calls: sync_error_calls.clone(),
    });

    let engine = SyncEngine::builder(status_manager)
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
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
    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let initializer: Initiator = TestInitiator::new("test_init_invalid_height", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_invalid_height".to_string();
    let sync = TestSyncInvalidFirst::new(&sync_id, sync_calls.clone()).into();

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
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

    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let initializer: Initiator = TestInitiator::new("test_init_timeout_retry", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let sync: Synchronizer = TestSyncTimesOutFirst { ranges: ranges.clone() }.into();
    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();
    let run_handle = engine.run();

    tokio::time::timeout(Duration::from_millis(500), async {
        while status_manager.storage.read().get("sync_timeout_retry").is_none() {
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
    fn initial_synced_height(&self) -> SyncHeight {
        0
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
        trigger_sender: Arc<Mutex<Option<tokio::sync::watch::Sender<SyncHeight>>>>,
    }

    #[async_trait::async_trait]
    impl SyncCallback for StopAfterFirstSyncCallback {
        async fn on_sync_complete(&self, _: &str, _: SyncHeight, _: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            self.trigger_sender.lock().take();
            Ok(())
        }
    }

    let status_manager = Arc::new(TestStatusManager::new());
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_allow_rewind".to_string();
    let sync = TestRewindSync::new(&sync_id, sync_calls.clone()).into();
    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(1);
    let trigger = TestTrigger(trigger_rx);
    let callback = Arc::new(StopAfterFirstSyncCallback {
        trigger_sender: Arc::new(Mutex::new(Some(trigger_tx))),
    });

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&trigger])?
        .add_callback(callback)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(200)).await?;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
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

    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let initializer: Initiator = TestInitiator::new("test_init_no_rewind", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync_id = "sync_no_rewind".to_string();
    let sync = TestNoRewindSync::new(&sync_id, sync_calls.clone()).into();

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
        .with_log_progress(|_from, to| to % 10 == 0)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(400)).await?;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(1, statuses[0]);
    assert!(sync_calls.load(Ordering::SeqCst) >= 2);
    Ok(())
}

#[tokio::test]
async fn test_initiator_callback_retries_same_event() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct StepInitiator {
        id: String,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SyncInitiator for StepInitiator {
        fn id(&self) -> &str {
            &self.id
        }

        fn retry_delay(&self) -> Duration {
            Duration::from_millis(10)
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
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
            _: &str,
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
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))
        .add_initiator(initiator)?
        .add_callback(callback.clone())
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.second_attempt.notified()).await?;
    assert_eq!(2, initiator_calls.load(Ordering::SeqCst));
    assert_eq!(vec![(1, 2), (1, 2)], *callback.events.lock());

    callback.release.notify_one();
    tokio::time::sleep(Duration::from_millis(20)).await;
    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;
    Ok(())
}

struct TestStepInitiator {
    id: String,
    latest_height_calls: Arc<AtomicUsize>,
}

impl TestStepInitiator {
    fn new(id: &str, latest_height_calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: id.to_string(),
            latest_height_calls,
        }
    }
}

#[async_trait::async_trait]
impl SyncInitiator for TestStepInitiator {
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

    fn initial_synced_height(&self) -> SyncHeight {
        0
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
    let status_manager = Arc::new(TestStatusManager::new());
    let initiator_calls = Arc::new(AtomicUsize::new(0));
    let initiator: Initiator = TestStepInitiator::new("test_init_complete_callback", initiator_calls.clone()).into();
    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync = TestCountingSync::new("sync_complete_callback", sync_calls.clone()).into();
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback = Arc::new(TestCompleteCallback {
        calls: callback_calls.clone(),
    });

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initiator])?
        .add_initiator(initiator)?
        .add_callback(callback)
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(250)).await?;

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
    init_test_runtime()?;
    let initializer: Initiator = TestInitiator::new("test_init_initial_height", 2).into();
    let status_manager = Arc::new(TestStatusManager::new());
    let sync_id = "sync_initial_height".to_string();

    struct TestInitialHeightSync {
        id: String,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SyncHandler for TestInitialHeightSync {
        fn id(&self) -> &str {
            &self.id
        }

        fn initial_synced_height(&self) -> SyncHeight {
            7
        }

        async fn sync_range(&mut self, _from: SyncHeight, to: SyncHeight) -> SyncCoreResult<Option<SyncHeight>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(to))
        }

        fn retry_delay(&self) -> Duration {
            Duration::from_millis(20)
        }
    }

    let sync_calls = Arc::new(AtomicUsize::new(0));
    let sync = TestInitialHeightSync {
        id: sync_id.clone(),
        calls: sync_calls.clone(),
    }
    .into();

    let engine = SyncEngine::builder(status_manager.clone())
        .add_synchronizer(sync, &[&initializer])?
        .add_initiator(initializer)?
        .build();

    let run_handle = engine.run();
    shutdown_engine_after(run_handle, Duration::from_millis(250)).await?;

    let statuses = status_manager.storage.read().get(&sync_id).cloned().unwrap_or_default();
    assert!(!statuses.is_empty());
    assert_eq!(8, statuses[0]);
    assert!(sync_calls.load(Ordering::SeqCst) > 0);
    Ok(())
}

#[tokio::test]
async fn test_shutdown_completes_while_custom_trigger_sender_is_alive() -> anyhow::Result<()> {
    init_test_runtime()?;

    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(0);
    let trigger = TestTrigger(trigger_rx);
    let sync: Synchronizer = TestSync::new("live_custom_trigger", 0).into();
    let mut sync_progress = sync.receiver();
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))
        .add_synchronizer(sync, &[&trigger])?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), sync_progress.changed()).await??;
    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;

    drop(trigger_tx);
    Ok(())
}

#[tokio::test]
async fn test_wait_returns_when_custom_trigger_closes() -> anyhow::Result<()> {
    init_test_runtime()?;

    let (trigger_tx, trigger_rx) = tokio::sync::watch::channel(0);
    let trigger = TestTrigger(trigger_rx);
    let sync: Synchronizer = TestSync::new("closing_custom_trigger", 0).into();
    let mut sync_progress = sync.receiver();
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))
        .add_synchronizer(sync, &[&trigger])?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), sync_progress.changed()).await??;
    drop(trigger_tx);
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await??;
    Ok(())
}

#[tokio::test]
async fn test_wait_returns_task_join_failure() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct PendingInitiator;

    #[async_trait::async_trait]
    impl SyncInitiator for PendingInitiator {
        fn id(&self) -> &str {
            "pending_initiator"
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            std::future::pending().await
        }
    }

    struct PanickingInitiator;

    #[async_trait::async_trait]
    impl SyncInitiator for PanickingInitiator {
        fn id(&self) -> &str {
            "panicking_initiator"
        }

        async fn latest_height(&mut self, _: SyncHeight) -> SyncCoreResult<SyncHeight> {
            panic!("intentional task panic")
        }
    }

    let pending: Initiator = PendingInitiator.into();
    let panicking: Initiator = PanickingInitiator.into();
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))
        .add_initiator(pending)?
        .add_initiator(panicking)?
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

    struct PendingInitiator {
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
    impl SyncInitiator for PendingInitiator {
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
    let engine = SyncEngine::builder(Arc::new(TestStatusManager::new()))
        .with_shutdown_timeout(Duration::from_millis(20))?
        .add_initiator(
            PendingInitiator {
                started: started.clone(),
                dropped: dropped.clone(),
            }
            .into(),
        )?
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

async fn shutdown_engine_after(run_handle: RunHandle, run_for: Duration) -> anyhow::Result<()> {
    tokio::time::sleep(run_for).await;
    run_handle.shutdown().await?;
    Ok(())
}

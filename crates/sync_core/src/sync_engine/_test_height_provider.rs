use super::_test_support::*;
use super::*;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[tokio::test]
async fn test_initial_loaded_height_is_published() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct FixedHeightLoader {
        id: String,
    }

    #[async_trait::async_trait]
    impl HeightLoader for FixedHeightLoader {
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

    let status_store = Arc::new(TestStatusStore::new(0));
    let height_provider: HeightProvider = FixedHeightLoader {
        id: "fixed_initial_height".to_string(),
    }
    .into();
    let id = "sync_initial_publish".to_string();
    let sync = TestSync::new(&id, 0).into();
    let engine = SyncEngine::builder(status_store.clone())
        .add_synchronizer(sync, &[&height_provider])?
        .add_height_provider(height_provider)?
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), async {
        while status_store.load_synced_height(&id).await? != Some(7) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<(), SyncCoreError>(())
    })
    .await??;

    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum InitialEvent {
    Loaded(String, SyncHeight, SyncHeight),
    Published(String, SyncHeight, SyncHeight),
}

struct InitialHeightLoader {
    height: SyncHeight,
    calls: usize,
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl HeightLoader for InitialHeightLoader {
    fn id(&self) -> &str {
        "initial_callback_height"
    }

    async fn latest_height(&mut self, after: SyncHeight) -> SyncCoreResult<SyncHeight> {
        if self.calls == 0 {
            self.calls += 1;
            return Ok(self.height);
        }
        self.release.notified().await;
        Ok(after)
    }
}

struct InitialCallback {
    events: Arc<Mutex<Vec<InitialEvent>>>,
    done: Notify,
}

#[async_trait::async_trait]
impl SyncCallback for InitialCallback {
    async fn on_height_loaded(
        &self,
        handler_id: &str,
        previous_height: SyncHeight,
        loaded_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        self.events
            .lock()
            .push(InitialEvent::Loaded(handler_id.to_owned(), previous_height, loaded_height));
        if loaded_height == 0 {
            self.done.notify_one();
        }
        Ok(())
    }

    async fn on_height_published(
        &self,
        handler_id: &str,
        previous_height: SyncHeight,
        published_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        self.events.lock().push(InitialEvent::Published(
            handler_id.to_owned(),
            previous_height,
            published_height,
        ));
        self.done.notify_one();
        Ok(())
    }
}

async fn initial_events(height: SyncHeight) -> anyhow::Result<Vec<InitialEvent>> {
    let release = Arc::new(Notify::new());
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback = Arc::new(InitialCallback {
        events: events.clone(),
        done: Notify::new(),
    });
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_height_provider(
            InitialHeightLoader {
                height,
                calls: 0,
                release: release.clone(),
            }
            .into(),
        )?
        .add_callback(callback.clone())
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.done.notified()).await?;
    run_handle.cancellation.cancel();
    release.notify_one();
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await??;

    let events = events.lock().clone();
    Ok(events)
}

#[tokio::test]
async fn test_initial_height_callbacks() -> anyhow::Result<()> {
    init_test_runtime()?;
    assert_eq!(
        vec![
            InitialEvent::Loaded("initial_callback_height".to_owned(), 0, 7),
            InitialEvent::Published("initial_callback_height".to_owned(), 0, 7),
        ],
        initial_events(7).await?
    );
    assert_eq!(
        vec![InitialEvent::Loaded("initial_callback_height".to_owned(), 0, 0)],
        initial_events(0).await?
    );
    Ok(())
}

struct RetryPublishedCallback {
    attempts: AtomicUsize,
    first_attempt: Notify,
    fail_first: Notify,
    second_attempt: Notify,
    finish: Notify,
}

#[async_trait::async_trait]
impl SyncCallback for RetryPublishedCallback {
    async fn on_height_published(
        &self,
        handler_id: &str,
        previous_height: SyncHeight,
        published_height: SyncHeight,
    ) -> SyncCoreResult<()> {
        assert_eq!("initial_callback_height", handler_id);
        assert_eq!((0, 7), (previous_height, published_height));
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_attempt.notify_one();
            self.fail_first.notified().await;
            Err(SyncCoreError::custom("retry initial publication callback"))
        } else {
            self.second_attempt.notify_one();
            self.finish.notified().await;
            Ok(())
        }
    }
}

#[tokio::test]
async fn test_initial_published_callback_retry_does_not_republish() -> anyhow::Result<()> {
    init_test_runtime()?;
    let loader_release = Arc::new(Notify::new());
    let height_provider: HeightProvider = InitialHeightLoader {
        height: 7,
        calls: 0,
        release: loader_release.clone(),
    }
    .into();
    let mut progress = height_provider.subscribe();
    let callback = Arc::new(RetryPublishedCallback {
        attempts: AtomicUsize::new(0),
        first_attempt: Notify::new(),
        fail_first: Notify::new(),
        second_attempt: Notify::new(),
        finish: Notify::new(),
    });
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_height_provider(height_provider)?
        .add_callback(callback.clone())
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.first_attempt.notified()).await?;
    assert_eq!(7, *progress.borrow_and_update());
    callback.fail_first.notify_one();
    tokio::time::timeout(Duration::from_millis(500), callback.second_attempt.notified()).await?;
    assert!(!progress.has_changed()?);

    run_handle.cancellation.cancel();
    callback.finish.notify_one();
    loader_release.notify_one();
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await??;
    assert_eq!(2, callback.attempts.load(Ordering::SeqCst));
    Ok(())
}

#[tokio::test]
async fn test_height_loaded_callback_retries_same_event() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct StepHeightLoader {
        id: String,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl HeightLoader for StepHeightLoader {
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
        async fn on_height_loaded(
            &self,
            _: &str,
            previous_height: SyncHeight,
            next_height: SyncHeight,
        ) -> SyncCoreResult<()> {
            if previous_height == 0 {
                return Ok(());
            }
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

    let height_loader_calls = Arc::new(AtomicUsize::new(0));
    let height_provider: HeightProvider = StepHeightLoader {
        id: "same_callback_event".to_string(),
        calls: height_loader_calls.clone(),
    }
    .into();
    let callback = Arc::new(RetryCallback {
        attempts: AtomicUsize::new(0),
        events: Mutex::new(Vec::new()),
        second_attempt: Notify::new(),
        release: Notify::new(),
    });
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_height_provider(height_provider)?
        .add_callback(callback.clone())
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.second_attempt.notified()).await?;
    assert_eq!(2, height_loader_calls.load(Ordering::SeqCst));
    assert_eq!(vec![(1, 2), (1, 2)], *callback.events.lock());

    callback.release.notify_one();
    tokio::time::sleep(Duration::from_millis(20)).await;
    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn test_shutdown_interrupts_callback_retry_loop() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct FailingCallback {
        called: Notify,
    }

    #[async_trait::async_trait]
    impl SyncCallback for FailingCallback {
        async fn on_height_loaded(&self, _: &str, previous_height: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            if previous_height == 0 {
                return Ok(());
            }
            self.called.notify_one();
            Err(SyncCoreError::custom("keep retrying"))
        }
    }

    let height_provider: HeightProvider = TestHeightLoader::new("callback_shutdown", 1).into();
    let receiver = height_provider.subscribe();
    let callback = Arc::new(FailingCallback { called: Notify::new() });
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_height_provider(height_provider)?
        .add_callback(callback.clone())
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.called.notified()).await?;
    tokio::time::timeout(Duration::from_millis(500), run_handle.shutdown()).await??;
    assert_eq!(1, *receiver.borrow());
    Ok(())
}

#[tokio::test]
async fn test_shutdown_during_successful_callback_prevents_publication() -> anyhow::Result<()> {
    init_test_runtime()?;

    struct BlockingCallback {
        started: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl SyncCallback for BlockingCallback {
        async fn on_height_loaded(&self, _: &str, previous_height: SyncHeight, _: SyncHeight) -> SyncCoreResult<()> {
            if previous_height == 0 {
                return Ok(());
            }
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    let height_provider: HeightProvider = TestHeightLoader::new("successful_callback_shutdown", 1).into();
    let receiver = height_provider.subscribe();
    let callback = Arc::new(BlockingCallback {
        started: Notify::new(),
        release: Notify::new(),
    });
    let engine = SyncEngine::builder(Arc::new(TestStatusStore::new(0)))
        .add_height_provider(height_provider)?
        .add_callback(callback.clone())
        .build();

    let run_handle = engine.run();
    tokio::time::timeout(Duration::from_millis(500), callback.started.notified()).await?;
    run_handle.cancellation.cancel();
    callback.release.notify_one();
    tokio::time::timeout(Duration::from_millis(500), run_handle.wait()).await??;
    assert_eq!(1, *receiver.borrow());
    Ok(())
}

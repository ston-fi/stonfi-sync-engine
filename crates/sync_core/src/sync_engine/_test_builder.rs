use super::_test_support::*;
use super::*;

#[tokio::test]
async fn test_builder_rejects_duplicate_component_ids() -> anyhow::Result<()> {
    init_test_runtime()?;

    let height_provider: HeightProvider = TestHeightLoader::new("test_init_dup_sync", 5).into();
    let status_store = Arc::new(TestStatusStore::new(0));
    let sync_1 = TestSync::new("sync_duplicate", 5).into();
    let sync_2 = TestSync::new("sync_duplicate", 5).into();

    let builder = SyncEngine::builder(status_store).add_synchronizer(sync_1, &[&height_provider])?;
    let err = match builder.add_synchronizer(sync_2, &[&height_provider]) {
        Ok(_) => return Err(anyhow::anyhow!("duplicate component ID should fail")),
        Err(err) => err,
    };
    assert!(matches!(err, SyncCoreError::Logic(_)));

    let provider_1 = TestHeightLoader::new("duplicate_entity", 5).into();
    let provider_2 = TestHeightLoader::new("duplicate_entity", 5).into();
    let builder = SyncEngine::builder(Arc::new(TestStatusStore::new(0))).add_height_provider(provider_1)?;
    assert!(matches!(builder.add_height_provider(provider_2), Err(SyncCoreError::Logic(_))));

    let progress_provider: HeightProvider = TestHeightLoader::new("progress_provider", 5).into();
    let sync = TestSync::new("shared_entity", 5).into();
    let colliding_height_provider = TestHeightLoader::new("shared_entity", 5).into();
    let builder =
        SyncEngine::builder(Arc::new(TestStatusStore::new(0))).add_synchronizer(sync, &[&progress_provider])?;
    assert!(matches!(
        builder.add_height_provider(colliding_height_provider),
        Err(SyncCoreError::Logic(_))
    ));
    Ok(())
}

#[tokio::test]
async fn test_builder_rejects_reserved_component_id() -> anyhow::Result<()> {
    init_test_runtime()?;

    let status_store = Arc::new(TestStatusStore::new(0));
    let height_provider: HeightProvider = TestHeightLoader::new(INITIAL_SYNC_ID, 5).into();
    assert!(matches!(
        SyncEngine::builder(status_store.clone()).add_height_provider(height_provider),
        Err(SyncCoreError::InvalidArgs(_))
    ));

    let progress_provider: HeightProvider = TestHeightLoader::new("reserved_id_progress", 5).into();
    let synchronizer = TestSync::new(INITIAL_SYNC_ID, 5).into();
    assert!(matches!(
        SyncEngine::builder(status_store).add_synchronizer(synchronizer, &[&progress_provider]),
        Err(SyncCoreError::InvalidArgs(_))
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

    let progress_provider: HeightProvider = TestHeightLoader::new("range_progress", 5).into();
    let invalid_ranges = [(0, 1), (1, 0), (2, 1)];

    for (index, (min, max)) in invalid_ranges.into_iter().enumerate() {
        let sync: Synchronizer = InvalidRangeSync {
            id: format!("invalid_range_{index}"),
            min,
            max,
        }
        .into();
        let builder = SyncEngine::builder(Arc::new(TestStatusStore::new(0)));
        assert!(matches!(
            builder.add_synchronizer(sync, &[&progress_provider]),
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

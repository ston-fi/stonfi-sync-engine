use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::{SyncHeight, SyncID};
use std::time::Duration;
use stonfi_commons_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_commons_metrics::metrics_provider::{BoxableCollector, MetricsProvider};
use stonfi_commons_metrics::prometheus::timer::duration_to_millis;
use stonfi_commons_metrics::prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts};

#[derive(strum::IntoStaticStr, Debug)]
pub(super) enum SyncPhase {
    Initiator,
    SyncRange,
    LoadHeight,
    SaveHeight,
    Callback,
}

pub(super) struct SyncEngineMetrics {
    sync_engine_last_initiator_height: IntGaugeVec,
    sync_engine_last_synced_height: IntGaugeVec,
    sync_engine_heights_processed: IntCounterVec,
    sync_engine_height_process_duration_ms: HistogramVec,
    sync_engine_retries: IntCounterVec,
}

impl SyncEngineMetrics {
    pub(super) fn new() -> SyncCoreResult<Self> {
        let common_labels = vec!["sync_id"];
        let phase_labels = vec!["sync_id", "phase"];

        let res = Self {
            sync_engine_last_initiator_height: IntGaugeVec::new(
                Opts::new(
                    "sync_engine_last_initiator_height",
                    "Max height returned by SyncInitiator::last_height()",
                ),
                &common_labels,
            )
            .map_err(SyncCoreError::system)?,
            sync_engine_last_synced_height: IntGaugeVec::new(
                Opts::new("sync_engine_last_synced_height", "Max synced height"),
                &common_labels,
            )
            .map_err(SyncCoreError::system)?,
            sync_engine_heights_processed: IntCounterVec::new(
                Opts::new("sync_engine_heights_processed", "How many heights were processed"),
                &common_labels,
            )
            .map_err(SyncCoreError::system)?,
            sync_engine_height_process_duration_ms: HistogramVec::new(
                HistogramOpts::new(
                    "sync_engine_height_process_duration_ms",
                    "process_duration / processed_heights_count",
                )
                .buckets(DURATION_BUCKETS_1MS_20S.clone()),
                &common_labels,
            )
            .map_err(SyncCoreError::system)?,
            sync_engine_retries: IntCounterVec::new(
                Opts::new("sync_engine_retries", "How many retries were made during processing"),
                &phase_labels,
            )
            .map_err(SyncCoreError::system)?,
        };
        Ok(res)
    }

    pub(super) fn update_initiator(&self, sync_id: &SyncID, last_height: SyncHeight) {
        self.sync_engine_last_initiator_height
            .with_label_values(&[sync_id])
            .set(last_height as i64);
    }

    pub(super) fn update_synced_height(&self, sync_id: &SyncID, height: SyncHeight) {
        self.sync_engine_last_synced_height
            .with_label_values(&[sync_id])
            .set(height as i64);
    }

    pub(super) fn update_sync(&self, sync_id: &SyncID, from: SyncHeight, to: SyncHeight, duration: Duration) {
        if to < from {
            log::warn!("[METRICS][{sync_id}] invalid sync range for metrics: from={from}, to={to}");
            return;
        }
        let heights_processed = to - from + 1;
        self.update_synced_height(sync_id, to);

        self.sync_engine_heights_processed
            .with_label_values(&[sync_id])
            .inc_by(heights_processed as u64);

        let duration_millis = duration_to_millis(duration);
        let duration_for_height = duration_millis as f64 / heights_processed as f64;
        self.sync_engine_height_process_duration_ms
            .with_label_values(&[sync_id])
            .observe(duration_for_height);
    }

    pub(super) fn inc_retries(&self, sync_id: &SyncID, phase: SyncPhase) {
        self.sync_engine_retries
            .with_label_values(&[sync_id.as_str(), phase.into()])
            .inc();
    }
}

impl MetricsProvider for SyncEngineMetrics {
    fn provide_metrics(&self) -> Vec<&dyn BoxableCollector> {
        vec![
            &self.sync_engine_last_initiator_height,
            &self.sync_engine_last_synced_height,
            &self.sync_engine_heights_processed,
            &self.sync_engine_height_process_duration_ms,
            &self.sync_engine_retries,
        ]
    }
}

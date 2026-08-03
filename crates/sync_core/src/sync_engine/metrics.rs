use crate::sync_engine::SyncHeight;
use std::time::Duration;
use stonfi_metrics::MetricsCell;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::prometheus::core::{AtomicU64, GenericGaugeVec, Opts};
use stonfi_metrics::prometheus::{self, HistogramVec, IntCounterVec};
use stonfi_metrics::utils::format_duration_ms;

static METRICS: MetricsCell<SyncEngineMetrics> = MetricsCell::new();
type UIntGaugeVec = GenericGaugeVec<AtomicU64>;

stonfi_metrics::register_metrics!(SyncEngineMetrics, METRICS);

#[derive(strum::IntoStaticStr, Debug)]
pub(super) enum SyncPhase {
    HeightLoad,
    SyncRange,
    LoadHeight,
    SaveHeight,
    Callback,
}

pub(super) struct SyncEngineMetrics {
    sync_engine_last_loaded_height: UIntGaugeVec,
    sync_engine_last_synced_height: UIntGaugeVec,
    sync_engine_heights_processed: IntCounterVec,
    sync_engine_height_process_duration_ms: HistogramVec,
    sync_engine_retries: IntCounterVec,
}

impl SyncEngineMetrics {
    fn new() -> anyhow::Result<Self> {
        let common_labels = &["handler_id"];
        let phase_labels = &["handler_id", "phase"];

        Ok(Self {
            sync_engine_last_loaded_height: register_uint_gauge_vec(
                "sync_engine_last_loaded_height",
                "Latest height returned by HeightLoader::latest_height()",
                common_labels,
            )?,
            sync_engine_last_synced_height: register_uint_gauge_vec(
                "sync_engine_last_synced_height",
                "Max synced height",
                common_labels,
            )?,
            sync_engine_heights_processed: prometheus::register_int_counter_vec!(
                "sync_engine_heights_processed",
                "How many heights were processed",
                common_labels,
            )?,
            sync_engine_height_process_duration_ms: prometheus::register_histogram_vec!(
                "sync_engine_height_process_duration_ms",
                "process_duration / processed_heights_count",
                common_labels,
                DURATION_BUCKETS_1MS_20S.clone(),
            )?,
            sync_engine_retries: prometheus::register_int_counter_vec!(
                "sync_engine_retries",
                "How many retries were made during processing",
                phase_labels,
            )?,
        })
    }

    pub(super) fn update_loaded_height(id: &str, latest_height: SyncHeight) {
        METRICS
            .sync_engine_last_loaded_height
            .with_label_values(&[id])
            .set(latest_height);
    }

    pub(super) fn update_synced_height(id: &str, height: SyncHeight) {
        METRICS.sync_engine_last_synced_height.with_label_values(&[id]).set(height);
    }

    pub(super) fn update_sync(id: &str, from: SyncHeight, to: SyncHeight, duration: Duration) {
        if to < from {
            tracing::warn!("[METRICS][{id}] invalid sync range for metrics: from={from}, to={to}");
            return;
        }
        let heights_processed = to - from + 1;
        Self::update_synced_height(id, to);

        METRICS
            .sync_engine_heights_processed
            .with_label_values(&[id])
            .inc_by(heights_processed);

        let duration_millis = format_duration_ms(duration);
        let duration_for_height = duration_millis / heights_processed as f64;
        METRICS
            .sync_engine_height_process_duration_ms
            .with_label_values(&[id])
            .observe(duration_for_height);
    }

    pub(super) fn inc_retries(id: &str, phase: SyncPhase) {
        METRICS.sync_engine_retries.with_label_values(&[id, phase.into()]).inc();
    }
}

fn register_uint_gauge_vec(name: &str, help: &str, labels: &[&str]) -> anyhow::Result<UIntGaugeVec> {
    let gauge = UIntGaugeVec::new(Opts::new(name, help), labels)?;
    prometheus::register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

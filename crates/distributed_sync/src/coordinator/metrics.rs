use std::time::Duration;
use stonfi_metrics::MetricsCell;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::prometheus::{self, HistogramVec, IntCounterVec, IntGaugeVec};
use stonfi_metrics::utils::format_duration_ms;

static METRICS: MetricsCell<CoordinatorMetrics> = MetricsCell::new();

stonfi_metrics::register_metrics!(CoordinatorMetrics, METRICS);

#[derive(strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum CoordinatorTaskStatus {
    Queued,
    Processed,
    Failed,
    TimedOut,
}

pub(super) struct CoordinatorMetrics {
    tasks: IntCounterVec,
    task_duration_ms: HistogramVec,
    queue_size: IntGaugeVec,
}

impl CoordinatorMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            tasks: prometheus::register_int_counter_vec!(
                "stonfi_distributed_sync_coordinator_tasks_total",
                "Distributed coordinator task outcomes",
                &["handler_id", "status"],
            )?,
            task_duration_ms: prometheus::register_histogram_vec!(
                "stonfi_distributed_sync_coordinator_task_duration_ms",
                "Distributed coordinator task duration in milliseconds",
                &["handler_id", "status"],
                DURATION_BUCKETS_1MS_20S.clone(),
            )?,
            queue_size: prometheus::register_int_gauge_vec!(
                "stonfi_distributed_sync_coordinator_queue_size",
                "Queued distributed tasks",
                &["kind"],
            )?,
        })
    }

    pub(super) fn queued(id: &str) {
        METRICS
            .tasks
            .with_label_values(&[id, CoordinatorTaskStatus::Queued.into()])
            .inc();
    }

    pub(super) fn complete(id: &str, status: CoordinatorTaskStatus, duration: Duration) {
        let status: &'static str = status.into();
        METRICS.tasks.with_label_values(&[id, status]).inc();
        METRICS
            .task_duration_ms
            .with_label_values(&[id, status])
            .observe(format_duration_ms(duration));
    }

    pub(super) fn set_queue_sizes(regular: usize, service: usize) {
        METRICS
            .queue_size
            .with_label_values(&["regular"])
            .set(i64::try_from(regular).unwrap_or(i64::MAX));
        METRICS
            .queue_size
            .with_label_values(&["service"])
            .set(i64::try_from(service).unwrap_or(i64::MAX));
    }
}

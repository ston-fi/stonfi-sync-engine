use std::time::Duration;
use stonfi_metrics::MetricsCell;
use stonfi_metrics::constants::DURATION_BUCKETS_1MS_20S;
use stonfi_metrics::prometheus::{self, HistogramVec, IntCounterVec};
use stonfi_metrics::utils::format_duration_ms;

static METRICS: MetricsCell<WorkerMetrics> = MetricsCell::new();

stonfi_metrics::register_metrics!(WorkerMetrics, METRICS);

#[derive(strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum PollOutcome {
    Task,
    Empty,
    Error,
}

#[derive(strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(super) enum WorkerTaskStatus {
    Received,
    Processed,
    Failed,
    TimedOut,
    CompletionFailed,
}

pub(super) struct WorkerMetrics {
    polls: IntCounterVec,
    tasks: IntCounterVec,
    task_duration_ms: HistogramVec,
}

impl WorkerMetrics {
    fn new() -> anyhow::Result<Self> {
        Ok(Self {
            polls: prometheus::register_int_counter_vec!(
                "stonfi_distributed_sync_worker_polls_total",
                "Distributed worker poll outcomes",
                &["outcome"],
            )?,
            tasks: prometheus::register_int_counter_vec!(
                "stonfi_distributed_sync_worker_tasks_total",
                "Distributed worker task outcomes",
                &["handler_id", "status"],
            )?,
            task_duration_ms: prometheus::register_histogram_vec!(
                "stonfi_distributed_sync_worker_task_duration_ms",
                "Distributed worker task duration in milliseconds",
                &["handler_id", "status"],
                DURATION_BUCKETS_1MS_20S.clone(),
            )?,
        })
    }

    pub(super) fn poll(outcome: PollOutcome) {
        let outcome: &'static str = outcome.into();
        METRICS.polls.with_label_values(&[outcome]).inc();
    }

    pub(super) fn task(id: &str, status: WorkerTaskStatus, duration: Duration) {
        let status: &'static str = status.into();
        METRICS.tasks.with_label_values(&[id, status]).inc();
        if !duration.is_zero() {
            METRICS
                .task_duration_ms
                .with_label_values(&[id, status])
                .observe(format_duration_ms(duration));
        }
    }
}

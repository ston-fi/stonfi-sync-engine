use parking_lot::RwLock;
use std::collections::HashSet;
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

#[derive(Clone, Copy, Default)]
pub(super) struct WorkerTaskStats {
    pub(super) received: u64,
    pub(super) processed: u64,
    pub(super) failed: u64,
    pub(super) timed_out: u64,
    pub(super) completion_failed: u64,
}

impl WorkerTaskStats {
    pub(super) fn saturating_delta(self, previous: Self) -> Self {
        Self {
            received: self.received.saturating_sub(previous.received),
            processed: self.processed.saturating_sub(previous.processed),
            failed: self.failed.saturating_sub(previous.failed),
            timed_out: self.timed_out.saturating_sub(previous.timed_out),
            completion_failed: self.completion_failed.saturating_sub(previous.completion_failed),
        }
    }
}

pub(super) struct WorkerMetrics {
    polls: IntCounterVec,
    tasks: IntCounterVec,
    task_duration_ms: HistogramVec,
    task_ids: RwLock<HashSet<String>>,
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
            task_ids: RwLock::new(HashSet::new()),
        })
    }

    pub(super) fn poll(outcome: PollOutcome) {
        let outcome: &'static str = outcome.into();
        METRICS.polls.with_label_values(&[outcome]).inc();
    }

    pub(super) fn task(id: &str, status: WorkerTaskStatus, duration: Duration) {
        let status: &'static str = status.into();
        Self::record_task_id(id);
        METRICS.tasks.with_label_values(&[id, status]).inc();
        if !duration.is_zero() {
            METRICS
                .task_duration_ms
                .with_label_values(&[id, status])
                .observe(format_duration_ms(duration));
        }
    }

    pub(super) fn task_stats(id: &str) -> WorkerTaskStats {
        WorkerTaskStats {
            received: Self::task_count(id, WorkerTaskStatus::Received),
            processed: Self::task_count(id, WorkerTaskStatus::Processed),
            failed: Self::task_count(id, WorkerTaskStatus::Failed),
            timed_out: Self::task_count(id, WorkerTaskStatus::TimedOut),
            completion_failed: Self::task_count(id, WorkerTaskStatus::CompletionFailed),
        }
    }

    pub(super) fn task_ids() -> Vec<String> {
        METRICS.task_ids.read().iter().cloned().collect()
    }

    fn task_count(id: &str, status: WorkerTaskStatus) -> u64 {
        let status: &'static str = status.into();
        METRICS.tasks.with_label_values(&[id, status]).get()
    }

    fn record_task_id(id: &str) {
        if METRICS.task_ids.read().contains(id) {
            return;
        }
        METRICS.task_ids.write().insert(id.to_owned());
    }
}

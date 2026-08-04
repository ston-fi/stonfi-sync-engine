use super::metrics::{WorkerMetrics, WorkerTaskStats};
use std::collections::HashMap;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const COUNTER_WIDTH: usize = 7;
pub(super) async fn progress_log_loop(cancellation: CancellationToken, logging_period: Duration) {
    let mut previous_stats = task_stats_snapshot().into_iter().collect();
    let mut interval = tokio::time::interval(logging_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            _ = interval.tick() => {},
        }

        let period_stats = period_deltas(&mut previous_stats, task_stats_snapshot());

        tracing::info!("{}", format_task_stats(&period_stats));
    }
}

fn task_stats_snapshot() -> Vec<(String, WorkerTaskStats)> {
    let mut ids = WorkerMetrics::task_ids();
    ids.sort_unstable();
    ids.into_iter()
        .map(|id| {
            let stats = WorkerMetrics::task_stats(&id);
            (id, stats)
        })
        .collect()
}

fn period_deltas(
    previous_stats: &mut HashMap<String, WorkerTaskStats>,
    current_stats: Vec<(String, WorkerTaskStats)>,
) -> Vec<(String, WorkerTaskStats)> {
    current_stats
        .into_iter()
        .map(|(id, current)| {
            let previous = previous_stats.insert(id.clone(), current).unwrap_or_default();
            (id, current.saturating_delta(previous))
        })
        .collect()
}

fn format_task_stats(stats: &[(String, WorkerTaskStats)]) -> String {
    let sync_width = stats.iter().map(|(id, _)| id.len()).max().unwrap_or_default().max("sync".len());
    let mut output = format!(
        "processed task stat:\n  {sync:<sync_width$} | {received:>COUNTER_WIDTH$} | {processed:>COUNTER_WIDTH$} | {failed:>COUNTER_WIDTH$} | {timed_out:>COUNTER_WIDTH$} | {completion_failed:>COUNTER_WIDTH$}\n",
        sync = "sync",
        received = "rcv",
        processed = "ok",
        failed = "err",
        timed_out = "timeout",
        completion_failed = "fail",
    );
    for (id, stats) in stats {
        output.push_str(&format!(
            "  {id:<sync_width$} | {received:>COUNTER_WIDTH$} | {processed:>COUNTER_WIDTH$} | {failed:>COUNTER_WIDTH$} | {timed_out:>COUNTER_WIDTH$} | {completion_failed:>COUNTER_WIDTH$}\n",
            received = stats.received,
            processed = stats.processed,
            failed = stats.failed,
            timed_out = stats.timed_out,
            completion_failed = stats.completion_failed,
        ));
    }
    output
}

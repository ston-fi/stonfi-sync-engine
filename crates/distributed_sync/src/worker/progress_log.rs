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
    let mut handler_ids = WorkerMetrics::task_ids();
    handler_ids.sort_unstable();
    handler_ids
        .into_iter()
        .map(|handler_id| {
            let stats = WorkerMetrics::task_stats(&handler_id);
            (handler_id, stats)
        })
        .collect()
}

fn period_deltas(
    previous_stats: &mut HashMap<String, WorkerTaskStats>,
    current_stats: Vec<(String, WorkerTaskStats)>,
) -> Vec<(String, WorkerTaskStats)> {
    current_stats
        .into_iter()
        .map(|(handler_id, current)| {
            let previous = previous_stats.insert(handler_id.clone(), current).unwrap_or_default();
            (handler_id, current.saturating_delta(previous))
        })
        .collect()
}

fn format_task_stats(stats: &[(String, WorkerTaskStats)]) -> String {
    let handler_id_width = stats
        .iter()
        .map(|(handler_id, _)| handler_id.len())
        .max()
        .unwrap_or_default()
        .max("handler_id".len());
    let mut output = format!(
        "processed task stat:\n  {handler_id:<handler_id_width$} | {received:>COUNTER_WIDTH$} | {processed:>COUNTER_WIDTH$} | {failed:>COUNTER_WIDTH$} | {timed_out:>COUNTER_WIDTH$} | {completion_failed:>COUNTER_WIDTH$}\n",
        handler_id = "handler_id",
        received = "rcv",
        processed = "ok",
        failed = "err",
        timed_out = "timeout",
        completion_failed = "fail",
    );
    for (handler_id, stats) in stats {
        output.push_str(&format!(
            "  {handler_id:<handler_id_width$} | {received:>COUNTER_WIDTH$} | {processed:>COUNTER_WIDTH$} | {failed:>COUNTER_WIDTH$} | {timed_out:>COUNTER_WIDTH$} | {completion_failed:>COUNTER_WIDTH$}\n",
            received = stats.received,
            processed = stats.processed,
            failed = stats.failed,
            timed_out = stats.timed_out,
            completion_failed = stats.completion_failed,
        ));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::format_task_stats;

    #[test]
    fn test_task_stats_header_uses_handler_id() {
        assert!(format_task_stats(&[]).contains("\n  handler_id |"));
    }
}

use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Matches Tokio's internal far-future timer horizon.
const MAX_DEADLINE_OFFSET: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 30);

pub(crate) fn timeout_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn deadline_unix_millis(duration: Duration) -> u64 {
    SystemTime::now()
        .checked_add(duration)
        .and_then(|deadline| deadline.duration_since(UNIX_EPOCH).ok())
        .and_then(|deadline| u64::try_from(deadline.as_millis()).ok())
        .unwrap_or(u64::MAX)
}

pub(crate) fn deadline_from_unix_millis(deadline_unix_ms: u64) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    let remaining = match UNIX_EPOCH.checked_add(Duration::from_millis(deadline_unix_ms)) {
        Some(deadline) => deadline.duration_since(SystemTime::now()).unwrap_or(Duration::ZERO),
        None => MAX_DEADLINE_OFFSET,
    };
    now.checked_add(remaining).unwrap_or(now + MAX_DEADLINE_OFFSET)
}

#[cfg(test)]
mod tests {
    use super::{deadline_unix_millis, timeout_millis};
    use std::time::Duration;

    #[test]
    fn test_millisecond_overflow_saturates() {
        assert_eq!(timeout_millis(Duration::MAX), u64::MAX);
        assert_eq!(deadline_unix_millis(Duration::MAX), u64::MAX);
    }
}

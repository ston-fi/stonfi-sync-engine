use std::time::{Duration, SystemTime, UNIX_EPOCH};
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};

pub(crate) fn timeout_deadline(duration: Duration, name: &str) -> SyncCoreResult<tokio::time::Instant> {
    if duration.is_zero() {
        return Err(SyncCoreError::invalid_args(format!("{name} must be positive")));
    }
    tokio::time::Instant::now()
        .checked_add(duration)
        .ok_or_else(|| SyncCoreError::invalid_args(format!("{name} is too large")))
}

pub(crate) fn validate_timeout(duration: Duration, name: &str) -> SyncCoreResult<()> {
    timeout_deadline(duration, name).map(|_| ())
}

pub(crate) fn timeout_millis(duration: Duration, name: &str) -> SyncCoreResult<u64> {
    validate_timeout(duration, name)?;
    let milliseconds = u64::try_from(duration.as_millis())
        .map_err(|_| SyncCoreError::invalid_args(format!("{name} exceeds u64 milliseconds")))?;
    if milliseconds == 0 {
        return Err(SyncCoreError::invalid_args(format!("{name} must be at least one millisecond")));
    }
    Ok(milliseconds)
}

pub(crate) fn validate_timeout_millis(duration: Duration, name: &str) -> SyncCoreResult<()> {
    timeout_millis(duration, name).map(|_| ())
}

pub(crate) fn deadline_unix_millis(duration: Duration, name: &str) -> SyncCoreResult<u64> {
    validate_timeout_millis(duration, name)?;
    let deadline = SystemTime::now()
        .checked_add(duration)
        .ok_or_else(|| SyncCoreError::invalid_args(format!("{name} is too large")))?;
    let milliseconds = deadline.duration_since(UNIX_EPOCH).map_err(SyncCoreError::system)?.as_millis();
    u64::try_from(milliseconds)
        .map_err(|_| SyncCoreError::invalid_args(format!("{name} deadline exceeds u64 milliseconds")))
}

pub(crate) fn deadline_from_unix_millis(deadline_unix_ms: u64, name: &str) -> SyncCoreResult<tokio::time::Instant> {
    if deadline_unix_ms == 0 {
        return Err(SyncCoreError::invalid_args(format!("{name} must be positive")));
    }
    let deadline = UNIX_EPOCH
        .checked_add(Duration::from_millis(deadline_unix_ms))
        .ok_or_else(|| SyncCoreError::invalid_args(format!("{name} is too large")))?;
    let remaining = deadline
        .duration_since(SystemTime::now())
        .map_err(|_| SyncCoreError::net(format!("{name} has elapsed")))?;
    timeout_deadline(remaining, name)
}

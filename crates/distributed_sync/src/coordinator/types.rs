use crate::utils::{deadline_from_unix_millis, deadline_unix_millis};
use std::time::Duration;

/// Coordinator dispatch priority.
///
/// Each queue dispatches higher priorities first and preserves FIFO within a
/// priority. Service-capable workers prefer the service queue.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
pub enum TaskPriority {
    /// Lowest available priority.
    Lowest,
    /// Lower than normal priority.
    Low,
    /// Default priority.
    #[default]
    Normal,
    /// Higher than normal priority.
    High,
    /// Highest available priority.
    Highest,
}

#[derive(Clone, Copy)]
pub(crate) struct TaskDeadline {
    pub(super) instant: tokio::time::Instant,
    pub(super) unix_ms: u64,
}

impl TaskDeadline {
    pub(crate) fn new(timeout: Duration) -> Self {
        let unix_ms = deadline_unix_millis(timeout);
        Self {
            instant: deadline_from_unix_millis(unix_ms),
            unix_ms,
        }
    }
}

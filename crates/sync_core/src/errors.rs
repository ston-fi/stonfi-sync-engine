use std::error::Error;
use std::sync::Arc;
use thiserror::Error;

/// Result type returned by sync-core operations.
pub type SyncCoreResult<T> = Result<T, SyncCoreError>;

/// Errors reported while configuring or running a sync engine.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
pub enum SyncCoreError {
    /// An internal or runtime dependency failed.
    #[error("System: {0:?}")]
    System(String),
    /// A caller supplied invalid configuration or input.
    #[error("InvalidArgs: {0}")]
    InvalidArgs(String),
    /// An engine invariant or configuration rule was violated.
    #[error("LogicError: {0:?}")]
    Logic(String),
    /// A network operation failed.
    #[error("NetError: {0:?}")]
    NetError(String),
    /// An error from a consumer-provided implementation.
    #[error("External: {0}")] // rethrow errors from external modules
    External(Arc<dyn Error + Send + Sync + 'static>),
    /// A consumer-defined error without a more specific classification.
    #[error("Custom: {0}")]
    Custom(String),
}

#[rustfmt::skip]
impl SyncCoreError {
    /// Creates an invalid-argument error.
    pub fn invalid_args(err: impl ToString) -> Self { Self::InvalidArgs(err.to_string()) }
    /// Creates a system error.
    pub fn system(err: impl ToString) -> Self { Self::System(err.to_string()) }
    /// Creates a logic error.
    pub fn logic(err: impl ToString) -> Self { Self::Logic(err.to_string()) }
    /// Creates a network error.
    pub fn net(err: impl ToString) -> Self { Self::NetError(err.to_string()) }
    /// Wraps an external error while preserving it for display and cloning.
    pub fn external(err: impl Into<Arc<dyn Error + Send + Sync + 'static>>) -> Self { Self::External(err.into()) }
    /// Creates a consumer-defined error.
    pub fn custom(err: impl ToString) -> Self { Self::Custom(err.to_string()) }
}

use std::io;

/// Ошибка хранилища Chimaera.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupt storage: {0}")]
    Corruption(String),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error("storage worker stopped")]
    WorkerStopped,
    #[error("write completion deadline exceeded; the accepted ticket remains pending")]
    WritePending(Box<crate::WriteTicket>),
    #[error("resource admission unavailable: {0:?}")]
    Overloaded(crate::OverloadReason),
    #[error("storage admission has stopped")]
    AdmissionStopped,
    #[error("deadline exceeded waiting for {0}; the accepted operation may still complete")]
    OperationPending(&'static str),
    #[error("storage is closed")]
    Closed,
    #[error("storage is locked: {0}")]
    Lock(String),
    #[error("catalog error: {0}")]
    Catalog(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("expired point: {0}")]
    ExpiredPoint(String),
    #[error("write queue is full")]
    Backpressure,
    #[error("query resource limit: {0}")]
    ResourceLimit(String),
    #[error("insufficient disk space: available {available} bytes, required {required} bytes")]
    InsufficientDiskSpace { available: u64, required: u64 },
    #[error("query was cancelled")]
    QueryCancelled,
    #[error("query deadline exceeded")]
    QueryDeadlineExceeded,
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn corrupt(message: impl Into<String>) -> Error {
    Error::Corruption(message.into())
}

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

pub mod asset;
pub mod kv;
pub mod r2;
pub mod sqlite;

/// Common bound for storage backend error types.
pub trait BackendError: std::error::Error + Send + Sync + 'static {}
impl<E> BackendError for E where E: std::error::Error + Send + Sync + 'static {}

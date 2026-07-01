//! Warning collection mechanism for entry processing.
//!
//! This module provides a task-local warning collector that allows warnings
//! generated during document processing to be associated with specific entries.

use std::cell::RefCell;

tokio::task_local! {
    static WARNINGS: RefCell<Vec<String>>;
}

/// Collect a warning message for the current entry.
/// If called outside of a warning collection scope, the warning is ignored.
pub fn collect(message: impl Into<String>) {
    let _ = WARNINGS.try_with(|warnings| {
        warnings.borrow_mut().push(message.into());
    });
}

/// Run a closure with warning collection enabled, returning the collected warnings.
pub async fn collect_warnings<F, T>(f: F) -> (T, Vec<String>)
where
    F: std::future::Future<Output = T>,
{
    WARNINGS
        .scope(RefCell::new(Vec::new()), async {
            let result = f.await;
            let warnings = WARNINGS.with(|w| std::mem::take(&mut *w.borrow_mut()));
            (result, warnings)
        })
        .await
}

/// Macro to emit a warning that will be collected for the current entry.
#[macro_export]
macro_rules! warn_entry {
    ($($arg:tt)*) => {
        $crate::warning::collect(format!($($arg)*))
    };
}

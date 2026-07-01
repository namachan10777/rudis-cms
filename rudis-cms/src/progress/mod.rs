//! Progress reporting and display.
//!
//! Provides a trait-based abstraction for progress reporting and a factory
//! that picks an implementation based on terminal capabilities.

use std::sync::Arc;

mod fancy;
mod format;
mod null;
mod simple;
mod state;

pub use fancy::FancyReporter;
pub use null::NullReporter;
pub use simple::SimpleReporter;

/// Status of a single entry being processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryStatus {
    Pending,
    Processing,
    ProcessingImages { current: usize, total: usize },
    Uploading,
    Done,
    Failed(String),
}

/// Status of a storage upload operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadStatus {
    Uploading,
    Uploaded,
    Skipped,
    Failed(String),
}

/// Phase of the overall batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchPhase {
    LoadingConfig,
    CompilingSchema,
    ProcessingDocuments,
    UploadingStorage,
    SyncingDatabase,
    CleaningUp,
    Completed,
    Failed(String),
}

/// Progress reporter trait - implement this for different display backends.
pub trait ProgressReporter: Send + Sync {
    fn set_phase(&self, phase: BatchPhase);
    fn register_entries(&self, entries: Vec<String>);
    fn update_entry(&self, entry: &str, status: EntryStatus);
    fn register_upload(&self, entry: &str, object_key: &str);
    fn update_upload(&self, object_key: &str, status: UploadStatus);
    fn add_entry_warning(&self, entry: &str, message: &str);
    fn log_info(&self, message: &str);
    fn log_warn(&self, message: &str);
    fn log_error(&self, message: &str);
    fn finish(&self);
}

/// Create an appropriate reporter based on terminal capabilities.
pub fn create_reporter() -> Arc<dyn ProgressReporter> {
    if console::Term::stderr().is_term() {
        Arc::new(FancyReporter::new())
    } else {
        Arc::new(SimpleReporter::new())
    }
}

/// Register a batch of uploads against the reporter, all with the same status.
/// Uploads without a `source_entry` are filed under `_unknown`.
pub fn register_uploads(
    reporter: &Arc<dyn ProgressReporter>,
    uploads: &[crate::process_data::table::Upload],
    status: UploadStatus,
) {
    for upload in uploads {
        let entry = upload.source_entry.as_deref().unwrap_or("_unknown");
        let key = upload.pointer.to_string();
        reporter.register_upload(entry, &key);
        reporter.update_upload(&key, status.clone());
    }
}

/// Mark every upload in the slice as `Uploaded`.
pub fn mark_uploads_uploaded(
    reporter: &Arc<dyn ProgressReporter>,
    uploads: &[crate::process_data::table::Upload],
) {
    for upload in uploads {
        let key = upload.pointer.to_string();
        reporter.update_upload(&key, UploadStatus::Uploaded);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(reporter: &dyn ProgressReporter) {
        reporter.register_entries(vec!["a.md".into(), "b.md".into()]);
        reporter.update_entry("a.md", EntryStatus::Processing);
        reporter.register_upload("a.md", "kv://ns/a");
        reporter.update_upload("kv://ns/a", UploadStatus::Uploaded);
        reporter.update_entry("a.md", EntryStatus::Done);
        reporter.update_entry("b.md", EntryStatus::Failed("nope".into()));
        reporter.add_entry_warning("a.md", "deprecated field");
    }

    #[test]
    fn null_reporter_accepts_all_calls() {
        let reporter = NullReporter;
        collect(&reporter);
        reporter.finish();
    }

    #[test]
    fn simple_reporter_tracks_basic_counters() {
        let reporter = SimpleReporter::new();
        collect(&reporter);
        let state = reporter.state_for_test();
        assert_eq!(state.stats.total_entries, 2);
        assert_eq!(state.stats.successful_entries, 1);
        assert_eq!(state.stats.failed_entries, 1);
        assert_eq!(state.stats.upload_count, 1);
        assert!(state.entries.contains_key("a.md"));
    }
}

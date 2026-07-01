use super::{BatchPhase, EntryStatus, ProgressReporter, UploadStatus};

/// A no-op reporter for when progress display is disabled.
pub struct NullReporter;

impl ProgressReporter for NullReporter {
    fn set_phase(&self, _phase: BatchPhase) {}
    fn register_entries(&self, _entries: Vec<String>) {}
    fn update_entry(&self, _entry: &str, _status: EntryStatus) {}
    fn register_upload(&self, _entry: &str, _object_key: &str) {}
    fn update_upload(&self, _object_key: &str, _status: UploadStatus) {}
    fn add_entry_warning(&self, _entry: &str, _message: &str) {}
    fn log_info(&self, _message: &str) {}
    fn log_warn(&self, _message: &str) {}
    fn log_error(&self, _message: &str) {}
    fn finish(&self) {}
}

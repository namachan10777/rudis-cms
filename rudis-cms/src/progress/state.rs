//! Internal state shared by the progress reporters.
//!
//! All mutable state lives behind a single `Mutex<State>` accessed via
//! [`StateLock::lock`], which centralises the lock-poisoning panic.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::{EntryStatus, UploadStatus};

#[derive(Debug, Default)]
pub(super) struct Stats {
    pub total_entries: usize,
    pub successful_entries: usize,
    pub failed_entries: usize,
    pub upload_count: usize,
    pub start_time: Option<Instant>,
}

#[derive(Debug, Clone)]
pub(super) struct UploadInfo {
    pub key: String,
    pub status: UploadStatus,
}

#[derive(Debug, Default)]
pub(super) struct EntryInfo {
    pub uploads: Vec<UploadInfo>,
    pub warnings: Vec<String>,
    pub status: Option<EntryStatus>,
}

#[derive(Debug)]
pub(super) struct State {
    pub stats: Stats,
    pub entries: HashMap<String, EntryInfo>,
    pub object_to_entry: HashMap<String, String>,
}

impl State {
    pub fn new() -> Self {
        Self {
            stats: Stats {
                start_time: Some(Instant::now()),
                ..Default::default()
            },
            entries: HashMap::new(),
            object_to_entry: HashMap::new(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.stats
            .start_time
            .map(|t| t.elapsed())
            .unwrap_or_default()
    }

    /// Initialise tracking slots for the given entries and update
    /// `total_entries`. Subsequent updates reuse these slots.
    pub fn register_entries(&mut self, entries: Vec<String>) {
        self.stats.total_entries = entries.len();
        for entry in entries {
            self.entries.insert(entry, EntryInfo::default());
        }
    }

    /// Record an entry's status; returns the status back so the caller can
    /// drive any reporter-specific I/O on the same value.
    pub fn update_entry(&mut self, entry: &str, status: EntryStatus) -> EntryStatus {
        if let Some(info) = self.entries.get_mut(entry) {
            info.status = Some(status.clone());
        }
        match &status {
            EntryStatus::Done => self.stats.successful_entries += 1,
            EntryStatus::Failed(_) => self.stats.failed_entries += 1,
            _ => {}
        }
        status
    }

    /// Track a new upload under the given entry, with an initial
    /// `Uploading` status.
    pub fn register_upload(&mut self, entry: &str, object_key: &str) {
        let info = self.entries.entry(entry.to_string()).or_default();
        info.uploads.push(UploadInfo {
            key: object_key.to_string(),
            status: UploadStatus::Uploading,
        });
        self.object_to_entry
            .insert(object_key.to_string(), entry.to_string());
    }

    /// Update an upload's status; returns the status back.
    pub fn update_upload(&mut self, object_key: &str, status: UploadStatus) -> UploadStatus {
        if let Some(entry) = self.object_to_entry.get(object_key).cloned()
            && let Some(info) = self.entries.get_mut(&entry)
            && let Some(upload) = info.uploads.iter_mut().find(|u| u.key == object_key)
        {
            upload.status = status.clone();
        }
        if matches!(status, UploadStatus::Uploaded | UploadStatus::Skipped) {
            self.stats.upload_count += 1;
        }
        status
    }

    pub fn add_entry_warning(&mut self, entry: &str, message: &str) {
        if let Some(info) = self.entries.get_mut(entry) {
            info.warnings.push(message.to_string());
        }
    }
}

/// Wrapper that hides the lock-poisoning concern from callers. We `expect`
/// rather than propagate because reporters cannot meaningfully recover from a
/// poisoned progress lock.
pub(super) struct StateLock(Mutex<State>);

impl StateLock {
    pub fn new() -> Self {
        Self(Mutex::new(State::new()))
    }

    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.0.lock().expect("progress lock poisoned")
    }
}

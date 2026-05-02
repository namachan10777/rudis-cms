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

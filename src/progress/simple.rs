//! Reporter for non-TTY environments. Logs phase transitions and failures
//! line-by-line and prints a tree summary on `finish()`.

use std::io::{Write, stderr};

use super::{
    BatchPhase, EntryStatus, ProgressReporter, UploadStatus,
    format::{pad_to_width, write_entries_tree, write_summary},
    state::{EntryInfo, StateLock, UploadInfo},
};

pub struct SimpleReporter {
    state: StateLock,
}

impl SimpleReporter {
    pub fn new() -> Self {
        Self {
            state: StateLock::new(),
        }
    }
}

impl Default for SimpleReporter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl SimpleReporter {
    pub(super) fn state_for_test(&self) -> std::sync::MutexGuard<'_, super::state::State> {
        self.state.lock()
    }
}

impl ProgressReporter for SimpleReporter {
    fn set_phase(&self, phase: BatchPhase) {
        let (emoji, msg) = match phase {
            BatchPhase::LoadingConfig => ("📋", "Loading configuration..."),
            BatchPhase::CompilingSchema => ("🔧", "Compiling schema..."),
            BatchPhase::ProcessingDocuments => ("📄", "Processing documents..."),
            BatchPhase::UploadingStorage => ("⬆️", "Uploading to storage..."),
            BatchPhase::SyncingDatabase => ("🗄️", "Syncing database..."),
            BatchPhase::CleaningUp => ("🧹", "Cleaning up old objects..."),
            BatchPhase::Completed => ("✅", "Completed!"),
            BatchPhase::Failed(ref e) => {
                eprintln!("{} Failed: {e}", pad_to_width("❌", 2));
                return;
            }
        };
        eprintln!("{} {}", pad_to_width(emoji, 2), msg);
    }

    fn register_entries(&self, entries: Vec<String>) {
        let mut state = self.state.lock();
        state.stats.total_entries = entries.len();
        for entry in entries {
            state.entries.insert(entry, EntryInfo::default());
        }
        eprintln!("   Found {} entries", state.stats.total_entries);
    }

    fn update_entry(&self, entry: &str, status: EntryStatus) {
        let mut state = self.state.lock();
        if let Some(info) = state.entries.get_mut(entry) {
            info.status = Some(status.clone());
        }
        match status {
            EntryStatus::Done => state.stats.successful_entries += 1,
            EntryStatus::Failed(ref e) => {
                state.stats.failed_entries += 1;
                eprintln!("   {} {}: {}", pad_to_width("❌", 2), entry, e);
            }
            _ => {}
        }
    }

    fn register_upload(&self, entry: &str, object_key: &str) {
        let mut state = self.state.lock();
        let info = state
            .entries
            .entry(entry.to_string())
            .or_default();
        info.uploads.push(UploadInfo {
            key: object_key.to_string(),
            status: UploadStatus::Uploading,
        });
        state
            .object_to_entry
            .insert(object_key.to_string(), entry.to_string());
    }

    fn update_upload(&self, object_key: &str, status: UploadStatus) {
        let mut state = self.state.lock();
        if let Some(entry) = state.object_to_entry.get(object_key).cloned()
            && let Some(info) = state.entries.get_mut(&entry)
            && let Some(upload) = info.uploads.iter_mut().find(|u| u.key == object_key)
        {
            upload.status = status.clone();
        }
        match status {
            UploadStatus::Uploaded | UploadStatus::Skipped => state.stats.upload_count += 1,
            UploadStatus::Failed(ref e) => {
                eprintln!("   {} upload {}: {}", pad_to_width("❌", 2), object_key, e);
            }
            _ => {}
        }
    }

    fn add_entry_warning(&self, entry: &str, message: &str) {
        {
            let mut state = self.state.lock();
            if let Some(info) = state.entries.get_mut(entry) {
                info.warnings.push(message.to_string());
            }
        }
        eprintln!("   {} {}: {}", pad_to_width("⚠️", 2), entry, message);
    }

    fn log_info(&self, message: &str) {
        eprintln!("{} {}", pad_to_width("ℹ️", 2), message);
    }

    fn log_warn(&self, message: &str) {
        eprintln!("{} {}", pad_to_width("⚠️", 2), message);
    }

    fn log_error(&self, message: &str) {
        eprintln!("{} {}", pad_to_width("❌", 2), message);
    }

    fn finish(&self) {
        let state = self.state.lock();
        let mut out = stderr().lock();
        let _ = writeln!(out);
        let _ = writeln!(out, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        let _ = writeln!(out, "{} Summary", pad_to_width("📊", 2));
        let _ = writeln!(out, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        write_entries_tree(&mut out, &state);
        write_summary(&mut out, &state);
    }
}

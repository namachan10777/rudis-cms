//! Reporter for non-TTY environments. Logs phase transitions and failures
//! line-by-line and prints a tree summary on `finish()`.

use std::io::{Write, stderr};

use super::{
    BatchPhase, EntryStatus, ProgressReporter, UploadStatus,
    format::{pad_to_width, write_entries_tree, write_summary},
    state::StateLock,
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
        state.register_entries(entries);
        eprintln!("   Found {} entries", state.stats.total_entries);
    }

    fn update_entry(&self, entry: &str, status: EntryStatus) {
        let status = self.state.lock().update_entry(entry, status);
        if let EntryStatus::Failed(e) = status {
            eprintln!("   {} {}: {}", pad_to_width("❌", 2), entry, e);
        }
    }

    fn register_upload(&self, entry: &str, object_key: &str) {
        self.state.lock().register_upload(entry, object_key);
    }

    fn update_upload(&self, object_key: &str, status: UploadStatus) {
        let status = self.state.lock().update_upload(object_key, status);
        if let UploadStatus::Failed(e) = status {
            eprintln!("   {} upload {}: {}", pad_to_width("❌", 2), object_key, e);
        }
    }

    fn add_entry_warning(&self, entry: &str, message: &str) {
        self.state.lock().add_entry_warning(entry, message);
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

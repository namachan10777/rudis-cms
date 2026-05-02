//! Reporter for interactive (TTY) environments. Renders spinners for
//! in-flight work via `indicatif` and prints a tree summary on `finish()`.

use std::collections::HashMap;
use std::io::stderr;
use std::sync::Mutex;

use super::{
    BatchPhase, EntryStatus, ProgressReporter, UploadStatus,
    format::{pad_to_width, write_entries_tree, write_summary},
    state::{EntryInfo, StateLock, UploadInfo},
};

pub struct FancyReporter {
    multi: indicatif::MultiProgress,
    active_entries: Mutex<HashMap<String, indicatif::ProgressBar>>,
    active_uploads: Mutex<HashMap<String, indicatif::ProgressBar>>,
    state: StateLock,
}

impl FancyReporter {
    pub fn new() -> Self {
        Self {
            multi: indicatif::MultiProgress::new(),
            active_entries: Mutex::new(HashMap::new()),
            active_uploads: Mutex::new(HashMap::new()),
            state: StateLock::new(),
        }
    }

    fn lock_active_entries(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, indicatif::ProgressBar>> {
        self.active_entries.lock().expect("progress lock poisoned")
    }

    fn lock_active_uploads(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, indicatif::ProgressBar>> {
        self.active_uploads.lock().expect("progress lock poisoned")
    }

    fn status_detail(status: &EntryStatus) -> &'static str {
        match status {
            EntryStatus::Pending => "pending",
            EntryStatus::Processing => "processing",
            EntryStatus::ProcessingImages { .. } => "processing images",
            EntryStatus::Uploading => "uploading",
            EntryStatus::Done => "done",
            EntryStatus::Failed(_) => "failed",
        }
    }

    fn create_spinner(&self, message: String) -> indicatif::ProgressBar {
        let pb = self.multi.add(indicatif::ProgressBar::new_spinner());
        pb.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("   {spinner:.cyan} {msg}")
                .expect("static template is valid"),
        );
        pb.set_message(message);
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        pb
    }
}

impl Default for FancyReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressReporter for FancyReporter {
    fn set_phase(&self, phase: BatchPhase) {
        let msg = match phase {
            BatchPhase::LoadingConfig => {
                format!("{} Loading configuration...", pad_to_width("📋", 2))
            }
            BatchPhase::CompilingSchema => format!("{} Compiling schema...", pad_to_width("🔧", 2)),
            BatchPhase::ProcessingDocuments => {
                format!("{} Processing documents...", pad_to_width("📄", 2))
            }
            BatchPhase::UploadingStorage => {
                format!("{} Uploading to storage...", pad_to_width("⬆️", 2))
            }
            BatchPhase::SyncingDatabase => format!("{} Syncing database...", pad_to_width("🗄️", 2)),
            BatchPhase::CleaningUp => {
                format!("{} Cleaning up old objects...", pad_to_width("🧹", 2))
            }
            BatchPhase::Completed => format!("{} Completed!", pad_to_width("✅", 2)),
            BatchPhase::Failed(ref e) => {
                self.multi
                    .println(format!("{} Failed: {e}", pad_to_width("❌", 2)))
                    .ok();
                return;
            }
        };
        self.multi.println(msg).ok();
    }

    fn register_entries(&self, entries: Vec<String>) {
        let mut state = self.state.lock();
        state.stats.total_entries = entries.len();
        for entry in entries {
            state.entries.insert(entry, EntryInfo::default());
        }
    }

    fn update_entry(&self, entry: &str, status: EntryStatus) {
        if matches!(status, EntryStatus::Done | EntryStatus::Failed(_)) {
            if let Some(pb) = self.lock_active_entries().remove(entry) {
                pb.finish_and_clear();
            }
            let mut state = self.state.lock();
            if let Some(info) = state.entries.get_mut(entry) {
                info.status = Some(status.clone());
            }
            match status {
                EntryStatus::Done => state.stats.successful_entries += 1,
                EntryStatus::Failed(_) => state.stats.failed_entries += 1,
                _ => {}
            }
            return;
        }

        let detail = Self::status_detail(&status);
        let mut active = self.lock_active_entries();
        if let Some(pb) = active.get(entry) {
            pb.set_message(format!("{}: {}", entry, detail));
        } else {
            let pb = self.create_spinner(format!("{}: {}", entry, detail));
            active.insert(entry.to_string(), pb);
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
        {
            let mut state = self.state.lock();
            if let Some(entry) = state.object_to_entry.get(object_key).cloned()
                && let Some(info) = state.entries.get_mut(&entry)
                && let Some(upload) = info.uploads.iter_mut().find(|u| u.key == object_key)
            {
                upload.status = status.clone();
            }
            if matches!(status, UploadStatus::Uploaded | UploadStatus::Skipped) {
                state.stats.upload_count += 1;
            }
        }

        if matches!(
            status,
            UploadStatus::Uploaded | UploadStatus::Skipped | UploadStatus::Failed(_)
        ) {
            if let Some(pb) = self.lock_active_uploads().remove(object_key) {
                pb.finish_and_clear();
            }
            return;
        }

        let mut active = self.lock_active_uploads();
        if let Some(pb) = active.get(object_key) {
            pb.set_message(format!("{} {}", pad_to_width("⏳", 2), object_key));
        } else {
            let pb = self.create_spinner(format!("{} {}", pad_to_width("⏳", 2), object_key));
            active.insert(object_key.to_string(), pb);
        }
    }

    fn add_entry_warning(&self, entry: &str, message: &str) {
        let mut state = self.state.lock();
        if let Some(info) = state.entries.get_mut(entry) {
            info.warnings.push(message.to_string());
        }
    }

    fn log_info(&self, message: &str) {
        self.multi
            .println(format!("{} {}", pad_to_width("ℹ️", 2), message))
            .ok();
    }

    fn log_warn(&self, message: &str) {
        self.multi
            .println(format!("{} {}", pad_to_width("⚠️", 2), message))
            .ok();
    }

    fn log_error(&self, message: &str) {
        self.multi
            .println(format!("{} {}", pad_to_width("❌", 2), message))
            .ok();
    }

    fn finish(&self) {
        for (_, pb) in self.lock_active_entries().drain() {
            pb.finish_and_clear();
        }
        for (_, pb) in self.lock_active_uploads().drain() {
            pb.finish_and_clear();
        }

        let state = self.state.lock();
        let mut out = stderr().lock();
        write_entries_tree(&mut out, &state);
        write_summary(&mut out, &state);
    }
}

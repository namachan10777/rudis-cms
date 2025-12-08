//! Progress reporting and display
//!
//! This module provides a trait-based abstraction for progress reporting,
//! allowing the core logic to remain decoupled from display concerns.

use std::collections::HashMap;
use std::sync::Arc;

/// Status of a single entry being processed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryStatus {
    /// Waiting to be processed
    Pending,
    /// Currently being processed
    Processing,
    /// Processing images
    ProcessingImages { current: usize, total: usize },
    /// Uploading to storage
    Uploading,
    /// Successfully completed
    Done,
    /// Failed with error
    Failed(String),
}

/// Status of a storage upload operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadStatus {
    /// Currently uploading
    Uploading,
    /// Successfully uploaded
    Done,
    /// Failed with error
    Failed(String),
}

/// Phase of the overall batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchPhase {
    /// Loading configuration
    LoadingConfig,
    /// Compiling schema
    CompilingSchema,
    /// Processing documents
    ProcessingDocuments,
    /// Uploading to R2/KV/Asset storage
    UploadingStorage,
    /// Syncing database
    SyncingDatabase,
    /// Cleaning up old objects
    CleaningUp,
    /// Completed successfully
    Completed,
    /// Failed with error
    Failed(String),
}

/// Progress reporter trait - implement this for different display backends.
pub trait ProgressReporter: Send + Sync {
    /// Set the overall batch phase.
    fn set_phase(&self, phase: BatchPhase);

    /// Register entries to track (call before processing starts).
    fn register_entries(&self, entries: Vec<String>);

    /// Update the status of a specific entry.
    fn update_entry(&self, entry: &str, status: EntryStatus);

    /// Register a storage object belonging to an entry.
    fn register_upload(&self, entry: &str, object_key: &str);

    /// Update the status of a storage upload.
    fn update_upload(&self, object_key: &str, status: UploadStatus);

    /// Add a warning associated with an entry (shown in tree).
    fn add_entry_warning(&self, entry: &str, message: &str);

    /// Log an informational message.
    fn log_info(&self, message: &str);

    /// Log a warning message (not associated with an entry).
    fn log_warn(&self, message: &str);

    /// Log an error message.
    fn log_error(&self, message: &str);

    /// Finish and clean up the display.
    fn finish(&self);
}

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

/// A simple reporter that just prints to stderr (for non-TTY).
pub struct SimpleReporter {
    stats: std::sync::RwLock<Stats>,
    /// Maps entry name to list of object keys
    entry_objects: std::sync::RwLock<HashMap<String, Vec<String>>>,
    /// Maps object key to entry name (for reverse lookup)
    object_to_entry: std::sync::RwLock<HashMap<String, String>>,
}

impl SimpleReporter {
    pub fn new() -> Self {
        Self {
            stats: std::sync::RwLock::new(Stats {
                start_time: Some(std::time::Instant::now()),
                ..Default::default()
            }),
            entry_objects: std::sync::RwLock::new(HashMap::new()),
            object_to_entry: std::sync::RwLock::new(HashMap::new()),
        }
    }

    fn print_summary(&self) {
        let stats = self.stats.read().unwrap();
        let entry_objects = self.entry_objects.read().unwrap();
        let duration = stats.start_time.map(|t| t.elapsed()).unwrap_or_default();

        eprintln!();
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        eprintln!("📊 Summary");
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Print tree of entries and their objects
        let mut entries: Vec<_> = entry_objects.iter().collect();
        entries.sort_by_key(|(k, _)| *k);

        for (i, (entry, objects)) in entries.iter().enumerate() {
            let is_last_entry = i == entries.len() - 1;
            let entry_prefix = if is_last_entry {
                "└──"
            } else {
                "├──"
            };
            eprintln!("{entry_prefix} 📄 {entry}");

            for (j, obj) in objects.iter().enumerate() {
                let is_last_obj = j == objects.len() - 1;
                let branch = if is_last_entry { "    " } else { "│   " };
                let obj_prefix = if is_last_obj {
                    "└──"
                } else {
                    "├──"
                };
                eprintln!("{branch}{obj_prefix} ☁️  {obj}");
            }
        }

        eprintln!();
        eprintln!("   📄 Entries:    {} total", stats.total_entries);
        eprintln!("   ✅ Successful: {}", stats.successful_entries);
        if stats.failed_entries > 0 {
            eprintln!("   ❌ Failed:     {}", stats.failed_entries);
        }
        if stats.upload_count > 0 {
            eprintln!("   ☁️  Uploads:    {}", stats.upload_count);
        }
        eprintln!("   ⏱️  Duration:   {:.2}s", duration.as_secs_f64());
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
}

impl Default for SimpleReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressReporter for SimpleReporter {
    fn set_phase(&self, phase: BatchPhase) {
        let (emoji, msg) = match phase {
            BatchPhase::LoadingConfig => ("📋", "Loading configuration..."),
            BatchPhase::CompilingSchema => ("🔧", "Compiling schema..."),
            BatchPhase::ProcessingDocuments => ("📄", "Processing documents..."),
            BatchPhase::UploadingStorage => ("☁️ ", "Uploading to storage..."),
            BatchPhase::SyncingDatabase => ("🗄️ ", "Syncing database..."),
            BatchPhase::CleaningUp => ("🧹", "Cleaning up old objects..."),
            BatchPhase::Completed => ("✅", "Completed!"),
            BatchPhase::Failed(ref e) => {
                eprintln!("❌ Failed: {e}");
                return;
            }
        };
        eprintln!("{emoji} {msg}");
    }

    fn register_entries(&self, entries: Vec<String>) {
        self.stats.write().unwrap().total_entries = entries.len();
        eprintln!("   Found {} entries", entries.len());
    }

    fn update_entry(&self, entry: &str, status: EntryStatus) {
        match status {
            EntryStatus::Done => {
                self.stats.write().unwrap().successful_entries += 1;
            }
            EntryStatus::Failed(ref e) => {
                self.stats.write().unwrap().failed_entries += 1;
                eprintln!("   ✗ {entry}: {e}");
            }
            _ => {}
        }
    }

    fn register_upload(&self, entry: &str, object_key: &str) {
        self.entry_objects
            .write()
            .unwrap()
            .entry(entry.to_string())
            .or_default()
            .push(object_key.to_string());
        self.object_to_entry
            .write()
            .unwrap()
            .insert(object_key.to_string(), entry.to_string());
    }

    fn update_upload(&self, object_key: &str, status: UploadStatus) {
        match status {
            UploadStatus::Done => {
                self.stats.write().unwrap().upload_count += 1;
            }
            UploadStatus::Failed(ref e) => {
                eprintln!("   ✗ upload {object_key}: {e}");
            }
            _ => {}
        }
    }

    fn add_entry_warning(&self, entry: &str, message: &str) {
        eprintln!("   ⚠️  {entry}: {message}");
    }

    fn log_info(&self, message: &str) {
        eprintln!("ℹ️  {message}");
    }

    fn log_warn(&self, message: &str) {
        eprintln!("⚠️  {message}");
    }

    fn log_error(&self, message: &str) {
        eprintln!("❌ {message}");
    }

    fn finish(&self) {
        self.print_summary();
    }
}

/// Statistics collected during processing.
#[derive(Debug, Default)]
struct Stats {
    total_entries: usize,
    successful_entries: usize,
    failed_entries: usize,
    upload_count: usize,
    start_time: Option<std::time::Instant>,
}

/// Entry info for tracking warnings and uploads per entry.
#[derive(Debug, Default)]
struct EntryInfo {
    uploads: Vec<String>,
    warnings: Vec<String>,
    status: Option<EntryStatus>,
}

/// Fancy interactive reporter with progress bars (for TTY).
/// Completed items stay at top, in-progress items shown at bottom with spinners.
pub struct FancyReporter {
    multi: indicatif::MultiProgress,
    /// Active spinner bars for in-progress entries (entry name -> progress bar)
    active_entries: std::sync::RwLock<HashMap<String, indicatif::ProgressBar>>,
    /// Active spinner bars for in-progress uploads (object key -> progress bar)
    active_uploads: std::sync::RwLock<HashMap<String, indicatif::ProgressBar>>,
    stats: std::sync::RwLock<Stats>,
    /// Per-entry info (uploads, warnings)
    entry_info: std::sync::RwLock<HashMap<String, EntryInfo>>,
    /// Maps object key to entry name (for reverse lookup)
    object_to_entry: std::sync::RwLock<HashMap<String, String>>,
}

impl FancyReporter {
    pub fn new() -> Self {
        let multi = indicatif::MultiProgress::new();

        Self {
            multi,
            active_entries: std::sync::RwLock::new(HashMap::new()),
            active_uploads: std::sync::RwLock::new(HashMap::new()),
            stats: std::sync::RwLock::new(Stats {
                start_time: Some(std::time::Instant::now()),
                ..Default::default()
            }),
            entry_info: std::sync::RwLock::new(HashMap::new()),
            object_to_entry: std::sync::RwLock::new(HashMap::new()),
        }
    }

    fn print_summary(&self) {
        let stats = self.stats.read().unwrap();
        let duration = stats.start_time.map(|t| t.elapsed()).unwrap_or_default();

        eprintln!();
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        eprintln!("   📄 Entries:    {} total", stats.total_entries);
        eprintln!("   ✅ Successful: {}", stats.successful_entries);
        if stats.failed_entries > 0 {
            eprintln!("   ❌ Failed:     {}", stats.failed_entries);
        }
        if stats.upload_count > 0 {
            eprintln!("   ☁️  Uploads:    {}", stats.upload_count);
        }
        eprintln!("   ⏱️  Duration:   {:.2}s", duration.as_secs_f64());
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
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
                .unwrap(),
        );
        pb.set_message(message);
        pb.enable_steady_tick(std::time::Duration::from_millis(80));
        pb
    }

    /// Print a completed entry with its tree of uploads and warnings
    fn print_completed_entry(&self, entry: &str, info: &EntryInfo) {
        let status_icon = match &info.status {
            Some(EntryStatus::Done) => "✅",
            Some(EntryStatus::Failed(_)) => "❌",
            _ => "✅",
        };

        let has_children = !info.uploads.is_empty() || !info.warnings.is_empty();

        if has_children {
            self.multi
                .println(format!("├── {status_icon} {entry}"))
                .ok();

            // Print warnings
            for (i, warning) in info.warnings.iter().enumerate() {
                let is_last = i == info.warnings.len() - 1 && info.uploads.is_empty();
                let prefix = if is_last {
                    "│   └──"
                } else {
                    "│   ├──"
                };
                self.multi.println(format!("{prefix} ⚠️  {warning}")).ok();
            }

            // Print uploads
            for (i, upload) in info.uploads.iter().enumerate() {
                let is_last = i == info.uploads.len() - 1;
                let prefix = if is_last {
                    "│   └──"
                } else {
                    "│   ├──"
                };
                self.multi.println(format!("{prefix} ☁️  {upload}")).ok();
            }
        } else {
            self.multi
                .println(format!("├── {status_icon} {entry}"))
                .ok();
        }
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
            BatchPhase::LoadingConfig => "📋 Loading configuration...",
            BatchPhase::CompilingSchema => "🔧 Compiling schema...",
            BatchPhase::ProcessingDocuments => "📄 Processing documents...",
            BatchPhase::UploadingStorage => "☁️  Uploading to storage...",
            BatchPhase::SyncingDatabase => "🗄️  Syncing database...",
            BatchPhase::CleaningUp => "🧹 Cleaning up old objects...",
            BatchPhase::Completed => "✅ Completed!",
            BatchPhase::Failed(ref e) => {
                self.multi.println(format!("❌ Failed: {e}")).ok();
                return;
            }
        };
        self.multi.println(msg).ok();
    }

    fn register_entries(&self, entries: Vec<String>) {
        let total = entries.len();
        self.stats.write().unwrap().total_entries = total;

        // Initialize entry info for all entries
        let mut info_map = self.entry_info.write().unwrap();
        for entry in entries {
            info_map.insert(entry, EntryInfo::default());
        }
    }

    fn update_entry(&self, entry: &str, status: EntryStatus) {
        // If done or failed, print completed line and remove spinner
        if matches!(status, EntryStatus::Done | EntryStatus::Failed(_)) {
            // Remove active spinner if exists
            if let Some(pb) = self.active_entries.write().unwrap().remove(entry) {
                pb.finish_and_clear();
            }

            // Update entry info with final status
            if let Some(info) = self.entry_info.write().unwrap().get_mut(entry) {
                info.status = Some(status.clone());
            }

            // Print completed entry with its tree
            let info_map = self.entry_info.read().unwrap();
            if let Some(info) = info_map.get(entry) {
                self.print_completed_entry(entry, info);
            }

            // Update stats
            let mut stats = self.stats.write().unwrap();
            match status {
                EntryStatus::Done => stats.successful_entries += 1,
                EntryStatus::Failed(_) => stats.failed_entries += 1,
                _ => {}
            }
            return;
        }

        // For in-progress states, create or update spinner
        let mut active = self.active_entries.write().unwrap();
        let detail = Self::status_detail(&status);

        if let Some(pb) = active.get(entry) {
            pb.set_message(format!("{entry}: {detail}"));
        } else {
            let pb = self.create_spinner(format!("{entry}: {detail}"));
            active.insert(entry.to_string(), pb);
        }
    }

    fn register_upload(&self, entry: &str, object_key: &str) {
        // Add to entry's upload list
        if let Some(info) = self.entry_info.write().unwrap().get_mut(entry) {
            info.uploads.push(object_key.to_string());
        }

        // Track reverse mapping
        self.object_to_entry
            .write()
            .unwrap()
            .insert(object_key.to_string(), entry.to_string());
    }

    fn update_upload(&self, object_key: &str, status: UploadStatus) {
        // If done or failed, remove spinner
        if matches!(status, UploadStatus::Done | UploadStatus::Failed(_)) {
            if let Some(pb) = self.active_uploads.write().unwrap().remove(object_key) {
                pb.finish_and_clear();
            }

            // Update stats
            if matches!(status, UploadStatus::Done) {
                self.stats.write().unwrap().upload_count += 1;
            }
            return;
        }

        // For uploading state, create or update spinner
        let mut active = self.active_uploads.write().unwrap();
        if let Some(pb) = active.get(object_key) {
            pb.set_message(format!("☁️  {object_key}"));
        } else {
            let pb = self.create_spinner(format!("☁️  {object_key}"));
            active.insert(object_key.to_string(), pb);
        }
    }

    fn add_entry_warning(&self, entry: &str, message: &str) {
        // Add warning to entry's info (will be shown in tree when entry completes)
        if let Some(info) = self.entry_info.write().unwrap().get_mut(entry) {
            info.warnings.push(message.to_string());
        }
    }

    fn log_info(&self, message: &str) {
        self.multi.println(format!("ℹ️  {message}")).ok();
    }

    fn log_warn(&self, message: &str) {
        self.multi.println(format!("⚠️  {message}")).ok();
    }

    fn log_error(&self, message: &str) {
        self.multi.println(format!("❌ {message}")).ok();
    }

    fn finish(&self) {
        // Clear any remaining entry spinners
        for (_, pb) in self.active_entries.write().unwrap().drain() {
            pb.finish_and_clear();
        }

        // Clear any remaining upload spinners
        for (_, pb) in self.active_uploads.write().unwrap().drain() {
            pb.finish_and_clear();
        }

        // Print summary
        self.print_summary();
    }
}

/// Create an appropriate reporter based on terminal capabilities.
pub fn create_reporter() -> Arc<dyn ProgressReporter> {
    if console::Term::stderr().is_term() {
        Arc::new(FancyReporter::new())
    } else {
        Arc::new(SimpleReporter::new())
    }
}

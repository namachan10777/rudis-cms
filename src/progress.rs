//! Progress reporting and display
//!
//! This module provides a trait-based abstraction for progress reporting,
//! allowing the core logic to remain decoupled from display concerns.

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

    /// Update upload progress.
    fn set_upload_progress(&self, current: usize, total: usize);

    /// Log an informational message.
    fn log_info(&self, message: &str);

    /// Log a warning message.
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
    fn set_upload_progress(&self, _current: usize, _total: usize) {}
    fn log_info(&self, _message: &str) {}
    fn log_warn(&self, _message: &str) {}
    fn log_error(&self, _message: &str) {}
    fn finish(&self) {}
}

/// A simple reporter that just prints to stderr (for non-TTY).
pub struct SimpleReporter {
    stats: std::sync::RwLock<Stats>,
}

impl SimpleReporter {
    pub fn new() -> Self {
        Self {
            stats: std::sync::RwLock::new(Stats {
                start_time: Some(std::time::Instant::now()),
                ..Default::default()
            }),
        }
    }

    fn print_summary(&self) {
        let stats = self.stats.read().unwrap();
        let duration = stats.start_time.map(|t| t.elapsed()).unwrap_or_default();

        eprintln!();
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        eprintln!("📊 Summary");
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
                eprintln!("   ✓ {entry}");
            }
            EntryStatus::Failed(ref e) => {
                self.stats.write().unwrap().failed_entries += 1;
                eprintln!("   ✗ {entry}: {e}");
            }
            _ => {}
        }
    }

    fn set_upload_progress(&self, current: usize, total: usize) {
        self.stats.write().unwrap().upload_count = total;
        if current == total {
            eprintln!("   Uploaded {total} objects");
        }
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

/// Fancy interactive reporter with progress bars (for TTY).
pub struct FancyReporter {
    multi: indicatif::MultiProgress,
    phase_bar: indicatif::ProgressBar,
    entries: std::sync::RwLock<std::collections::HashMap<String, Option<indicatif::ProgressBar>>>,
    main_progress: std::sync::RwLock<Option<indicatif::ProgressBar>>,
    stats: std::sync::RwLock<Stats>,
}

impl FancyReporter {
    pub fn new() -> Self {
        let multi = indicatif::MultiProgress::new();
        let phase_bar = multi.add(indicatif::ProgressBar::new_spinner());
        phase_bar.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .unwrap(),
        );
        phase_bar.enable_steady_tick(std::time::Duration::from_millis(100));

        Self {
            multi,
            phase_bar,
            entries: std::sync::RwLock::new(std::collections::HashMap::new()),
            main_progress: std::sync::RwLock::new(None),
            stats: std::sync::RwLock::new(Stats {
                start_time: Some(std::time::Instant::now()),
                ..Default::default()
            }),
        }
    }

    fn print_summary(&self) {
        let stats = self.stats.read().unwrap();
        let duration = stats.start_time.map(|t| t.elapsed()).unwrap_or_default();

        eprintln!();
        eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        eprintln!("📊 Summary");
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

    fn status_emoji(status: &EntryStatus) -> &'static str {
        match status {
            EntryStatus::Pending => "⏳",
            EntryStatus::Processing => "⚙️ ",
            EntryStatus::ProcessingImages { .. } => "🖼️ ",
            EntryStatus::Uploading => "☁️ ",
            EntryStatus::Done => "✅",
            EntryStatus::Failed(_) => "❌",
        }
    }

    fn status_detail(status: &EntryStatus) -> String {
        match status {
            EntryStatus::Pending => "pending".to_string(),
            EntryStatus::Processing => "processing".to_string(),
            EntryStatus::ProcessingImages { current, total } => {
                format!("images ({current}/{total})")
            }
            EntryStatus::Uploading => "uploading".to_string(),
            EntryStatus::Done => "done".to_string(),
            EntryStatus::Failed(e) => e.clone(),
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
                self.phase_bar
                    .finish_with_message(format!("❌ Failed: {e}"));
                return;
            }
        };
        self.phase_bar.set_message(msg.to_string());

        if matches!(phase, BatchPhase::Completed) {
            self.phase_bar.finish_with_message(msg.to_string());
        }
    }

    fn register_entries(&self, entries: Vec<String>) {
        let mut map = self.entries.write().unwrap();
        let total = entries.len();

        // Update stats
        self.stats.write().unwrap().total_entries = total;

        // Create main progress bar
        let main_pb = self.multi.add(indicatif::ProgressBar::new(total as u64));
        main_pb.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("   {bar:40.cyan/blue} {pos}/{len} entries")
                .unwrap()
                .progress_chars("█▓▒░  "),
        );
        *self.main_progress.write().unwrap() = Some(main_pb);

        // Store all entry names, but don't create progress bars yet
        // Progress bars are created on-demand when entries start processing
        for entry in entries {
            map.insert(entry, None);
        }
    }

    fn update_entry(&self, entry: &str, status: EntryStatus) {
        let mut map = self.entries.write().unwrap();

        // If done or failed, remove the progress bar (hide completed entries)
        if matches!(status, EntryStatus::Done | EntryStatus::Failed(_)) {
            if let Some(Some(pb)) = map.get(entry) {
                pb.finish_and_clear();
            }
            map.remove(entry);

            if let Some(ref main_pb) = *self.main_progress.read().unwrap() {
                main_pb.inc(1);
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

        // For in-progress states, create or update progress bar
        if let Some(entry_slot) = map.get_mut(entry) {
            let emoji = Self::status_emoji(&status);
            let detail = Self::status_detail(&status);

            if let Some(pb) = entry_slot {
                // Update existing progress bar
                pb.set_message(format!("{emoji} {entry}: {detail}"));
            } else {
                // Create new progress bar for this entry
                let pb = self.multi.add(indicatif::ProgressBar::new_spinner());
                pb.set_style(
                    indicatif::ProgressStyle::default_spinner()
                        .template("   {msg}")
                        .unwrap(),
                );
                pb.set_message(format!("{emoji} {entry}: {detail}"));
                pb.enable_steady_tick(std::time::Duration::from_millis(100));
                *entry_slot = Some(pb);
            }
        }
    }

    fn set_upload_progress(&self, current: usize, total: usize) {
        self.phase_bar
            .set_message(format!("☁️  Uploading to storage... ({current}/{total})"));

        // Update stats with total upload count
        self.stats.write().unwrap().upload_count = total;
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
        // Clear any remaining entry bars
        let map = self.entries.read().unwrap();
        for entry_slot in map.values() {
            if let Some(pb) = entry_slot {
                pb.finish_and_clear();
            }
        }

        // Finish main progress
        if let Some(ref main_pb) = *self.main_progress.read().unwrap() {
            main_pb.finish_and_clear();
        }

        self.phase_bar.finish_and_clear();

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

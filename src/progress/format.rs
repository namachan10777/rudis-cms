//! Display formatting helpers shared by the progress reporters.

use std::io::Write;

use unicode_width::UnicodeWidthStr;

use super::{
    UploadStatus,
    state::{EntryInfo, State},
};

/// Pad a string to a given display width, accounting for unicode character widths.
pub(super) fn pad_to_width(s: &str, target_width: usize) -> String {
    let current_width = UnicodeWidthStr::width(s);
    if current_width >= target_width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(target_width - current_width))
    }
}

/// Render the summary footer (counters + duration) to `out`.
pub(super) fn write_summary(out: &mut dyn Write, state: &State) {
    let duration = state.elapsed();
    let _ = writeln!(out);
    let _ = writeln!(out, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    let _ = writeln!(
        out,
        "   {} Entries:    {} total",
        pad_to_width("📄", 2),
        state.stats.total_entries
    );
    let _ = writeln!(
        out,
        "   {} Successful: {}",
        pad_to_width("✅", 2),
        state.stats.successful_entries
    );
    if state.stats.failed_entries > 0 {
        let _ = writeln!(
            out,
            "   {} Failed:     {}",
            pad_to_width("❌", 2),
            state.stats.failed_entries
        );
    }
    if state.stats.upload_count > 0 {
        let _ = writeln!(
            out,
            "   {} Uploads:    {}",
            pad_to_width("⬆️", 2),
            state.stats.upload_count
        );
    }
    let _ = writeln!(
        out,
        "   {} Duration:   {:.2}s",
        pad_to_width("⏱️", 2),
        duration.as_secs_f64()
    );
    let _ = writeln!(out, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}

/// Render the per-entry tree (uploads + warnings) to `out`.
pub(super) fn write_entries_tree(out: &mut dyn Write, state: &State) {
    let mut entries: Vec<(&String, &EntryInfo)> = state
        .entries
        .iter()
        .filter(|(_, info)| {
            !info.uploads.is_empty() || !info.warnings.is_empty() || info.status.is_some()
        })
        .collect();
    entries.sort_by_key(|(k, _)| k.as_str());

    if entries.is_empty() {
        return;
    }

    for (i, (entry, info)) in entries.iter().enumerate() {
        let is_last = i == entries.len() - 1;
        write_entry_tree(out, entry, info, is_last);
    }
}

fn write_entry_tree(out: &mut dyn Write, entry: &str, info: &EntryInfo, is_last: bool) {
    use crate::progress::EntryStatus;

    let status_icon = match &info.status {
        Some(EntryStatus::Done) => pad_to_width("✅", 2),
        Some(EntryStatus::Failed(_)) => pad_to_width("❌", 2),
        _ => pad_to_width("📄", 2),
    };

    let entry_prefix = if is_last { "└──" } else { "├──" };
    let branch = if is_last { "    " } else { "│   " };

    let _ = writeln!(out, "{} {} {}", entry_prefix, status_icon, entry);

    let total_children = info.warnings.len() + info.uploads.len();
    let mut child_idx = 0;

    for warning in &info.warnings {
        child_idx += 1;
        let is_last_child = child_idx == total_children;
        let child_prefix = if is_last_child {
            "└──"
        } else {
            "├──"
        };
        let _ = writeln!(
            out,
            "{}{} {} {}",
            branch,
            child_prefix,
            pad_to_width("⚠️", 2),
            warning
        );
    }

    for upload in &info.uploads {
        child_idx += 1;
        let is_last_child = child_idx == total_children;
        let child_prefix = if is_last_child {
            "└──"
        } else {
            "├──"
        };
        match &upload.status {
            UploadStatus::Uploaded => {
                let _ = writeln!(
                    out,
                    "{}{} {} {}",
                    branch,
                    child_prefix,
                    pad_to_width("⬆️", 2),
                    upload.key
                );
            }
            UploadStatus::Skipped => {
                let style = console::Style::new().dim();
                let _ = writeln!(
                    out,
                    "{}",
                    style.apply_to(format!(
                        "{}{} {} {}",
                        branch,
                        child_prefix,
                        pad_to_width("⏭️", 2),
                        upload.key
                    ))
                );
            }
            UploadStatus::Uploading => {
                let _ = writeln!(
                    out,
                    "{}{} {} {}",
                    branch,
                    child_prefix,
                    pad_to_width("⏳", 2),
                    upload.key
                );
            }
            UploadStatus::Failed(e) => {
                let _ = writeln!(
                    out,
                    "{}{} {} {} ({})",
                    branch,
                    child_prefix,
                    pad_to_width("❌", 2),
                    upload.key,
                    e
                );
            }
        }
    }
}

//! Upload filtering utilities
//!
//! This module provides functions for filtering uploads based on existing objects.

use std::collections::HashSet;

use indexmap::IndexMap;

use crate::process_data::{self, StoragePointer};

/// Filter uploads to exclude already-present objects (unless force is true).
pub(crate) fn filter_uploads<T>(
    uploads: impl Iterator<Item = process_data::table::Upload>,
    present_objects: &IndexMap<blake3::Hash, T>,
    force: bool,
) -> impl Iterator<Item = process_data::table::Upload> {
    uploads.filter_map(move |upload| {
        if force || !present_objects.contains_key(&upload.hash) {
            Some(upload)
        } else {
            None
        }
    })
}

/// Partition uploads into (to_upload, skipped) based on existing objects.
/// Returns uploads that need to be uploaded and those that can be skipped.
pub fn partition_uploads<T>(
    uploads: process_data::table::Uploads,
    present_objects: &IndexMap<blake3::Hash, T>,
    force: bool,
) -> (process_data::table::Uploads, process_data::table::Uploads) {
    if force {
        (uploads, Vec::new())
    } else {
        uploads
            .into_iter()
            .partition(|upload| !present_objects.contains_key(&upload.hash))
    }
}

/// Find objects that have disappeared (no longer referenced).
pub fn disappeared_objects<'a, T>(
    present_objects: IndexMap<blake3::Hash, StoragePointer>,
    appeared_objects: &'a IndexMap<blake3::Hash, T>,
    mask: &'a HashSet<StoragePointer>,
) -> impl 'a + Iterator<Item = StoragePointer> {
    present_objects
        .into_iter()
        .filter(|(hash, pointer)| !appeared_objects.contains_key(hash) && !mask.contains(pointer))
        .map(|(_, pointer)| pointer)
}

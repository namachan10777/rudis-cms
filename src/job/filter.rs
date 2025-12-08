//! Upload filtering utilities
//!
//! This module provides functions for filtering uploads based on existing objects.

use std::collections::HashSet;

use indexmap::IndexMap;

use crate::process_data::{self, StoragePointer};

/// Filter uploads to exclude already-present objects (unless force is true).
pub fn filter_uploads<T>(
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

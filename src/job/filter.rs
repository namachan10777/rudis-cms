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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process_data::table::Upload;
    use crate::process_data::{StorageContent, StoragePointer};

    fn upload(hash_byte: u8, key: &str) -> Upload {
        let pointer = StoragePointer::Kv {
            namespace: "ns".into(),
            key: key.into(),
        };
        Upload {
            data: StorageContent::Text(String::new()),
            hash: blake3::Hash::from_bytes([hash_byte; 32]),
            pointer,
            content_type: "application/json".into(),
            source_entry: None,
        }
    }

    #[test]
    fn partition_skips_present_objects() {
        let mut present = IndexMap::new();
        present.insert(blake3::Hash::from_bytes([1; 32]), ());
        let uploads = vec![upload(1, "a"), upload(2, "b")];
        let (to_upload, skipped) = partition_uploads(uploads, &present, false);
        assert_eq!(to_upload.len(), 1);
        assert_eq!(skipped.len(), 1);
        assert_eq!(
            to_upload[0].pointer,
            StoragePointer::Kv {
                namespace: "ns".into(),
                key: "b".into()
            }
        );
    }

    #[test]
    fn partition_force_keeps_everything() {
        let mut present = IndexMap::new();
        present.insert(blake3::Hash::from_bytes([1; 32]), ());
        let uploads = vec![upload(1, "a"), upload(2, "b")];
        let (to_upload, skipped) = partition_uploads(uploads, &present, true);
        assert_eq!(to_upload.len(), 2);
        assert!(skipped.is_empty());
    }

    #[test]
    fn disappeared_excludes_masked_pointers() {
        let mut present = IndexMap::new();
        let p_keep = StoragePointer::Kv {
            namespace: "ns".into(),
            key: "still-used".into(),
        };
        let p_disappear = StoragePointer::Kv {
            namespace: "ns".into(),
            key: "gone".into(),
        };
        present.insert(blake3::Hash::from_bytes([1; 32]), p_keep.clone());
        present.insert(blake3::Hash::from_bytes([2; 32]), p_disappear.clone());

        let appeared: IndexMap<blake3::Hash, ()> = IndexMap::new();
        let mut mask = HashSet::new();
        mask.insert(p_keep.clone());

        let result: Vec<_> = disappeared_objects(present, &appeared, &mask).collect();
        assert_eq!(result, vec![p_disappear]);
    }
}

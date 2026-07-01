//! Storage multiplexing utilities
//!
//! This module provides functions for routing uploads and deletions
//! to the appropriate storage backend (R2, KV, Asset).

use std::path::PathBuf;

use crate::process_data::{self, StorageContent, StoragePointer};

/// R2 (object storage) delete operation.
#[derive(Hash, PartialEq, Eq)]
pub struct R2Delete {
    pub bucket: String,
    pub key: String,
}

/// KV (key-value) delete operation.
#[derive(Hash, PartialEq, Eq)]
pub struct KvDelete {
    pub namespace: String,
    pub key: String,
}

/// Asset delete operation.
#[derive(Hash, PartialEq, Eq)]
pub struct AssetDelete {
    pub path: PathBuf,
}

/// R2 (object storage) upload operation.
#[derive(derive_debug::Dbg)]
pub struct R2Upload {
    pub bucket: String,
    pub key: String,
    #[dbg(skip)]
    pub body: Box<[u8]>,
    pub content_type: String,
}

/// KV (key-value) upload operation.
#[derive(derive_debug::Dbg)]
pub struct KvUpload {
    pub namespace: String,
    pub key: String,
    #[dbg(skip)]
    pub content: StorageContent,
}

/// Asset upload operation.
#[derive(derive_debug::Dbg)]
pub struct AssetUpload {
    pub path: PathBuf,
    #[dbg(skip)]
    pub body: Box<[u8]>,
}

/// Route uploads to appropriate storage backends.
pub fn multiplex_upload(
    uploads: impl Iterator<Item = process_data::table::Upload>,
) -> (Vec<R2Upload>, Vec<KvUpload>, Vec<AssetUpload>) {
    let mut r2 = Vec::new();
    let mut kv = Vec::new();
    let mut asset = Vec::new();
    uploads.for_each(|upload| match upload.pointer {
        StoragePointer::Asset { path } => asset.push(AssetUpload {
            path,
            body: upload.data.into(),
        }),
        StoragePointer::Inline { .. } => {}
        StoragePointer::Kv { namespace, key } => kv.push(KvUpload {
            namespace,
            key,
            content: upload.data,
        }),
        StoragePointer::R2 { bucket, key } => r2.push(R2Upload {
            key,
            bucket,
            body: upload.data.into(),
            content_type: upload.content_type,
        }),
    });
    (r2, kv, asset)
}

/// Route deletions to appropriate storage backends.
pub fn multiplex_delete(
    disappeards: impl Iterator<Item = StoragePointer>,
) -> (Vec<R2Delete>, Vec<KvDelete>, Vec<AssetDelete>) {
    let mut r2 = Vec::new();
    let mut kv = Vec::new();
    let mut asset = Vec::new();
    disappeards.for_each(|pointer| match pointer {
        StoragePointer::R2 { bucket, key } => r2.push(R2Delete { bucket, key }),
        StoragePointer::Asset { path } => asset.push(AssetDelete { path }),
        StoragePointer::Kv { namespace, key } => kv.push(KvDelete { namespace, key }),
        StoragePointer::Inline { .. } => {}
    });
    (r2, kv, asset)
}
